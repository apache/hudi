/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

import static org.apache.hudi.common.schema.HoodieSchemaCompatibility.areSchemasProjectionEquivalent;

/**
 * Streaming sorted-merge reader for LSM file groups whose delta files are parquet files.
 *
 * <p>Each input file is expected to be sorted by record key. The iterator keeps one record from
 * each file in memory, merges all versions for the same key with the regular file-group reader
 * merge semantics, and emits the final row.
 */
public class LsmFileGroupRecordIterator<T> implements ClosableIterator<BufferedRecord<T>> {

  private static final String DELETE_LOG_RECORD_KEY_FIELD = "record_key";
  private static final HoodieSchema DELETE_LOG_SCHEMA = HoodieSchema.parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"hudi_delete_log_record\","
          + "\"fields\":["
          + "{\"name\":\"record_key\",\"type\":\"string\"},"
          + "{\"name\":\"partition_path\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"ordering_val\",\"type\":[\"null\",\"bytes\"],\"default\":null}"
          + "]"
          + "}");

  private final HoodieReaderContext<T> readerContext;
  private final HoodieStorage storage;
  private final InputSplit inputSplit;
  private final HoodieSchema readerSchema;
  private final List<String> orderingFieldNames;
  private final boolean includeBaseFile;
  private final BufferedRecordMerger<T> bufferedRecordMerger;
  private final UpdateProcessor<T> updateProcessor;
  private final LoserTree<T> readers;
  private BufferedRecord<T> nextRecord;

  public LsmFileGroupRecordIterator(HoodieReaderContext<T> readerContext,
                                    HoodieStorage storage,
                                    InputSplit inputSplit,
                                    List<String> orderingFieldNames,
                                    HoodieTableMetaClient metaClient,
                                    TypedProperties props,
                                    ReaderParameters readerParameters,
                                    HoodieReadStats readStats,
                                    Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback) throws IOException {
    this(readerContext, storage, inputSplit, orderingFieldNames, metaClient, props, readerParameters, readStats, fileGroupUpdateCallback, true);
  }

  public LsmFileGroupRecordIterator(HoodieReaderContext<T> readerContext,
                                    HoodieStorage storage,
                                    InputSplit inputSplit,
                                    List<String> orderingFieldNames,
                                    HoodieTableMetaClient metaClient,
                                    TypedProperties props,
                                    ReaderParameters readerParameters,
                                    HoodieReadStats readStats,
                                    Option<BaseFileUpdateCallback<T>> fileGroupUpdateCallback,
                                    boolean includeBaseFile) throws IOException {
    this.readerContext = readerContext;
    this.storage = storage;
    this.inputSplit = inputSplit;
    this.readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    this.orderingFieldNames = orderingFieldNames;
    this.includeBaseFile = includeBaseFile;
    this.bufferedRecordMerger = BufferedRecordMergerFactory.create(
        readerContext, readerContext.getMergeMode(), false, readerContext.getRecordMerger(),
        readerSchema, readerContext.getPayloadClasses(props), props, metaClient.getTableConfig().getPartialUpdateMode());
    this.updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDeletes(), fileGroupUpdateCallback, props);
    this.readers = new LoserTree<>(initializeReaders());
  }

  private List<ReaderState<T>> initializeReaders() throws IOException {
    List<ReaderState<T>> readerStates = new ArrayList<>();
    int mergeOrder = 0;
    if (includeBaseFile && inputSplit.getBaseFileOption().isPresent()) {
      addReader(readerStates, mergeOrder++, createBaseFileIterator(inputSplit.getBaseFileOption().get()));
    }
    for (HoodieLogFile logFile : inputSplit.getLogFiles()) {
      addReader(readerStates, mergeOrder++, createFileIterator(logFile.getPathInfo(), logFile.getPath(), logFile.getFileSize()));
    }
    return readerStates;
  }

  private void addReader(List<ReaderState<T>> readerStates, int mergeOrder, ClosableIterator<BufferedRecord<T>> iterator) {
    ReaderState<T> readerState = new ReaderState<>(mergeOrder, iterator);
    if (readerState.advance()) {
      readerStates.add(readerState);
    } else {
      readerState.close();
    }
  }

  private ClosableIterator<BufferedRecord<T>> createBaseFileIterator(HoodieBaseFile baseFile) throws IOException {
    BaseFile file = baseFile.getBootstrapBaseFile().orElse(baseFile);
    return createFileIterator(file.getPathInfo(), file.getStoragePath(), file.getFileSize());
  }

  private ClosableIterator<BufferedRecord<T>> createFileIterator(StoragePathInfo pathInfo,
                                                                 StoragePath path,
                                                                 long fileSize) throws IOException {
    StoragePath storagePath = pathInfo != null ? pathInfo.getPath() : path;
    if (FSUtils.isNativeDeleteLogFile(storagePath.getName())) {
      return createNativeDeleteLogIterator(pathInfo, storagePath, fileSize);
    }
    Pair<HoodieSchema, Map<String, String>> requiredSchemaAndRenamedColumns =
        readerContext.getSchemaHandler().getRequiredSchemaForFileAndRenamedColumns(storagePath);
    HoodieSchema fileRequiredSchema = requiredSchemaAndRenamedColumns.getLeft();
    ClosableIterator<T> recordIterator;
    if (pathInfo != null) {
      recordIterator = readerContext.getFileRecordIterator(
          pathInfo, 0, pathInfo.getLength(), readerContext.getSchemaHandler().getTableSchema(), fileRequiredSchema, storage);
    } else {
      long length = fileSize >= 0 ? fileSize : storage.getPathInfo(storagePath).getLength();
      recordIterator = readerContext.getFileRecordIterator(
          storagePath, 0, length, readerContext.getSchemaHandler().getTableSchema(), fileRequiredSchema, storage);
    }
    if (!areSchemasProjectionEquivalent(fileRequiredSchema, readerSchema) || !requiredSchemaAndRenamedColumns.getRight().isEmpty()) {
      UnaryOperator<T> projector = readerContext.getRecordContext()
          .projectRecord(fileRequiredSchema, readerSchema, requiredSchemaAndRenamedColumns.getRight());
      recordIterator = new CloseableMappingIterator<>(recordIterator, projector);
    }
    if (readerContext.getInstantRange().isPresent()) {
      recordIterator = readerContext.applyInstantRangeFilter(recordIterator);
    }
    return new CloseableMappingIterator<>(recordIterator, record -> BufferedRecords.fromEngineRecord(
        readerContext.getRecordContext().seal(record),
        readerSchema,
        readerContext.getRecordContext(),
        orderingFieldNames,
        readerContext.getRecordContext().isDeleteRecord(record, readerContext.getSchemaHandler().getDeleteContext().withReaderSchema(readerSchema))));
  }

  private ClosableIterator<BufferedRecord<T>> createNativeDeleteLogIterator(StoragePathInfo pathInfo,
                                                                            StoragePath storagePath,
                                                                            long fileSize) throws IOException {
    ClosableIterator<T> recordIterator;
    if (pathInfo != null) {
      recordIterator = readerContext.getFileRecordIterator(
          pathInfo, 0, pathInfo.getLength(), DELETE_LOG_SCHEMA, DELETE_LOG_SCHEMA, storage);
    } else {
      long length = fileSize >= 0 ? fileSize : storage.getPathInfo(storagePath).getLength();
      recordIterator = readerContext.getFileRecordIterator(
          storagePath, 0, length, DELETE_LOG_SCHEMA, DELETE_LOG_SCHEMA, storage);
    }
    return new CloseableMappingIterator<>(recordIterator, record -> {
      Object recordKey = readerContext.getRecordContext().getValue(record, DELETE_LOG_SCHEMA, DELETE_LOG_RECORD_KEY_FIELD);
      return BufferedRecords.createDelete(recordKey.toString(), OrderingValues.getDefault());
    });
  }

  @Override
  public boolean hasNext() {
    if (nextRecord != null) {
      return true;
    }
    while (!readers.isEmpty()) {
      BufferedRecord<T> mergedRecord = nextMergedRecord();
      nextRecord = updateProcessor.processUpdate(
          mergedRecord.getRecordKey(), null, mergedRecord, mergedRecord.isDelete());
      if (nextRecord != null) {
        return true;
      }
    }
    return false;
  }

  private BufferedRecord<T> nextMergedRecord() {
    BufferedRecord<T> firstRecord = readers.peekWinner();
    String recordKey = firstRecord.getRecordKey();
    BufferedRecord<T> mergedRecord = null;
    while (!readers.isEmpty() && recordKey.equals(readers.peekWinner().getRecordKey())) {
      mergedRecord = merge(mergedRecord, readers.popWinner());
    }
    return mergedRecord;
  }

  private BufferedRecord<T> merge(BufferedRecord<T> existingRecord, BufferedRecord<T> newRecord) {
    if (existingRecord == null) {
      return newRecord;
    }
    try {
      return bufferedRecordMerger.deltaMerge(newRecord, existingRecord).orElse(existingRecord);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to merge LSM records for key " + newRecord.getRecordKey(), e);
    }
  }

  @Override
  public BufferedRecord<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    BufferedRecord<T> record = nextRecord;
    nextRecord = null;
    return record;
  }

  @Override
  public void close() {
    readers.close();
  }

  private enum State {
    WINNER_WITH_NEW_KEY,
    WINNER_WITH_SAME_KEY,
    WINNER_POPPED,
    LOSER_WITH_NEW_KEY,
    LOSER_WITH_SAME_KEY,
    LOSER_POPPED
  }

  /**
   * Loser-tree state machine for k-way merging. Each leaf keeps one active record from
   * one sorted input stream; {@code tree[0]} stores the current champion and internal
   * nodes store the loser from the corresponding tournament match.
   */
  private static class LoserTree<T> {
    private final List<ReaderState<T>> leaves;
    private final int leafBase;
    private final int[] tree;
    private final int[] winners;

    private LoserTree(List<ReaderState<T>> leaves) {
      this.leaves = leaves;
      this.leafBase = nextPowerOfTwo(Math.max(1, leaves.size()));
      this.tree = new int[leafBase];
      this.winners = new int[leafBase << 1];
      Arrays.fill(tree, -1);
      Arrays.fill(winners, -1);
      build();
    }

    private void build() {
      for (int i = 0; i < leaves.size(); i++) {
        winners[leafBase + i] = leaves.get(i).current == null ? -1 : i;
      }
      if (leafBase == 1) {
        tree[0] = winners[leafBase];
      } else {
        for (int node = leafBase - 1; node > 0; node--) {
          replay(node);
        }
      }
      setChampionState(null);
    }

    private boolean isEmpty() {
      return tree[0] < 0;
    }

    private BufferedRecord<T> peekWinner() {
      int winnerIndex = tree[0];
      return winnerIndex < 0 ? null : leaves.get(winnerIndex).current;
    }

    private BufferedRecord<T> popWinner() {
      int winnerIndex = tree[0];
      ReaderState<T> winner = leaves.get(winnerIndex);
      BufferedRecord<T> record = winner.current;
      String recordKey = record.getRecordKey();
      winner.state = State.WINNER_POPPED;
      winner.firstSameKeyIndex = -1;
      if (!winner.advance()) {
        winner.state = State.LOSER_POPPED;
        winner.close();
      }
      update(winnerIndex, recordKey);
      return record;
    }

    private void update(int leafIndex, String poppedKey) {
      winners[leafBase + leafIndex] = leaves.get(leafIndex).current == null ? -1 : leafIndex;
      if (leafBase == 1) {
        tree[0] = winners[leafBase];
        setChampionState(poppedKey);
        return;
      }
      int node = (leafBase + leafIndex) >> 1;
      while (node > 0) {
        replay(node);
        node >>= 1;
      }
      setChampionState(poppedKey);
    }

    private void replay(int node) {
      int left = winners[node << 1];
      int right = winners[(node << 1) + 1];
      if (left < 0 && right < 0) {
        winners[node] = -1;
        tree[node] = -1;
      } else if (left < 0) {
        winners[node] = right;
        tree[node] = -1;
      } else if (right < 0) {
        winners[node] = left;
        tree[node] = -1;
      } else {
        int compareResult = compare(left, right);
        if (compareResult <= 0) {
          winners[node] = left;
          tree[node] = right;
          markLoser(right, left, compareResult);
        } else {
          winners[node] = right;
          tree[node] = left;
          markLoser(left, right, compareResult);
        }
      }
      if (node == 1) {
        tree[0] = winners[node];
      }
    }

    private int compare(int leftIndex, int rightIndex) {
      ReaderState<T> left = leaves.get(leftIndex);
      ReaderState<T> right = leaves.get(rightIndex);
      int keyCompare = left.current.getRecordKey().compareTo(right.current.getRecordKey());
      if (keyCompare != 0) {
        return keyCompare;
      }
      // Process older sources first so the regular merger sees later sources last.
      // This preserves HoodieFileGroupReader tie semantics when ordering values are equal:
      // base < older log instant/version < newer log instant/version.
      return Integer.compare(left.mergeOrder, right.mergeOrder);
    }

    private void markLoser(int loserIndex, int winnerIndex, int compareResult) {
      ReaderState<T> loser = leaves.get(loserIndex);
      boolean sameKey = leaves.get(loserIndex).current.getRecordKey().equals(leaves.get(winnerIndex).current.getRecordKey());
      loser.state = sameKey ? State.LOSER_WITH_SAME_KEY : State.LOSER_WITH_NEW_KEY;
      loser.firstSameKeyIndex = sameKey ? winnerIndex : -1;
    }

    private void setChampionState(String poppedKey) {
      int championIndex = tree[0];
      if (championIndex < 0) {
        return;
      }
      ReaderState<T> champion = leaves.get(championIndex);
      champion.state = poppedKey != null && poppedKey.equals(champion.current.getRecordKey())
          ? State.WINNER_WITH_SAME_KEY
          : State.WINNER_WITH_NEW_KEY;
      champion.firstSameKeyIndex = findFirstSameKeyIndex(championIndex);
    }

    private int findFirstSameKeyIndex(int championIndex) {
      String championKey = leaves.get(championIndex).current.getRecordKey();
      int firstSameKeyIndex = -1;
      for (int loserIndex : tree) {
        if (loserIndex >= 0 && loserIndex != championIndex
            && leaves.get(loserIndex).current != null
            && championKey.equals(leaves.get(loserIndex).current.getRecordKey())
            && (firstSameKeyIndex < 0 || leaves.get(loserIndex).mergeOrder < leaves.get(firstSameKeyIndex).mergeOrder)) {
          firstSameKeyIndex = loserIndex;
        }
      }
      return firstSameKeyIndex;
    }

    private void close() {
      leaves.forEach(ReaderState::close);
    }

    private static int nextPowerOfTwo(int value) {
      int result = 1;
      while (result < value) {
        result <<= 1;
      }
      return result;
    }
  }

  private static class ReaderState<T> {
    private final int mergeOrder;
    private final ClosableIterator<BufferedRecord<T>> iterator;
    private BufferedRecord<T> current;
    private State state = State.WINNER_WITH_NEW_KEY;
    private int firstSameKeyIndex = -1;
    private boolean closed;

    private ReaderState(int mergeOrder, ClosableIterator<BufferedRecord<T>> iterator) {
      this.mergeOrder = mergeOrder;
      this.iterator = iterator;
    }

    private boolean advance() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      current = null;
      return false;
    }

    private void close() {
      if (!closed) {
        iterator.close();
        closed = true;
      }
    }
  }
}
