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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.config.HoodieReaderConfig.LSM_SORT_MERGE_SPILL_THRESHOLD;
import static org.apache.hudi.io.util.FileIOUtils.getDefaultSpillableMapBasePath;

/**
 * Streaming sorted-merge iterator for RFC-103 LSM file groups.
 *
 * <p>The iterator merges one optional L1/base sorted run with zero or more L0/native parquet log
 * runs. Every input file must already be sorted by record key; this class does not sort records
 * within a file. The active head record from each participating run is tracked by a loser tree,
 * which provides efficient k-way merge behavior while preserving deterministic source ordering for
 * records with the same key.
 *
 * <p>Merge order follows the same conflict resolution model as {@code HoodieFileGroupReader}: the
 * L1/base file is processed first, and L0 log files are processed in file-group log order so newer
 * log instants or versions can win when ordering values tie. Native delete logs contain only delete
 * metadata and are converted into {@link BufferedRecord} delete records before entering the merge.
 *
 * <p>To reduce open-reader memory pressure when a file group has many L0 runs, the iterator can spill
 * selected L0 file iterators to sequential temporary files. Spilling changes how an input run is
 * buffered, but not its merge order or merge semantics. The L1/base iterator is always kept direct,
 * and native delete logs plus smaller L0 files are prioritized for direct reading.
 */
public class LsmFileGroupRecordIterator<T> implements ClosableIterator<BufferedRecord<T>> {

  private final HoodieReaderContext<T> readerContext;
  private final HoodieTableMetaClient metaClient;
  private final HoodieStorage storage;
  private final InputSplit inputSplit;
  private final HoodieSchema readerSchema;
  private final List<String> orderingFieldNames;
  private final TypedProperties props;
  private final boolean readBaseFile;
  private final BufferedRecordMerger<T> bufferedRecordMerger;
  private final UpdateProcessor<T> updateProcessor;
  private final LoserTree<T> readers;
  private final int spillThreshold;
  private final String spillBasePath;
  private BufferedRecord<T> nextRecord;

  /**
   * Creates an iterator that merges both the L1/base file, when present, and all L0 native log files.
   */
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

  /**
   * Creates an iterator over an LSM file group.
   *
   * @param includeBaseFile whether the L1/base file should be included in the merge. Passing
   *                        {@code false} produces a log-only view for callers that only need L0 data.
   */
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
    this.metaClient = metaClient;
    this.storage = storage;
    this.inputSplit = inputSplit;
    this.readerSchema = readerContext.getSchemaHandler().getRequiredSchema();
    this.orderingFieldNames = orderingFieldNames;
    this.props = props;
    this.readBaseFile = includeBaseFile && inputSplit.getBaseFileOption().isPresent();
    this.spillThreshold = Math.max(0, props.getInteger(LSM_SORT_MERGE_SPILL_THRESHOLD.key(), LSM_SORT_MERGE_SPILL_THRESHOLD.defaultValue()));
    this.spillBasePath = props.getString(SPILLABLE_MAP_BASE_PATH.key(), getDefaultSpillableMapBasePath());
    this.bufferedRecordMerger = BufferedRecordMergerFactory.create(
        readerContext, readerContext.getMergeMode(), false, readerContext.getRecordMerger(),
        readerSchema, readerContext.getPayloadClasses(props), props, metaClient.getTableConfig().getPartialUpdateMode());
    this.updateProcessor = UpdateProcessor.create(readStats, readerContext, readerParameters.isEmitDeletes(), fileGroupUpdateCallback, props);
    this.readers = new LoserTree<>(initializeReaders());
  }

  /**
   * Builds one sorted-run reader for each sorted run that has at least one record.
   *
   * <p>The assigned {@code mergeOrder} is the stable source precedence used when multiple runs expose
   * the same key. It is assigned before spill selection so direct and spilled iterators remain
   * semantically identical during the loser-tree merge.
   */
  private List<SortedRunReader<T>> initializeReaders() throws IOException {
    List<SortedRunReader<T>> sortedRunReaders = new ArrayList<>();
    int mergeOrder = 0;
    if (readBaseFile) {
      addReader(sortedRunReaders, mergeOrder++, LsmFileIterators.createBaseFileIterator(
          readerContext, storage, inputSplit.getBaseFileOption().get(),
          inputSplit.getStart(), inputSplit.getLength(), orderingFieldNames, false));
    }

    if (inputSplit.hasRecordIterator()) {
      addReader(sortedRunReaders, mergeOrder++, createRecordIterator(inputSplit.getRecordIterator()));
    }

    List<LogReaderSpec> logReaderSpecs = new ArrayList<>();
    if (!inputSplit.hasRecordIterator()) {
      for (HoodieLogFile logFile : inputSplit.getLogFiles()) {
        logReaderSpecs.add(new LogReaderSpec(mergeOrder++, logFile));
      }
    }
    Set<Integer> directLogMergeOrders = selectDirectLogMergeOrders(logReaderSpecs, readBaseFile);
    for (LogReaderSpec spec : logReaderSpecs) {
      ClosableIterator<BufferedRecord<T>> iterator = LsmFileIterators.createLogFileIterator(
          readerContext, metaClient, storage, spec.logFile, orderingFieldNames);
      addReader(sortedRunReaders, spec.mergeOrder, maybeSpillIterator(directLogMergeOrders.contains(spec.mergeOrder), iterator));
    }
    return sortedRunReaders;
  }

  /**
   * Selects which L0 log readers stay direct under the configured spill threshold.
   *
   * <p>The base/L1 reader consumes one direct-reader slot when present and is never spilled. Remaining
   * direct-reader budget is spent on native delete logs first, then smaller log files, because those
   * readers tend to be cheaper to keep open while avoiding unnecessary spill materialization.
   */
  private Set<Integer> selectDirectLogMergeOrders(List<LogReaderSpec> logReaderSpecs, boolean hasBaseFileReader) {
    return selectDirectLogMergeOrders(logReaderSpecs, hasBaseFileReader, spillThreshold);
  }

  @VisibleForTesting
  static Set<Integer> selectDirectLogMergeOrders(List<LogReaderSpec> logReaderSpecs,
                                                 boolean hasBaseFileReader,
                                                 int spillThreshold) {
    int directLogBudget = spillThreshold - (hasBaseFileReader ? 1 : 0);
    if (directLogBudget <= 0) {
      return new HashSet<>();
    }
    Set<Integer> directMergeOrders = new HashSet<>();
    logReaderSpecs.stream()
        .sorted(Comparator
            .comparing((LogReaderSpec spec) -> !spec.nativeDeleteLog)
            .thenComparingLong(spec -> spec.fileSize)
            .thenComparingInt(spec -> spec.mergeOrder))
        .limit(directLogBudget)
        .forEach(spec -> directMergeOrders.add(spec.mergeOrder));
    return directMergeOrders;
  }

  /**
   * Returns the original iterator when it is selected for direct reading, otherwise materializes it
   * into a sequential spill iterator.
   */
  private ClosableIterator<BufferedRecord<T>> maybeSpillIterator(boolean directReader,
                                                                 ClosableIterator<BufferedRecord<T>> iterator) {
    if (directReader) {
      return iterator;
    }
    return new SpillableLsmRecordIterator<>(iterator, readerContext.getRecordSerializer(), readerContext.getRecordContext(), spillBasePath);
  }

  /**
   * Metadata used only for choosing the direct-versus-spilled L0 reader plan.
   */
  @VisibleForTesting
  static class LogReaderSpec {
    final int mergeOrder;
    final HoodieLogFile logFile;
    final boolean nativeDeleteLog;
    final long fileSize;

    LogReaderSpec(int mergeOrder, HoodieLogFile logFile) {
      this.mergeOrder = mergeOrder;
      this.logFile = logFile;
      this.nativeDeleteLog = FSUtils.isNativeDeleteLogFile(logFile.getFileName());
      this.fileSize = logFile.getFileSize() >= 0 ? logFile.getFileSize() : Long.MAX_VALUE;
    }
  }

  /**
   * Adds a reader to the merge only when the underlying sorted run contains at least one record.
   */
  private void addReader(List<SortedRunReader<T>> sortedRunReaders, int mergeOrder, ClosableIterator<BufferedRecord<T>> iterator) {
    SortedRunReader<T> sortedRunReader = new SortedRunReader<>(mergeOrder, iterator);
    if (sortedRunReader.advance()) {
      sortedRunReaders.add(sortedRunReader);
    } else {
      sortedRunReader.close();
    }
  }

  /**
   * Creates a sorted-run iterator from incoming write records.
   */
  private ClosableIterator<BufferedRecord<T>> createRecordIterator(Iterator<HoodieRecord> recordIterator) {
    HoodieSchema recordSchema = HoodieSchemaCache.intern(getRecordSchema());
    String[] orderingFieldsArray = orderingFieldNames.toArray(new String[0]);
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, recordSchema);
    return new ClosableIterator<BufferedRecord<T>>() {
      @Override
      public boolean hasNext() {
        return recordIterator.hasNext();
      }

      @Override
      public BufferedRecord<T> next() {
        return BufferedRecords.fromHoodieRecord(recordIterator.next(), recordSchema, readerContext.getRecordContext(),
            props, orderingFieldsArray, deleteContext);
      }

      @Override
      public void close() {
        // no op.
      }
    };
  }

  private HoodieSchema getRecordSchema() {
    Option<Pair<String, String>> payloadClasses = readerContext.getPayloadClasses(props);
    if (payloadClasses.isPresent() && payloadClasses.get().getRight().equals("org.apache.spark.sql.hudi.command.payload.ExpressionPayload")) {
      String schemaStr = props.getString("hoodie.payload.record.schema");
      return HoodieSchema.parse(schemaStr);
    }
    return HoodieSchemaUtils.removeMetadataFields(readerContext.getSchemaHandler().getRequestedSchema());
  }

  @Override
  public boolean hasNext() {
    if (nextRecord != null) {
      return true;
    }
    while (!readers.isEmpty()) {
      MergeResult<T> records = nextMergedRecord();
      BufferedRecord<T> mergedRecord = records.mergedRecord;
      // Match HoodieFileGroupReader: untouched base records bypass update processing. processUpdate
      // is reserved for base records with a matching log record and records found only in logs.
      nextRecord = records.hasLogRecord
          ? updateProcessor.processUpdate(
              mergedRecord.getRecordKey(), records.baseRecord, mergedRecord, mergedRecord.isDelete())
          : mergedRecord;
      if (nextRecord != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Pops and merges all currently visible versions for the next record key.
   */
  private MergeResult<T> nextMergedRecord() {
    boolean firstRecordIsFromBase = readBaseFile && readers.peekWinnerMergeOrder() == 0;
    BufferedRecord<T> mergedRecord = readers.popWinner();
    String recordKey = mergedRecord.getRecordKey();
    BufferedRecord<T> baseRecord = firstRecordIsFromBase ? mergedRecord : null;
    boolean hasLogRecord = !firstRecordIsFromBase;
    while (!readers.isEmpty() && recordKey.equals(readers.peekWinner().getRecordKey())) {
      hasLogRecord = true;
      mergedRecord = merge(mergedRecord, readers.popWinner());
    }
    return new MergeResult<>(baseRecord, mergedRecord, hasLogRecord);
  }

  /**
   * Result of merging all records with the same key across the sorted runs.
   */
  private static class MergeResult<T> {
    // The original base record, or null when the key does not exist in the base file.
    private final BufferedRecord<T> baseRecord;
    // The record produced after merging all visible versions of the key.
    private final BufferedRecord<T> mergedRecord;
    // Whether any version came from a log file and therefore requires update processing.
    private final boolean hasLogRecord;

    private MergeResult(BufferedRecord<T> baseRecord,
                        BufferedRecord<T> mergedRecord,
                        boolean hasLogRecord) {
      this.baseRecord = baseRecord;
      this.mergedRecord = mergedRecord;
      this.hasLogRecord = hasLogRecord;
    }
  }

  /**
   * Applies the configured buffered-record merger so later sources can win when ordering values tie.
   */
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

  /**
   * Loser-tree state machine for k-way merging. Each leaf keeps one active record from
   * one sorted input stream; {@code tree[0]} stores the current champion and internal
   * nodes store the loser from the corresponding tournament match.
   */
  @VisibleForTesting
  static class LoserTree<T> {
    private final List<SortedRunReader<T>> leaves;
    private final int leafBase;
    private final int[] tree;
    private final int[] winners;

    LoserTree(List<SortedRunReader<T>> leaves) {
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
    }

    boolean isEmpty() {
      return tree[0] < 0;
    }

    BufferedRecord<T> peekWinner() {
      int winnerIndex = tree[0];
      return winnerIndex < 0 ? null : leaves.get(winnerIndex).current;
    }

    int peekWinnerMergeOrder() {
      int winnerIndex = tree[0];
      return winnerIndex < 0 ? -1 : leaves.get(winnerIndex).mergeOrder;
    }

    BufferedRecord<T> popWinner() {
      int winnerIndex = tree[0];
      SortedRunReader<T> winner = leaves.get(winnerIndex);
      BufferedRecord<T> record = winner.current;
      if (!winner.advance()) {
        winner.close();
      }
      update(winnerIndex);
      return record;
    }

    private void update(int leafIndex) {
      winners[leafBase + leafIndex] = leaves.get(leafIndex).current == null ? -1 : leafIndex;
      if (leafBase == 1) {
        tree[0] = winners[leafBase];
        return;
      }
      int node = (leafBase + leafIndex) >> 1;
      while (node > 0) {
        replay(node);
        node >>= 1;
      }
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
        if (compare(left, right) <= 0) {
          winners[node] = left;
          tree[node] = right;
        } else {
          winners[node] = right;
          tree[node] = left;
        }
      }
      if (node == 1) {
        tree[0] = winners[node];
      }
    }

    private int compare(int leftIndex, int rightIndex) {
      SortedRunReader<T> left = leaves.get(leftIndex);
      SortedRunReader<T> right = leaves.get(rightIndex);
      int keyCompare = left.current.getRecordKey().compareTo(right.current.getRecordKey());
      if (keyCompare != 0) {
        return keyCompare;
      }
      // Process older sources first so the regular merger sees later sources last.
      // This preserves HoodieFileGroupReader tie semantics when ordering values are equal:
      // base < older log instant/version < newer log instant/version.
      return Integer.compare(left.mergeOrder, right.mergeOrder);
    }

    private void close() {
      leaves.forEach(SortedRunReader::close);
    }

    private static int nextPowerOfTwo(int value) {
      int result = 1;
      while (result < value) {
        result <<= 1;
      }
      return result;
    }
  }

  @VisibleForTesting
  static class SortedRunReader<T> {
    private final int mergeOrder;
    private final ClosableIterator<BufferedRecord<T>> iterator;
    private BufferedRecord<T> current;
    private boolean closed;

    SortedRunReader(int mergeOrder, ClosableIterator<BufferedRecord<T>> iterator) {
      this.mergeOrder = mergeOrder;
      this.iterator = iterator;
    }

    boolean advance() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      current = null;
      return false;
    }

    void close() {
      if (!closed) {
        iterator.close();
        closed = true;
      }
    }
  }
}
