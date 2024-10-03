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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.Schema;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_RECORD_KEY;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into record position based map.
 * Here the position means that record position in the base file. The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class HoodiePositionBasedFileGroupRecordBuffer<T> extends HoodieKeyBasedFileGroupRecordBuffer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodiePositionBasedFileGroupRecordBuffer.class);

  private static final String ROW_INDEX_COLUMN_NAME = "row_index";
  public static final String ROW_INDEX_TEMPORARY_COLUMN_NAME = "_tmp_metadata_" + ROW_INDEX_COLUMN_NAME;
  private long nextRecordPosition = 0L;
  private boolean needToDoHybridStrategy = false;

  public HoodiePositionBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                                  HoodieTableMetaClient hoodieTableMetaClient,
                                                  Option<String> partitionNameOverrideOpt,
                                                  Option<String[]> partitionPathFieldOpt,
                                                  TypedProperties props) {
    super(readerContext, hoodieTableMetaClient, partitionNameOverrideOpt, partitionPathFieldOpt, props);
  }

  @Override
  public BufferType getBufferType() {
    return readerContext.getShouldMergeUseRecordPosition() ? BufferType.POSITION_BASED_MERGE : super.getBufferType();
  }

  @Override
  public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      super.processDataBlock(dataBlock, keySpecOpt);
      return;
    }
    // Extract positions from data block.
    List<Long> recordPositions = extractRecordPositions(dataBlock);
    if (recordPositions == null) {
      LOG.warn("Falling back to key based merge for Read");
      fallbackToKeyBasedBuffer();
      super.processDataBlock(dataBlock, keySpecOpt);
      return;
    }
    // Prepare key filters.
    Set<String> keys = new HashSet<>();
    boolean isFullKey = true;
    if (keySpecOpt.isPresent()) {
      if (!keySpecOpt.get().getKeys().isEmpty()) {
        keys = new HashSet<>(keySpecOpt.get().getKeys());
      }
      isFullKey = keySpecOpt.get().isFullKey();
    }

    if (dataBlock.containsPartialUpdates()) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
    }

    Pair<Function<T, T>, Schema> schemaTransformerWithEvolvedSchema = getSchemaTransformerWithEvolvedSchema(dataBlock);

    // TODO: Return an iterator that can generate sequence number with the record.
    //       Then we can hide this logic into data block.
    try (ClosableIterator<T> recordIterator = dataBlock.getEngineRecordIterator(readerContext)) {
      int recordIndex = 0;
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();

        // Skip a record if it is not contained in the specified keys.
        if (shouldSkip(nextRecord, dataBlock.getKeyFieldName(), isFullKey, keys, dataBlock.getSchema())) {
          recordIndex++;
          continue;
        }

        long recordPosition = recordPositions.get(recordIndex++);

        T evolvedNextRecord = schemaTransformerWithEvolvedSchema.getLeft().apply(nextRecord);
        processNextDataRecord(
            evolvedNextRecord,
            readerContext.generateMetadataForRecord(evolvedNextRecord, schemaTransformerWithEvolvedSchema.getRight()),
            recordPosition
        );
      }
    }
  }

  private void fallbackToKeyBasedBuffer() {
    readerContext.setShouldMergeUseRecordPosition(false);
    //need to make a copy of the keys to avoid concurrent modification exception
    ArrayList<Serializable> positions = new ArrayList<>(records.keySet());
    for (Serializable position : positions) {
      Pair<Option<T>, Map<String, Object>> entry = records.get(position);
      Object recordKey = entry.getRight().get(INTERNAL_META_RECORD_KEY);
      if (entry.getLeft().isPresent() || recordKey != null) {

        records.put((String) recordKey, entry);
        records.remove(position);
      } else {
        //if it's a delete record and the key is null, then we need to still use positions
        //this happens when we read the positions using logBlock.getRecordPositions()
        //instead of reading the delete records themselves
        needToDoHybridStrategy = true;
      }
    }
  }

  @Override
  public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      super.processDeleteBlock(deleteBlock);
      return;
    }

    List<Long> recordPositions = extractRecordPositions(deleteBlock);
    if (recordPositions == null) {
      LOG.warn("Falling back to key based merge for Read");
      fallbackToKeyBasedBuffer();
      super.processDeleteBlock(deleteBlock);
      return;
    }

    switch (recordMergeMode) {
      case OVERWRITE_WITH_LATEST:
        for (Long recordPosition : recordPositions) {
          records.putIfAbsent(recordPosition,
              Pair.of(Option.empty(), readerContext.generateMetadataForRecord(null, "", orderingFieldDefault, orderingFieldType)));
        }
        return;
      case EVENT_TIME_ORDERING:
      case CUSTOM:
      default:
        int recordIndex = 0;
        Iterator<DeleteRecord> it = Arrays.stream(deleteBlock.getRecordsToDelete()).iterator();
        while (it.hasNext()) {
          DeleteRecord record = it.next();
          long recordPosition = recordPositions.get(recordIndex++);
          processNextDeletedRecord(record, recordPosition);
        }
    }
  }

  @Override
  public void processNextDeletedRecord(DeleteRecord deleteRecord, Serializable recordPosition) {
    Pair<Option<T>, Map<String, Object>> existingRecordMetadataPair = records.get(recordPosition);
    Option<DeleteRecord> recordOpt = doProcessNextDeletedRecord(deleteRecord, existingRecordMetadataPair);
    if (recordOpt.isPresent()) {
      String recordKey = recordOpt.get().getRecordKey();
      records.put(recordPosition, Pair.of(Option.empty(), readerContext.generateMetadataForRecord(
          recordKey, recordOpt.get().getPartitionPath(), recordOpt.get().getOrderingValue() == null ? orderingFieldDefault : recordOpt.get().getOrderingValue(), orderingFieldType)));
    }
  }

  @Override
  public boolean containsLogRecord(String recordKey) {
    return records.values().stream()
        .filter(r -> r.getLeft().isPresent())
        .map(r -> readerContext.getRecordKey(r.getKey().get(), readerSchema)).anyMatch(recordKey::equals);
  }

  @Override
  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      return doHasNextFallbackBaseRecord(baseRecord);
    }

    nextRecordPosition = readerContext.extractRecordPosition(baseRecord, readerSchema,
        ROW_INDEX_TEMPORARY_COLUMN_NAME, nextRecordPosition);
    Pair<Option<T>, Map<String, Object>> logRecordInfo = records.remove(nextRecordPosition++);

    Map<String, Object> metadata = readerContext.generateMetadataForRecord(
        baseRecord, readerSchema);

    Option<T> resultRecord = logRecordInfo != null
        ? merge(Option.of(baseRecord), metadata, logRecordInfo.getLeft(), logRecordInfo.getRight())
        : merge(Option.empty(), Collections.emptyMap(), Option.of(baseRecord), metadata);
    if (resultRecord.isPresent()) {
      nextRecord = readerContext.seal(resultRecord.get());
      return true;
    }
    return false;
  }

  private boolean doHasNextFallbackBaseRecord(T baseRecord) throws IOException {
    if (needToDoHybridStrategy) {
      //see if there is a delete block with record positions
      nextRecordPosition = readerContext.extractRecordPosition(baseRecord, readerSchema,
          ROW_INDEX_TEMPORARY_COLUMN_NAME, nextRecordPosition);
      Pair<Option<T>, Map<String, Object>> logRecordInfo  = records.remove(nextRecordPosition++);
      if (logRecordInfo != null) {
        //we have a delete that was not able to be converted. Since it is the newest version, the record is deleted
        //remove a key based record if it exists
        records.remove(readerContext.getRecordKey(baseRecord, readerSchema));
        return false;
      }
    }
    return super.hasNextBaseRecord(baseRecord);
  }

  /**
   * Filter a record for downstream processing when:
   * 1. A set of pre-specified keys exists.
   * 2. The key of the record is not contained in the set.
   */
  protected boolean shouldSkip(T record, String keyFieldName, boolean isFullKey, Set<String> keys, Schema writerSchema) {
    // No keys are specified. Cannot skip at all.
    if (keys.isEmpty()) {
      return false;
    }

    String recordKey = readerContext.getValue(record, writerSchema, keyFieldName).toString();
    // Can not extract the record key, throw.
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("Can not extract the key for a record");
    }

    // When the record key matches with one of the keys or key prefixes, can not skip.
    if ((isFullKey && keys.contains(recordKey))
        || (!isFullKey && keys.stream().anyMatch(recordKey::startsWith))) {
      return false;
    }

    // Otherwise, this record is not needed.
    return true;
  }

  /**
   * Extract the record positions from a log block header.
   *
   * @param logBlock
   * @return
   * @throws IOException
   */
  protected static List<Long> extractRecordPositions(HoodieLogBlock logBlock) throws IOException {
    List<Long> blockPositions = new ArrayList<>();

    Roaring64NavigableMap positions = logBlock.getRecordPositions();
    if (positions == null || positions.isEmpty()) {
      LOG.warn("No record position info is found when attempt to do position based merge.");
      return null;
    }

    Iterator<Long> iterator = positions.iterator();
    while (iterator.hasNext()) {
      blockPositions.add(iterator.next());
    }

    if (blockPositions.isEmpty()) {
      LOG.warn("No positions are extracted.");
      return null;
    }

    return blockPositions;
  }
}