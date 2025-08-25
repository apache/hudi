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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.KeySpec;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into record position based map.
 * Here the position means that record position in the base file. The records from the base file is accessed from an iterator object. These records are merged when the
 * {@link #hasNext} method is called.
 */
public class PositionBasedFileGroupRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(PositionBasedFileGroupRecordBuffer.class);

  private static final String ROW_INDEX_COLUMN_NAME = "row_index";
  public static final String ROW_INDEX_TEMPORARY_COLUMN_NAME = "_tmp_metadata_" + ROW_INDEX_COLUMN_NAME;
  protected final String baseFileInstantTime;
  private long nextRecordPosition = 0L;
  private boolean needToDoHybridStrategy = false;

  public PositionBasedFileGroupRecordBuffer(HoodieReaderContext<T> readerContext,
                                            HoodieTableMetaClient hoodieTableMetaClient,
                                            RecordMergeMode recordMergeMode,
                                            Option<PartialUpdateMode> partialUpdateModeOpt,
                                            String baseFileInstantTime,
                                            TypedProperties props,
                                            List<String> orderingFieldNames,
                                            UpdateProcessor<T> updateProcessor) {
    super(readerContext, hoodieTableMetaClient, recordMergeMode, partialUpdateModeOpt, props, orderingFieldNames, updateProcessor);
    this.baseFileInstantTime = baseFileInstantTime;
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
    List<Long> recordPositions = extractRecordPositions(dataBlock, baseFileInstantTime);
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

    if (dataBlock.containsPartialUpdates() && !enablePartialMerging) {
      // When a data block contains partial updates, subsequent record merging must always use
      // partial merging.
      enablePartialMerging = true;
      bufferedRecordMerger = BufferedRecordMergerFactory.create(
          readerContext,
          recordMergeMode,
          true,
          recordMerger,
          orderingFieldNames,
          readerSchema,
          payloadClasses,
          props,
          partialUpdateModeOpt);
    }

    Pair<Function<T, T>, Schema> schemaTransformerWithEvolvedSchema = getSchemaTransformerWithEvolvedSchema(dataBlock);

    Schema schema = AvroSchemaCache.intern(schemaTransformerWithEvolvedSchema.getRight());

    // TODO: Return an iterator that can generate sequence number with the record.
    //       Then we can hide this logic into data block.
    try (ClosableIterator<T> recordIterator = dataBlock.getEngineRecordIterator(readerContext)) {
      int recordIndex = 0;
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();

        // Skip a record if it is not contained in the specified keys.
        if (shouldSkip(nextRecord, isFullKey, keys, dataBlock.getSchema())) {
          recordIndex++;
          continue;
        }

        long recordPosition = recordPositions.get(recordIndex++);
        T evolvedNextRecord = schemaTransformerWithEvolvedSchema.getLeft().apply(nextRecord);
        boolean isDelete = readerContext.getRecordContext().isDeleteRecord(evolvedNextRecord, deleteContext);
        BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(evolvedNextRecord, schema, readerContext.getRecordContext(), orderingFieldNames, isDelete);
        processNextDataRecord(bufferedRecord, recordPosition);
      }
    }
  }

  private void fallbackToKeyBasedBuffer() {
    readerContext.setShouldMergeUseRecordPosition(false);
    //need to make a copy of the keys to avoid concurrent modification exception
    ArrayList<Serializable> positions = new ArrayList<>(records.keySet());
    for (Serializable position : positions) {
      BufferedRecord<T> entry = records.get(position);
      String recordKey = entry.getRecordKey();
      if (!entry.isDelete() || recordKey != null) {

        records.put(recordKey, entry);
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

    List<Long> recordPositions = extractRecordPositions(deleteBlock, baseFileInstantTime);
    if (recordPositions == null) {
      LOG.warn("Falling back to key based merge for Read");
      fallbackToKeyBasedBuffer();
      super.processDeleteBlock(deleteBlock);
      return;
    }

    switch (recordMergeMode) {
      case COMMIT_TIME_ORDERING:
        int commitTimeBasedRecordIndex = 0;
        DeleteRecord[] deleteRecords = deleteBlock.getRecordsToDelete();
        for (Long recordPosition : recordPositions) {
          // IMPORTANT:
          // use #put for log files with regular order(see HoodieLogFile.LOG_FILE_COMPARATOR);
          // use #putIfAbsent for log files with reverse order(see HoodieLogFile.LOG_FILE_COMPARATOR_REVERSED),
          // the delete block would be parsed ahead of a data block if they are in different log files.

          // set up the record key for key-based fallback handling, this is needed
          // because under hybrid strategy in #doHasNextFallbackBaseRecord, if the record keys are not set up,
          // this delete-vector could be kept in the records cache(see the check in #fallbackToKeyBasedBuffer),
          // and these keys would be deleted no matter whether there are following-up inserts/updates.
          DeleteRecord deleteRecord = deleteRecords[commitTimeBasedRecordIndex++];
          BufferedRecord<T> record = BufferedRecords.fromDeleteRecord(deleteRecord, readerContext.getRecordContext());
          records.put(recordPosition, record);
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
  public boolean containsLogRecord(String recordKey) {
    return records.values().stream()
        .filter(r -> !r.isDelete())
        .map(r -> readerContext.getRecordContext().getRecordKey(r.getRecord(), readerSchema)).anyMatch(recordKey::equals);
  }

  @Override
  protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      return doHasNextFallbackBaseRecord(baseRecord);
    }

    nextRecordPosition = readerContext.getRecordContext().extractRecordPosition(baseRecord, readerSchema,
        ROW_INDEX_TEMPORARY_COLUMN_NAME, nextRecordPosition);
    BufferedRecord<T> logRecordInfo = records.remove(nextRecordPosition++);
    return super.hasNextBaseRecord(baseRecord, logRecordInfo);
  }

  private boolean doHasNextFallbackBaseRecord(T baseRecord) throws IOException {
    if (needToDoHybridStrategy) {
      //see if there is a delete block with record positions
      nextRecordPosition = readerContext.getRecordContext().extractRecordPosition(baseRecord, readerSchema,
          ROW_INDEX_TEMPORARY_COLUMN_NAME, nextRecordPosition);
      BufferedRecord<T> logRecordInfo  = records.remove(nextRecordPosition++);
      if (logRecordInfo != null) {
        //we have a delete that was not to be able to be converted. Since it is the newest version, the record is deleted
        //remove a key based record if it exists
        records.remove(readerContext.getRecordContext().getRecordKey(baseRecord, readerSchema));
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
  protected boolean shouldSkip(T record, boolean isFullKey, Set<String> keys, Schema writerSchema) {
    // No keys are specified. Cannot skip at all.
    if (keys.isEmpty()) {
      return false;
    }

    String recordKey = readerContext.getRecordContext().getRecordKey(record, writerSchema);
    // Can not extract the record key, throw.
    if (recordKey == null || recordKey.isEmpty()) {
      throw new HoodieKeyException("Can not extract the key for a record");
    }

    // When the record key matches with one of the keys or key prefixes, can not skip.
    return (!isFullKey || !keys.contains(recordKey))
        && (isFullKey || keys.stream().noneMatch(recordKey::startsWith));
  }

  /**
   * Extracts valid record positions from a log block header.
   *
   * @param logBlock            {@link HoodieLogBlock} instance of the log block
   * @param baseFileInstantTime base file instant time for the file group to read
   *
   * @return valid record positions
   * @throws IOException upon I/O errors
   */
  protected static List<Long> extractRecordPositions(HoodieLogBlock logBlock,
                                                     String baseFileInstantTime) throws IOException {
    List<Long> blockPositions = new ArrayList<>();

    String blockBaseFileInstantTime = logBlock.getBaseFileInstantTimeOfPositions();
    if (StringUtils.isNullOrEmpty(blockBaseFileInstantTime) || !baseFileInstantTime.equals(blockBaseFileInstantTime)) {
      LOG.debug("The record positions cannot be used because the base file instant time "
              + "is either missing or different from the base file to merge. "
              + "Instant time in the header: {}, base file instant time of the file group: {}.",
          blockBaseFileInstantTime, baseFileInstantTime);
      return null;
    }
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