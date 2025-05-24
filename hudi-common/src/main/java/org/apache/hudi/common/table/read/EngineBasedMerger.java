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

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;

/**
 * Handles the logic for merging two records in an engine agnostic way. This allows the Hudi project to consolidate the merging logic and avoid deviation between engines.
 * The class takes in {@link HoodieReaderContext<T>} for the engine specific operations such as fetching the value representing the event time when {@link RecordMergeMode#EVENT_TIME_ORDERING} is used.
 * @param <T> The type of the engine's row
 */
public class EngineBasedMerger<T> {
  private final HoodieReaderContext<T> readerContext;
  private final RecordMergeMode recordMergeMode;
  private final Option<HoodieRecordMerger> recordMerger;
  private final Option<String> payloadClass;
  private final Schema readerSchema;
  private final TypedProperties props;

  public EngineBasedMerger(HoodieReaderContext<T> readerContext, RecordMergeMode recordMergeMode, HoodieTableConfig tableConfig, TypedProperties props) {
    this.readerContext = readerContext;
    this.readerSchema = AvroSchemaCache.intern(readerContext.getSchemaHandler().getRequiredSchema());
    this.recordMergeMode = recordMergeMode;
    this.recordMerger = readerContext.getRecordMerger();
    if (recordMerger.isPresent() && recordMerger.get().getMergingStrategy().equals(PAYLOAD_BASED_MERGE_STRATEGY_UUID)) {
      this.payloadClass = Option.of(tableConfig.getPayloadClass());
    } else {
      this.payloadClass = Option.empty();
    }
    this.props = props;
  }

  boolean shouldKeepIncomingDelete(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord) {
    if (existingRecord == null) {
      return true;
    }
    switch (recordMergeMode) {
      case COMMIT_TIME_ORDERING:
        return true;
      case EVENT_TIME_ORDERING:
      case CUSTOM:
      default:
        if (existingRecord.isCommitTimeOrderingDelete()) {
          return false;
        }
        Comparable existingOrderingVal = existingRecord.getOrderingValue();
        Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
        // Checks the ordering value does not equal to 0
        // because we use 0 as the default value which means natural order
        boolean chooseExisting = !deleteOrderingVal.equals(DEFAULT_ORDERING_VALUE)
            && ReflectionUtils.isSameClass(existingOrderingVal, deleteOrderingVal)
            && existingOrderingVal.compareTo(deleteOrderingVal) > 0;
        return !chooseExisting;
    }
  }

  BufferedRecord<T> merge(Option<BufferedRecord<T>> olderOption,
                          Option<BufferedRecord<T>> newerOption,
                          boolean enablePartialMerging) throws IOException {
    if (olderOption.isEmpty()) {
      return newerOption.orElseThrow(() -> new IllegalArgumentException("Both older and newer record are empty"));
    }
    if (newerOption.isEmpty()) {
      return olderOption.orElseThrow(() -> new IllegalArgumentException("Both older and newer record are empty"));
    }
    BufferedRecord<T> older = olderOption.get();
    BufferedRecord<T> newer = newerOption.get();
    // First check if either record is a delete record and handle this case
    if (older.isDelete() || newer.isDelete()) {
      if (recordMergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
        return newer;
      } else {
        return getNewerRecordWithEventTimeOrdering(older, newer);
      }
    }
    // If both records are not deletes, then they can be merged with partial or standard merging
    if (enablePartialMerging) {
      // TODO(HUDI-7843): decouple the merging logic from the merger
      //  and use the record merge mode to control how to merge partial updates
      Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().partialMerge(
          readerContext.constructHoodieRecord(older),
          readerContext.getSchemaFromBufferRecord(older),
          readerContext.constructHoodieRecord(newer),
          readerContext.getSchemaFromBufferRecord(newer),
          readerSchema,
          props);

      return mergedRecord.map(combinedRecordAndSchema -> {
        HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();
        // If pre-combine returns existing record, no need to update it
        if (combinedRecord.getData() != olderOption.map(BufferedRecord::getRecord).orElse(null)) {
          return BufferedRecord.forRecordWithContext(combinedRecord, combinedRecordAndSchema.getRight(), readerContext, props);
        }
        return older;
      }).orElseThrow(() -> new IllegalStateException(""));
    } else {
      switch (recordMergeMode) {
        case COMMIT_TIME_ORDERING:
          return newer;
        case EVENT_TIME_ORDERING:
          return getNewerRecordWithEventTimeOrdering(older, newer);
        case CUSTOM:
        default:
          Option<BufferedRecord<T>> mergeResult;
          if (payloadClass.isPresent()) {
            Option<Pair<HoodieRecord, Schema>> mergedRecord = getMergedRecord(older, newer);
            mergeResult = mergedRecord.map(combinedRecordAndSchema -> {
              IndexedRecord indexedRecord;
              if (!combinedRecordAndSchema.getRight().equals(readerSchema)) {
                indexedRecord = (IndexedRecord) combinedRecordAndSchema.getLeft().rewriteRecordWithNewSchema(combinedRecordAndSchema.getRight(), null, readerSchema).getData();
              } else {
                indexedRecord = (IndexedRecord) combinedRecordAndSchema.getLeft().getData();
              }
              T record = readerContext.convertAvroRecord(indexedRecord);
              return BufferedRecord.forConvertedRecord(record, combinedRecordAndSchema.getLeft(), combinedRecordAndSchema.getRight(), readerContext, props);
            });
          } else {
            Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
                readerContext.constructHoodieRecord(older), readerContext.getSchemaFromBufferRecord(older),
                readerContext.constructHoodieRecord(newer), readerContext.getSchemaFromBufferRecord(newer), props);
            mergeResult = mergedRecord.map(combinedRecordAndSchema -> {
              HoodieRecord<T> record;
              if (!combinedRecordAndSchema.getRight().equals(readerSchema)) {
                record = (HoodieRecord<T>) combinedRecordAndSchema.getLeft().rewriteRecordWithNewSchema(combinedRecordAndSchema.getRight(), null, readerSchema);
              } else {
                record = (HoodieRecord<T>) combinedRecordAndSchema.getLeft();
              }
              return BufferedRecord.forRecordWithContext(record, combinedRecordAndSchema.getRight(), readerContext, props);
            });
          }
          return mergeResult.orElseThrow(() -> new IllegalStateException(""));
      }
    }
  }

  private static <T> BufferedRecord<T> getNewerRecordWithEventTimeOrdering(BufferedRecord<T> older, BufferedRecord<T> newer) {
    if (newer.isCommitTimeOrderingDelete()) {
      return newer;
    }
    if (older.isCommitTimeOrderingDelete()) {
      return older;
    }
    Comparable newOrderingValue = newer.getOrderingValue();
    Comparable oldOrderingValue = older.getOrderingValue();
    return oldOrderingValue.compareTo(newOrderingValue) > 0 ? older : newer;
  }

  private Option<Pair<HoodieRecord, Schema>> getMergedRecord(BufferedRecord<T> older, BufferedRecord<T> newer) throws IOException {
    ValidationUtils.checkArgument(!Objects.equals(payloadClass.get(), OverwriteWithLatestAvroPayload.class.getCanonicalName())
        && !Objects.equals(payloadClass.get(), DefaultHoodieRecordPayload.class.getCanonicalName()));
    HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(readerContext, older);
    HoodieRecord newHoodieRecord = constructHoodieAvroRecord(readerContext, newer);
    return recordMerger.get().merge(oldHoodieRecord, getSchemaForAvroPayloadMerge(oldHoodieRecord, older),
        newHoodieRecord, getSchemaForAvroPayloadMerge(newHoodieRecord, newer), props);
  }

  private Schema getSchemaForAvroPayloadMerge(HoodieRecord record, BufferedRecord<T> bufferedRecord) throws IOException {
    if (record.isDelete(readerSchema, props)) {
      return readerSchema;
    }
    return readerContext.getSchemaFromBufferRecord(bufferedRecord);
  }

  /**
   * Constructs a new {@link HoodieAvroRecord} for payload based merging
   *
   * @param readerContext reader context
   * @param bufferedRecord the provided engine specific record and its metadata
   * @return A new instance of {@link HoodieRecord}.
   */
  private HoodieRecord constructHoodieAvroRecord(HoodieReaderContext<T> readerContext, BufferedRecord<T> bufferedRecord) {
    GenericRecord record = null;
    if (bufferedRecord.getRecord() != null) {
      Schema recordSchema = readerContext.getSchemaFromBufferRecord(bufferedRecord);
      record = readerContext.convertToAvroRecord(bufferedRecord.getRecord(), recordSchema);
    }
    return new HoodieAvroRecord<>(new HoodieKey(bufferedRecord.getRecordKey(), null), HoodieRecordUtils.loadPayload(payloadClass.get(), record, bufferedRecord.getOrderingValue()));
  }
}
