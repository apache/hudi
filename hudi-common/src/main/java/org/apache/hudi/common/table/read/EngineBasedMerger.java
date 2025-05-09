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
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

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
      }).orElseGet(() -> getLatestAsDeleteRecord(newer, older));
    } else {
      switch (recordMergeMode) {
        case COMMIT_TIME_ORDERING:
          return newer;
        case EVENT_TIME_ORDERING:
          return getNewerRecordWithEventTimeOrdering(newer, older);
        case CUSTOM:
        default:
          if (older.isDelete() || newer.isDelete()) {
            // IMPORTANT:
            // this is needed when the fallback HoodieAvroRecordMerger got used, the merger would
            // return Option.empty when the new payload data is empty(a delete) and ignores its ordering value directly.
            return getNewerRecordWithEventTimeOrdering(newer, older);
          }
          Option<BufferedRecord<T>> mergeResult;
          if (payloadClass.isPresent()) {
            Option<Pair<HoodieRecord, Schema>> mergedRecord = getMergedRecord(older, newer);
            mergeResult = mergedRecord.map(combinedRecordAndSchema -> {
              T record = readerContext.convertAvroRecord((IndexedRecord) combinedRecordAndSchema.getLeft().getData());
              return BufferedRecord.forConvertedRecord(record, combinedRecordAndSchema.getLeft(), combinedRecordAndSchema.getRight(), readerContext, props);
            });
          } else {
            Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
                readerContext.constructHoodieRecord(older), readerContext.getSchemaFromBufferRecord(older),
                readerContext.constructHoodieRecord(newer), readerContext.getSchemaFromBufferRecord(newer), props);
            mergeResult = mergedRecord.map(combinedRecordAndSchema ->
                BufferedRecord.forRecordWithContext((HoodieRecord<T>) combinedRecordAndSchema.getLeft(),
                    combinedRecordAndSchema.getRight(), readerContext, props));
          }
          return mergeResult.orElseGet(() -> getLatestAsDeleteRecord(newer, older));
      }
    }
  }

  private static <T> BufferedRecord<T> getNewerRecordWithEventTimeOrdering(BufferedRecord<T> newer, BufferedRecord<T> older) {
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

  /**
   * Picks the "latest" record based on the merger strategy and converts that to a delete by setting the `delete` flag.
   * @param newer the newer record passed to the merge function
   * @param older the older record passed to the merge function
   * @return the latest record as a delete record
   */
  private BufferedRecord<T> getLatestAsDeleteRecord(BufferedRecord<T> newer, BufferedRecord<T> older) {
    if (recordMerger.map(merger -> merger.getMergingStrategy().equals(HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID)).orElse(false)) {
      return newer.asDeleteRecord();
    }
    return getNewerRecordWithEventTimeOrdering(newer, older).asDeleteRecord();
  }
}
