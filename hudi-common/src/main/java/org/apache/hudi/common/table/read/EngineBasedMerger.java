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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;

class EngineBasedMerger<T> {
  private final HoodieReaderContext<T> readerContext;
  private final RecordMergeMode recordMergeMode;
  private final Option<HoodieRecordMerger> recordMerger;
  private final Option<String> payloadClass;
  private final Schema readerSchema;
  private final Option<String> orderingFieldName;
  private final TypedProperties props;

  EngineBasedMerger(HoodieReaderContext<T> readerContext, RecordMergeMode recordMergeMode, HoodieTableConfig tableConfig, TypedProperties props) {
    this.readerContext = readerContext;
    this.readerSchema = AvroSchemaCache.intern(readerContext.getSchemaHandler().getRequiredSchema());
    this.recordMergeMode = recordMergeMode;
    this.recordMerger = readerContext.getRecordMerger();
    if (recordMerger.isPresent() && recordMerger.get().getMergingStrategy().equals(PAYLOAD_BASED_MERGE_STRATEGY_UUID)) {
      this.payloadClass = Option.of(tableConfig.getPayloadClass());
    } else {
      this.payloadClass = Option.empty();
    }
    this.orderingFieldName = recordMergeMode == RecordMergeMode.COMMIT_TIME_ORDERING
        ? Option.empty()
        : Option.ofNullable(ConfigUtils.getOrderingField(props))
        .or(() -> {
          String preCombineField = tableConfig.getPreCombineField();
          if (StringUtils.isNullOrEmpty(preCombineField)) {
            return Option.empty();
          }
          return Option.of(preCombineField);
        });
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
          readerContext.decodeAvroSchema(older.getSchemaId()),
          readerContext.constructHoodieRecord(newer),
          readerContext.decodeAvroSchema(newer.getSchemaId()),
          readerSchema,
          props);

      return mergedRecord.map(combinedRecordAndSchema -> {
        HoodieRecord<T> combinedRecord = combinedRecordAndSchema.getLeft();
        // If pre-combine returns existing record, no need to update it
        if (combinedRecord.getData() != olderOption.orElse(null)) {
          return BufferedRecord.forRecordWithContext(combinedRecord.getData(), combinedRecordAndSchema.getRight(), readerContext, orderingFieldName, false);
        }
        return older;
      }).orElseGet(newer::asDeleteRecord);
    } else {
      switch (recordMergeMode) {
        case COMMIT_TIME_ORDERING:
          return newer;
        case EVENT_TIME_ORDERING:
          if (newer.isHardDelete()) {
            return newer;
          }
          if (older.isHardDelete()) {
            return older;
          }
          Comparable newOrderingValue = newer.getOrderingValue();
          Comparable oldOrderingValue = olderOption.get().getOrderingValue();
          if (oldOrderingValue.compareTo(newOrderingValue) > 0) {
            return older;
          }
          return newer;
        case CUSTOM:
        default:
          if (payloadClass.isPresent()) {
            Option<Pair<HoodieRecord, Schema>> mergedRecord = getMergedRecord(older, newer);
            return mergedRecord.map(combinedRecordAndSchema -> {
              T record = readerContext.convertAvroRecord((IndexedRecord) combinedRecordAndSchema.getLeft());
              return BufferedRecord.forRecordWithContext(record, combinedRecordAndSchema.getRight(), readerContext, orderingFieldName, false);
            }).orElseGet(newer::asDeleteRecord);
          } else {
            Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
                readerContext.constructHoodieRecord(older), readerContext.decodeAvroSchema(older.getSchemaId()),
                readerContext.constructHoodieRecord(newer), readerContext.decodeAvroSchema(newer.getSchemaId()), props);
            return mergedRecord.map(combinedRecordAndSchema ->
                BufferedRecord.forRecordWithContext((T) combinedRecordAndSchema.getLeft().getData(), combinedRecordAndSchema.getRight(), readerContext, orderingFieldName, false))
                .orElseGet(newer::asDeleteRecord);
          }
      }
    }
  }

  private Option<Pair<HoodieRecord, Schema>> getMergedRecord(BufferedRecord<T> older, BufferedRecord<T> newer) throws IOException {
    ValidationUtils.checkArgument(!Objects.equals(payloadClass, OverwriteWithLatestAvroPayload.class.getCanonicalName())
        && !Objects.equals(payloadClass, DefaultHoodieRecordPayload.class.getCanonicalName()));
    HoodieRecord oldHoodieRecord = constructHoodieAvroRecord(readerContext, older);
    HoodieRecord newHoodieRecord = constructHoodieAvroRecord(readerContext, newer);
    Option<Pair<HoodieRecord, Schema>> mergedRecord = recordMerger.get().merge(
        oldHoodieRecord, getSchemaForAvroPayloadMerge(oldHoodieRecord, older.getSchemaId()),
        newHoodieRecord, getSchemaForAvroPayloadMerge(newHoodieRecord, newer.getSchemaId()), props);
    return mergedRecord;
  }

  private Schema getSchemaForAvroPayloadMerge(HoodieRecord record, Integer schemaId) throws IOException {
    if (record.isDelete(readerSchema, props)) {
      return readerSchema;
    }
    return readerContext.decodeAvroSchema(schemaId);
  }

  /**
   * Constructs a new {@link HoodieAvroRecord} for payload based merging
   *
   * @param readerContext reader context
   * @param bufferedRecord TODO
   * @return A new instance of {@link HoodieRecord}.
   */
  private HoodieRecord constructHoodieAvroRecord(HoodieReaderContext<T> readerContext, BufferedRecord<T> bufferedRecord) {
    GenericRecord record = null;
    if (bufferedRecord.getRecord() != null) {
      Schema recordSchema = readerContext.decodeAvroSchema(bufferedRecord.getSchemaId());
      record = readerContext.convertToAvroRecord(bufferedRecord.getRecord(), recordSchema);
    }
    return new HoodieAvroRecord<>(HoodieRecordUtils.loadPayload(payloadClass.get(), record, bufferedRecord.getOrderingValue()));
  }
}
