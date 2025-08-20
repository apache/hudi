/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.nonEmpty;

/**
 * HoodieMerge defines how to merge two records. It is a stateless component.
 * It can implement the merging logic of HoodieRecord of different engines
 * and avoid the performance consumption caused by the serialization/deserialization of Avro payload.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface HoodieRecordMerger extends Serializable {

  // Uses event time ordering to determine which record is chosen
  String EVENT_TIME_BASED_MERGE_STRATEGY_UUID = "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5";

  // Always chooses the most recently written record
  String COMMIT_TIME_BASED_MERGE_STRATEGY_UUID = "ce9acb64-bde0-424c-9b91-f6ebba25356d";

  // Uses customized merge strategy to merge records
  String CUSTOM_MERGE_STRATEGY_UUID = "1897ef5f-18bc-4557-939c-9d6a8afd1519";

  // Use avro payload to merge records
  String PAYLOAD_BASED_MERGE_STRATEGY_UUID = "00000000-0000-0000-0000-000000000000";

  /**
   * This method converges combineAndGetUpdateValue and precombine from HoodiePayload.
   * It'd be associative operation: f(a, f(b, c)) = f(f(a, b), c) (which we can translate as having 3 versions A, B, C
   * of the single record, both orders of operations applications have to yield the same result)
   * This method takes only full records for merging.
   */
  Pair<HoodieRecord, Schema> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException;

  /**
   * Merges records which can contain partial updates, i.e., only subset of fields and values are
   * present in the record representing the updates, and absent fields are not updated. The fields
   * exist in older and newer records indicate the fields with changed values. When merging, only
   * the changed fields should be included in the merging results.
   * <p>
   * For example, the reader schema is
   * {[
   * {"name":"id", "type":"string"},
   * {"name":"ts", "type":"long"},
   * {"name":"name", "type":"string"},
   * {"name":"price", "type":"double"},
   * {"name":"tags", "type":"string"}
   * ]}
   * The older and newer records can be (omitting Hudi meta fields):
   * <p>
   * (1) older (complete record update):
   * id | ts | name  | price | tags
   *  1 | 10 | apple |  2.3  | fruit
   * <p>
   * newer (partial record update):
   * ts | price
   * 16 |  2.8
   * <p>
   * In this case, in the newer record, only "ts" and "price" fields are updated. With the default
   * merging strategy, the newer record updates the older record and the merging result is
   * <p>
   * id | ts | name  | price | tags
   *  1 | 16 | apple |  2.8  | fruit
   * <p>
   * (2) older (partial record update):
   * ts | price
   * 10 | 2.8
   * <p>
   * newer (partial record update):
   * ts | tag
   * 16 | fruit,juicy
   * <p>
   * In this case, in the older record, only "ts" and "price" fields are updated. In the newer
   * record, only "ts" and "tag" fields are updated. With the default merging strategy, all the
   * changed fields should be included in the merging results.
   * <p>
   * ts | price | tags
   * 16 |  2.8  | fruit,juicy
   *
   * @param older        Older record.
   * @param oldSchema    Schema of the older record.
   * @param newer        Newer record.
   * @param newSchema    Schema of the newer record.
   * @param readerSchema Reader schema containing all the fields to read. This is used to maintain
   *                     the ordering of the fields of the merged record.
   * @param props        Configuration in {@link TypedProperties}.
   * @return The merged record and schema.
   * @throws IOException upon merging error.
   */
  default Pair<HoodieRecord, Schema> partialMerge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, Schema readerSchema, TypedProperties props) throws IOException {
    throw new UnsupportedOperationException("Partial merging logic is not implemented by " + this.getClass().getName());
  }

  /**
   * If false, whenever we have log files, we will need to read all columns
   * If true, mor merging can be done without all columns. The columns required can be configured
   * by overriding getMandatoryFieldsForMerging
   * EventTime based merging and CommitTime based merging are projection compatible
   */
  default boolean isProjectionCompatible() {
    return false;
  }

  /**
   * Returns a list of fields required for mor merging. The default implementation will return the recordKey field and the ordering fields.
   */
  default String[] getMandatoryFieldsForMerging(Schema dataSchema, HoodieTableConfig cfg, TypedProperties properties) {
    ArrayList<String> requiredFields = new ArrayList<>();

    if (cfg.populateMetaFields()) {
      requiredFields.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    } else {
      Option<String[]> fields = cfg.getRecordKeyFields();
      if (fields.isPresent()) {
        requiredFields.addAll(Arrays.asList(fields.get()));
      }
    }

    List<String> orderingFields = cfg.getOrderingFields();
    requiredFields.addAll(orderingFields);
    return requiredFields.toArray(new String[0]);
  }

  /**
   * The record type handled by the current merger.
   * SPARK, AVRO, FLINK
   */
  HoodieRecordType getRecordType();

  /**
   * The kind of merging strategy this recordMerger belongs to. An UUID represents merging strategy.
   */
  String getMergingStrategy();

  static String getRecordMergeStrategyId(RecordMergeMode mergeMode, String payloadClassName,
                                         String recordMergeStrategyId, HoodieTableVersion tableVersion) {
    switch (mergeMode) {
      case COMMIT_TIME_ORDERING:
        return COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
      case EVENT_TIME_ORDERING:
        return EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
      case CUSTOM:
      default:
        return getCustomRecordMergeStrategyId(payloadClassName, recordMergeStrategyId, tableVersion);
    }
  }

  static String getCustomRecordMergeStrategyId(String payloadClassName, String recordMergeStrategyId, HoodieTableVersion tableVersion) {
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      // For table version 8, we give preference to input recordMergeStrategyId over payload based strategy
      if (nonEmpty(recordMergeStrategyId)) {
        return recordMergeStrategyId;
      } else if (nonEmpty(payloadClassName)) {
        return PAYLOAD_BASED_MERGE_STRATEGY_UUID;
      }
      return null;
    } else {
      // For table version 6, we give preference to payload based strategy over input recordMergeStrategyId
      if (nonEmpty(payloadClassName)) {
        return PAYLOAD_BASED_MERGE_STRATEGY_UUID;
      } else if (nonEmpty(recordMergeStrategyId)) {
        return recordMergeStrategyId;
      }
      return null;
    }
  }
}
