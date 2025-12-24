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

import org.apache.avro.Schema;
import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.io.Serializable;

/**
 * HoodieMerge defines how to merge two records. It is a stateless component.
 * It can implement the merging logic of HoodieRecord of different engines
 * and avoid the performance consumption caused by the serialization/deserialization of Avro payload.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface HoodieRecordMerger extends Serializable {

  String DEFAULT_MERGER_STRATEGY_UUID = "eeb8d96f-b1e4-49fd-bbf8-28ac514178e5";
  // Uses event time ordering to determine which record is chosen
  String EVENT_TIME_BASED_MERGE_STRATEGY_UUID = "53ce6957-2a07-4c95-bf1e-548663905b70";

  // Always chooses the most recently written record
  String COMMIT_TIME_BASED_MERGE_STRATEGY_UUID = "ce9acb64-bde0-424c-9b91-f6ebba25356d";

  // Use overwrite with latest record
  // String OVERWRITE_LATEST_MERGE_STRATEGY_UUID = "9b450e53-5324-424b-a486-af177c427e49";

  // Uses customized merge strategy to merge records
  String CUSTOM_MERGE_STRATEGY_UUID = "1897ef5f-18bc-4557-939c-9d6a8afd1519";

  // partial update
  String PARTIAL_UPDATE_MERGE_STRATEGY_UUID = "759aa20e-3966-4244-ba6f-c1c2300e8784";

  // partial update support multi ts
  String PARTIAL_UPDATE_MULTI_TS_MERGE_STRATEGY_UUID = "f80795fd-d331-4a78-97c2-6f19e750ac29";

  // Use avro payload to merge records
  String PAYLOAD_BASED_MERGE_STRATEGY_UUID = "00000000-0000-0000-0000-000000000000";

  /**
   * This method converges combineAndGetUpdateValue and precombine from HoodiePayload.
   * It'd be associative operation: f(a, f(b, c)) = f(f(a, b), c) (which we can translate as having 3 versions A, B, C
   * of the single record, both orders of operations applications have to yield the same result)
   */
  Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException;

  /**
   * The record type handled by the current merger.
   * SPARK, AVRO, FLINK
   */
  HoodieRecordType getRecordType();

  /**
   * The kind of merging strategy this recordMerger belongs to. An UUID represents merging strategy.
   */
  String getMergingStrategy();
}
