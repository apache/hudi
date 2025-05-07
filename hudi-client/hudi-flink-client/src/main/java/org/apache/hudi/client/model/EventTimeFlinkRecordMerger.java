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

package org.apache.hudi.client.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Record merger for Flink HoodieRecord that implements event time based merging strategy.
 */
public class EventTimeFlinkRecordMerger extends HoodieFlinkRecordMerger {

  public static final EventTimeFlinkRecordMerger INSTANCE = new EventTimeFlinkRecordMerger();

  @Override
  public String getMergingStrategy() {
    return EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(
      HoodieRecord older,
      Schema oldSchema,
      HoodieRecord newer,
      Schema newSchema,
      TypedProperties props) throws IOException {
    // Note: can be removed if we can ensure the type from invoker.
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecord.HoodieRecordType.FLINK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecord.HoodieRecordType.FLINK);

    if (older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props)) > 0) {
      return Option.of(Pair.of(older, oldSchema));
    } else {
      return Option.of(Pair.of(newer, newSchema));
    }
  }
}
