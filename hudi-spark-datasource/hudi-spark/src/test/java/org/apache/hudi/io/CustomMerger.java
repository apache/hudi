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

package org.apache.hudi.io;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.UUID;

public class CustomMerger implements HoodieRecordMerger {
  private static final String STRATEGY_ID = UUID.randomUUID().toString();

  public static String getStrategyId() {
    return STRATEGY_ID;
  }

  @Override
  public Pair<HoodieRecord, Schema> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    Long olderTimestamp = (Long) older.getOrderingValue(oldSchema, props, new String[0]);
    Long newerTimestamp = (Long) newer.getOrderingValue(oldSchema, props, new String[0]);
    if (olderTimestamp.equals(newerTimestamp)) {
      // If the timestamps are the same, we do not update
      return Pair.of(older, oldSchema);
    } else if (olderTimestamp < newerTimestamp) {
      // Custom merger chooses record with lower ordering value
      return Pair.of(older, oldSchema);
    } else {
      // Custom merger chooses record with lower ordering value
      return Pair.of(newer, newSchema);
    }
  }

  @Override
  public HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.AVRO;
  }

  @Override
  public String getMergingStrategy() {
    return STRATEGY_ID;
  }
}
