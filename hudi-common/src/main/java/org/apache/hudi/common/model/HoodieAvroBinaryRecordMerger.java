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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;

import java.io.IOException;

public class HoodieAvroBinaryRecordMerger implements HoodieRecordMerger, OperationModeAwareness {
  public static final HoodieAvroBinaryRecordMerger INSTANCE = new HoodieAvroBinaryRecordMerger();

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    if (older == null || older.isDelete(oldSchema, props)) {
      return Option.of(Pair.of(newer, newSchema));
    }
    Comparable oldOrderingValue = older.getOrderingValue(oldSchema, props);
    Comparable newOrderingValue = newer.getOrderingValue(newSchema, props);
    // Handle delete cases.
    if (older.isDelete(oldSchema, props) && oldOrderingValue == OrderingValues.getDefault()
        || newer.isDelete(newSchema, props) && newOrderingValue == OrderingValues.getDefault()) {
      return Option.of(Pair.of(newer, newSchema));
    }
    // Handle regular cases.
    if (newOrderingValue.compareTo(oldOrderingValue) >= 0) {
      return Option.of(Pair.of(newer, newSchema));
    }
    return Option.of(Pair.of(older, oldSchema));
  }

  @Override
  public HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.AVRO_BINARY;
  }

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public HoodieRecordMerger asPreCombiningMode() {
    return HoodiePreCombineAvroBinaryRecordMerger.INSTANCE;
  }
}
