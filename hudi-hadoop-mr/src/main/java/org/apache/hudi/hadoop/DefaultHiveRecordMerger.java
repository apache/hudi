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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Record merger for hive that implements the default merger strategy
 */
public class DefaultHiveRecordMerger extends HoodieHiveRecordMerger {

  private String[] orderingFields;

  @Override
  public Pair<HoodieRecord, Schema> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecord.HoodieRecordType.HIVE);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecord.HoodieRecordType.HIVE);
    if (newer instanceof HoodieHiveRecord) {
      HoodieHiveRecord newHiveRecord = (HoodieHiveRecord) newer;
      if (newHiveRecord.isDelete(newSchema, props)) {
        return Pair.of(new HoodieEmptyRecord<>(newer.getKey(), HoodieOperation.DELETE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.HIVE), newSchema);
      }
    } else if (newer.getData() == null) {
      return Pair.of(new HoodieEmptyRecord<>(newer.getKey(), HoodieOperation.DELETE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.HIVE), newSchema);
    }

    if (older instanceof HoodieHiveRecord) {
      HoodieHiveRecord oldHiveRecord = (HoodieHiveRecord) older;
      if (oldHiveRecord.isDelete(oldSchema, props)) {
        return Pair.of(newer, newSchema);
      }
    } else if (older.getData() == null) {
      return Pair.of(new HoodieEmptyRecord<>(newer.getKey(), HoodieOperation.DELETE, OrderingValues.getDefault(), HoodieRecord.HoodieRecordType.HIVE), newSchema);
    }
    if (orderingFields == null) {
      orderingFields = ConfigUtils.getOrderingFields(props);
    }
    if (older.getOrderingValue(oldSchema, props, orderingFields).compareTo(newer.getOrderingValue(newSchema, props, orderingFields)) > 0) {
      return Pair.of(older, oldSchema);
    } else {
      return Pair.of(newer, newSchema);
    }
  }

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }
}
