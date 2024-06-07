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

package org.apache.hudi;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.merge.SparkRecordMergingUtils;

import org.apache.avro.Schema;

import java.io.IOException;

public class HoodieSparkRecordMerger implements HoodieRecordMerger {

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.SPARK);
    try {
      Comparable olderOrderingValue = older.getOrderingValue(oldSchema, props);
      Comparable newerOrderingValue = newer.getOrderingValue(newSchema, props);
      if (olderOrderingValue.compareTo(newerOrderingValue) > 0) {
        if (older instanceof HoodieEmptyRecord) {
          newer.setOrderingValue(olderOrderingValue);
          return Option.of(Pair.of(newer, newSchema));
        }
        HoodieSparkRecord oldSparkRecord = (HoodieSparkRecord) older;
        if (oldSparkRecord.isDelete(oldSchema, props)) {
          newer.setOrderingValue(olderOrderingValue);
          return Option.of(Pair.of(newer, newSchema));
        }
        return Option.of(Pair.of(older, oldSchema));
      } else {
        return Option.of(Pair.of(newer, newSchema));
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      throw e;
    }
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> partialMerge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, Schema readerSchema, TypedProperties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.SPARK);

    if (newer instanceof HoodieSparkRecord) {
      HoodieSparkRecord newSparkRecord = (HoodieSparkRecord) newer;
      if (newSparkRecord.isDeleted()) {
        // Delete record
        return Option.empty();
      }
    } else {
      if (newer.getData() == null) {
        // Delete record
        return Option.empty();
      }
    }

    if (older instanceof HoodieSparkRecord) {
      HoodieSparkRecord oldSparkRecord = (HoodieSparkRecord) older;
      if (oldSparkRecord.isDeleted()) {
        // use natural order for delete record
        return Option.of(Pair.of(newer, newSchema));
      }
    } else {
      if (older.getData() == null) {
        // use natural order for delete record
        return Option.of(Pair.of(newer, newSchema));
      }
    }
    if (older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props)) > 0) {
      return Option.of(SparkRecordMergingUtils.mergePartialRecords(
          (HoodieSparkRecord) newer, newSchema, (HoodieSparkRecord) older, oldSchema, readerSchema, props));
    } else {
      return Option.of(SparkRecordMergingUtils.mergePartialRecords(
          (HoodieSparkRecord) older, oldSchema, (HoodieSparkRecord) newer, newSchema, readerSchema, props));
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }
}