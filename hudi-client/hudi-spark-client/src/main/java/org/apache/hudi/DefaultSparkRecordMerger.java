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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.merge.SparkRecordMergingUtils;

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * Record merger for spark that implements the default merger strategy
 */
public class DefaultSparkRecordMerger extends HoodieSparkRecordMerger {

  private String[] orderingFields;

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    Option<Pair<HoodieRecord, Schema>> deleteHandlingResult = handleDeletes(older, oldSchema, newer, newSchema, props);
    if (deleteHandlingResult != null) {
      return deleteHandlingResult;
    }

    if (orderingFields == null) {
      orderingFields = ConfigUtils.getOrderingFields(props);
    }
    if (older.getOrderingValue(oldSchema, props, orderingFields).compareTo(newer.getOrderingValue(newSchema, props, orderingFields)) > 0) {
      return Option.of(Pair.of(older, oldSchema));
    } else {
      return Option.of(Pair.of(newer, newSchema));
    }
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> partialMerge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, Schema readerSchema, TypedProperties props) throws IOException {
    Option<Pair<HoodieRecord, Schema>> deleteHandlingResult = handleDeletes(older, oldSchema, newer, newSchema, props);
    if (deleteHandlingResult != null) {
      return deleteHandlingResult;
    }

    if (orderingFields == null) {
      orderingFields = ConfigUtils.getOrderingFields(props);
    }
    if (older.getOrderingValue(oldSchema, props, orderingFields).compareTo(newer.getOrderingValue(newSchema, props, orderingFields)) > 0) {
      return Option.of(SparkRecordMergingUtils.mergePartialRecords(
          (HoodieSparkRecord) newer, newSchema, (HoodieSparkRecord) older, oldSchema, readerSchema, props));
    } else {
      return Option.of(SparkRecordMergingUtils.mergePartialRecords(
          (HoodieSparkRecord) older, oldSchema, (HoodieSparkRecord) newer, newSchema, readerSchema, props));
    }
  }
}