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

package org.apache.hudi;

import org.apache.avro.Schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;

public class HoodieSparkRecordMerger implements HoodieRecordMerger {

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older,
                                                  Schema oldSchema,
                                                  HoodieRecord newer,
                                                  Schema newSchema,
                                                  TypedProperties props) throws IOException {
    boolean isOldValid = isValid(older);
    boolean isNewValid = isValid(newer);
    boolean isOldDelete = isDelete(older, oldSchema, props);
    boolean isNewDelete = isDelete(newer, newSchema, props);

    if (!isNewValid) { // Old record will be kept for data safety.
      return Option.of(Pair.of(older, oldSchema));
    } else if (isNewDelete || !isOldValid || isOldDelete) { // New record will be returned.
      return Option.of(Pair.of(newer, newSchema));
    } else { // Do update, merge, comparison, etc.
      return handleUpdateCase(older, oldSchema, newer, newSchema, props);
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }

  private boolean isValid(HoodieRecord record) {
    return record.getRecordType() == HoodieRecordType.SPARK;
  }

  private boolean isDelete(HoodieRecord record, Schema schema, TypedProperties props) throws IOException {
    return HoodieOperation.isDelete(record.getOperation())
        || record instanceof HoodieEmptyRecord
        || record.isDelete(schema, props);
  }

  /**
   * Handle update case.
   */
  Option<Pair<HoodieRecord, Schema>> handleUpdateCase(HoodieRecord older,
                                                      Schema oldSchema,
                                                      HoodieRecord newer,
                                                      Schema newSchema,
                                                      TypedProperties props) {
    if (older.getOrderingValue(oldSchema, props).compareTo(
        newer.getOrderingValue(newSchema, props)) > 0) {
      return Option.of(Pair.of(older, oldSchema));
    } else {
      return Option.of(Pair.of(newer, newSchema));
    }
  }
}