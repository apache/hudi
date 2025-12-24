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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;

public class EventTimeHoodieSparkRecordMerger extends HoodieSparkRecordMerger {

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
    ValidationUtils.checkArgument(older.getRecordType() == HoodieRecordType.SPARK);
    ValidationUtils.checkArgument(newer.getRecordType() == HoodieRecordType.SPARK);

    if (newer.getData() == null) {
      // Delete record
      return Option.of(Pair.of(newer, newSchema));
    }
    if (older.getData() == null) {
      // use natural order for delete record
      return Option.of(Pair.of(newer, newSchema));
    }
    if (older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props)) > 0) {
      return Option.of(Pair.of(older, oldSchema));
    } else if (older.getOrderingValue(oldSchema, props).compareTo(newer.getOrderingValue(newSchema, props)) < 0) {
      return Option.of(Pair.of(newer, newSchema));
    } else {
      Comparable olderCommitTime = (Comparable)older.getColumnValues(oldSchema,
          new String[]{HoodieRecord.COMMIT_TIME_METADATA_FIELD}, false)[0];
      Comparable newerCommitTime = (Comparable)newer.getColumnValues(newSchema,
          new String[]{HoodieRecord.COMMIT_TIME_METADATA_FIELD}, false)[0];
      if (olderCommitTime.compareTo(newerCommitTime) > 0) {
        return Option.of(Pair.of(older, oldSchema));
      } else if (olderCommitTime.compareTo(newerCommitTime)  < 0) {
        return Option.of(Pair.of(newer, newSchema));
      } else {
        UTF8String oldCommitSeqNo = (UTF8String)older.getColumnValues(oldSchema,
            new String[] {HoodieRecord.COMMIT_SEQNO_METADATA_FIELD}, false)[0];
        UTF8String newCommitSeqNo = (UTF8String)newer.getColumnValues(newSchema,
            new String[] {HoodieRecord.COMMIT_SEQNO_METADATA_FIELD}, false)[0];
        Long olderCommitSeqNoValue = Long.valueOf(oldCommitSeqNo.toString());
        Long newerCommitSeqNoValue = Long.valueOf(newCommitSeqNo.toString());
        if (olderCommitSeqNoValue.compareTo(newerCommitSeqNoValue) > 0) {
          return Option.of(Pair.of(older, oldSchema));
        } else {
          return Option.of(Pair.of(newer, newSchema));
        }
      }
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.SPARK;
  }
}