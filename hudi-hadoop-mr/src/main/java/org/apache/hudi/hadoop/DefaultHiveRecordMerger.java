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
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;

import java.io.IOException;

/**
 * Record merger for hive that implements the default merger strategy
 */
public class DefaultHiveRecordMerger extends HoodieHiveRecordMerger {

  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    // If the new record is a commit time ordered delete, it will always be used regardless of the ordering value of the old record.
    // If the old record was a commit time ordered delete, the newer record will be returned because it occurred after that delete and ordering time comparison is not needed.
    if (HoodieRecordMerger.mergingCommitTimeOrderedDelete(older, newer)) {
      return newer;
    }
    if (older.getOrderingValue().compareTo(newer.getOrderingValue()) > 0) {
      return older;
    } else {
      return newer;
    }
  }

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }
}
