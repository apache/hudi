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
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.merge.SparkRecordMergingUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

/**
 * Record merger for spark that implements the default merger strategy
 */
public class DefaultSparkRecordMerger extends HoodieSparkRecordMerger {

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    if (HoodieRecordMerger.isCommitTimeOrderingDelete(older, newer)) {
      return newer;
    }
    if (older.getOrderingValue().compareTo(newer.getOrderingValue()) > 0) {
      return older;
    } else {
      return newer;
    }
  }

  @Override
  public <T> BufferedRecord<T> partialMerge(BufferedRecord<T> older, BufferedRecord<T> newer, Schema readerSchema, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    if (HoodieRecordMerger.isCommitTimeOrderingDelete(older, newer)) {
      return newer;
    }
    if (older.getOrderingValue().compareTo(newer.getOrderingValue()) > 0) {
      if (older.isDelete() || newer.isDelete()) {
        return older;
      }
      Schema oldSchema = recordContext.getSchemaFromBufferRecord(older);
      Schema newSchema = recordContext.getSchemaFromBufferRecord(newer);
      return (BufferedRecord<T>) SparkRecordMergingUtils.mergePartialRecords((BufferedRecord<InternalRow>) newer, newSchema,
          (BufferedRecord<InternalRow>) older, oldSchema, readerSchema, (RecordContext<InternalRow>) recordContext);
    } else {
      if (newer.isDelete() || older.isDelete()) {
        return newer;
      }
      Schema oldSchema = recordContext.getSchemaFromBufferRecord(older);
      Schema newSchema = recordContext.getSchemaFromBufferRecord(newer);
      return (BufferedRecord<T>) SparkRecordMergingUtils.mergePartialRecords((BufferedRecord<InternalRow>) older, oldSchema,
          (BufferedRecord<InternalRow>) newer, newSchema, readerSchema, (RecordContext<InternalRow>) recordContext);
    }
  }
}