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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.merge.SparkRecordMergingUtils;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

/**
 * Spark merger that always chooses the newer record
 */
public class OverwriteWithLatestSparkRecordMerger extends HoodieSparkRecordMerger {

  @Override
  public String getMergingStrategy() {
    return COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    return newer;
  }

  @Override
  public <T> BufferedRecord<T> partialMerge(BufferedRecord<T> older, BufferedRecord<T> newer, Schema readerSchema, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    if (newer.isDelete()) {
      return newer;
    }
    HoodieSchema oldSchema = recordContext.getSchemaFromBufferRecord(older);
    HoodieSchema newSchema = recordContext.getSchemaFromBufferRecord(newer);
    return (BufferedRecord<T>) SparkRecordMergingUtils.mergePartialRecords((BufferedRecord<InternalRow>) older, oldSchema.toAvroSchema(),
        (BufferedRecord<InternalRow>) newer, newSchema.toAvroSchema(), readerSchema, (RecordContext<InternalRow>) recordContext);
  }
}
