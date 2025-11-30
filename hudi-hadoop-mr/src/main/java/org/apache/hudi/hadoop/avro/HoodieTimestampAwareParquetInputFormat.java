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

package org.apache.hudi.hadoop.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;

import java.io.IOException;

/**
 * To resolve issue <a href="https://issues.apache.org/jira/browse/HUDI-83">Fix Timestamp/Date type read by Hive3</a>,
 * we need to handle timestamp types separately based on the parquet-avro approach.
 */
public class HoodieTimestampAwareParquetInputFormat extends ParquetInputFormat<ArrayWritable> {
  private final Option<InternalSchema> internalSchemaOption;
  private final Option<Schema> dataSchema;

  public HoodieTimestampAwareParquetInputFormat(Option<InternalSchema> internalSchemaOption, Option<Schema> dataSchema) {
    super();
    this.internalSchemaOption = internalSchemaOption;
    this.dataSchema = dataSchema;
  }

  @Override
  public RecordReader<Void, ArrayWritable> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);
    return new HoodieAvroParquetReader(inputSplit, conf, internalSchemaOption, dataSchema);
  }
}
