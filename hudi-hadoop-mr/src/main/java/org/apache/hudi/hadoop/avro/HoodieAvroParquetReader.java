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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.HoodieColumnProjectionUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.parquet.avro.AvroAdapter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

public class HoodieAvroParquetReader extends RecordReader<Void, ArrayWritable> {

  private final ParquetRecordReader<GenericData.Record> parquetRecordReader;
  private Schema baseSchema;
  private static final AvroAdapter ADAPTER = AvroAdapter.getAdapter();

  public HoodieAvroParquetReader(InputSplit inputSplit, Configuration conf, Option<InternalSchema> internalSchemaOption) throws IOException {
    // get base schema
    ParquetMetadata fileFooter =
        ParquetFileReader.readFooter(conf, ((ParquetInputSplit) inputSplit).getPath(), ParquetMetadataConverter.NO_FILTER);
    MessageType messageType = fileFooter.getFileMetaData().getSchema();
    baseSchema = ADAPTER.getAvroSchemaConverter(new HadoopStorageConfiguration(conf)).convert(messageType);

    if (internalSchemaOption.isPresent()) {
      // do schema reconciliation in case there exists read column which is not in the file schema.
      InternalSchema mergedInternalSchema = new InternalSchemaMerger(
              AvroInternalSchemaConverter.convert(baseSchema),
              internalSchemaOption.get(),
              true,
              true).mergeSchema();
      baseSchema = AvroInternalSchemaConverter.convert(mergedInternalSchema, baseSchema.getFullName());
    }

    // if exists read columns, we need to filter columns.
    List<String> readColNames = Arrays.asList(HoodieColumnProjectionUtils.getReadColumnNames(conf));
    if (!readColNames.isEmpty()) {
      Schema filterSchema = HoodieAvroUtils.generateProjectionSchema(baseSchema, readColNames);
      AvroReadSupport.setAvroReadSchema(conf, filterSchema);
      AvroReadSupport.setRequestedProjection(conf, filterSchema);
    }

    parquetRecordReader = new ParquetRecordReader<>(new AvroReadSupport<>(), getFilter(conf));
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    parquetRecordReader.initialize(split, context);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return parquetRecordReader.nextKeyValue();
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return parquetRecordReader.getCurrentKey();
  }

  @Override
  public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
    GenericRecord record = parquetRecordReader.getCurrentValue();
    return (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(record, baseSchema, true);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return parquetRecordReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    parquetRecordReader.close();
  }
}