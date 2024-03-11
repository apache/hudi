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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.hadoop.HoodieColumnProjectionUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getTableMetaClientForBasePathUnchecked;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.addPartitionFields;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.avroToArrayWritable;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.constructHiveOrderedSchema;
import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getNameToFieldMap;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

public class HoodieAvroParquetReader extends RecordReader<Void, ArrayWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieAvroParquetReader.class);
  private final ParquetRecordReader<GenericData.Record> parquetRecordReader;
  private Schema baseSchema;

  public HoodieAvroParquetReader(InputSplit inputSplit, Configuration conf) throws IOException {
    Path filePath = ((ParquetInputSplit) inputSplit).getPath();
    Schema writerSchema;
    try {
      HoodieTableMetaClient metaClient = getTableMetaClientForBasePathUnchecked(conf, filePath);
      writerSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
      LOG.warn("Got writer schema from table schema: {}", writerSchema);
    } catch (Exception e) {
      LOG.error("Failed to get writer schema from table schema", e);
      LOG.warn("Falling back to reading writer schema from parquet file");
      ParquetMetadata fileFooter = ParquetFileReader.readFooter(conf, filePath, ParquetMetadataConverter.NO_FILTER);
      MessageType messageType = fileFooter.getFileMetaData().getSchema();
      writerSchema = new AvroSchemaConverter(conf).convert(messageType);
    }
    JobConf jobConf = new JobConf(conf);
    try {
      // Add partitioning fields to writer schema for resulting row to contain null values for these fields
      String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
      List<String> partitioningFields =
          partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
              : new ArrayList<>();
      LOG.warn("Got partitioning fields: {}", partitioningFields);
      writerSchema = addPartitionFields(writerSchema, partitioningFields);
      LOG.warn("Added partition fields to writer schema: {}", writerSchema);
    } catch (Exception e) {
      LOG.error("Failed to add partition fields to writer schema", e);
    }
    Map<String, Schema.Field> schemaFieldsMap = getNameToFieldMap(writerSchema);
    LOG.warn("Got schema fields map: {}", schemaFieldsMap);
    baseSchema = constructHiveOrderedSchema(writerSchema, schemaFieldsMap, jobConf.get(hive_metastoreConstants.META_TABLE_COLUMNS, EMPTY_STRING));
    LOG.warn("Got hive ordered schema: {}", baseSchema);

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
    return (ArrayWritable) avroToArrayWritable(record, baseSchema, true);
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