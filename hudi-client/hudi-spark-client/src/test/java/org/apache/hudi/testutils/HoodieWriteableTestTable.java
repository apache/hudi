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

package org.apache.hudi.testutils;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.io.storage.HoodieAvroParquetConfig;
import org.apache.hudi.io.storage.HoodieParquetWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;

public class HoodieWriteableTestTable extends HoodieTestTable {
  private static final Logger LOG = LogManager.getLogger(HoodieWriteableTestTable.class);

  private final Schema schema;
  private final BloomFilter filter;

  private HoodieWriteableTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter) {
    super(basePath, fs, metaClient);
    this.schema = schema;
    this.filter = filter;
  }

  public static HoodieWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter) {
    return new HoodieWriteableTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient, schema, filter);
  }

  public static HoodieWriteableTestTable of(HoodieTableMetaClient metaClient, Schema schema) {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(10000, 0.0000001, -1, BloomFilterTypeCode.SIMPLE.name());
    return of(metaClient, schema, filter);
  }

  public static HoodieWriteableTestTable of(HoodieTable hoodieTable, Schema schema) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema);
  }

  public static HoodieWriteableTestTable of(HoodieTable hoodieTable, Schema schema, BloomFilter filter) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema, filter);
  }

  @Override
  public HoodieWriteableTestTable addCommit(String instantTime) throws Exception {
    return (HoodieWriteableTestTable) super.addCommit(instantTime);
  }

  @Override
  public HoodieWriteableTestTable forCommit(String instantTime) {
    return (HoodieWriteableTestTable) super.forCommit(instantTime);
  }

  public String getFileIdWithInserts(String partition) throws Exception {
    return getFileIdWithInserts(partition, new HoodieRecord[0]);
  }

  public String getFileIdWithInserts(String partition, HoodieRecord... records) throws Exception {
    return getFileIdWithInserts(partition, Arrays.asList(records));
  }

  public String getFileIdWithInserts(String partition, List<HoodieRecord> records) throws Exception {
    String fileId = UUID.randomUUID().toString();
    withInserts(partition, fileId, records);
    return fileId;
  }

  public HoodieWriteableTestTable withInserts(String partition, String fileId) throws Exception {
    return withInserts(partition, fileId, new HoodieRecord[0]);
  }

  public HoodieWriteableTestTable withInserts(String partition, String fileId, HoodieRecord... records) throws Exception {
    return withInserts(partition, fileId, Arrays.asList(records));
  }

  public HoodieWriteableTestTable withInserts(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    FileCreateUtils.createPartitionMetaFile(basePath, partition);
    String fileName = baseFileName(currentInstantTime, fileId);

    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        new AvroSchemaConverter().convert(schema), schema, filter);
    HoodieAvroParquetConfig config = new HoodieAvroParquetConfig(writeSupport, CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
        new Configuration(), Double.parseDouble(HoodieStorageConfig.DEFAULT_STREAM_COMPRESSION_RATIO));
    try (HoodieParquetWriter writer = new HoodieParquetWriter(
        currentInstantTime,
        new Path(Paths.get(basePath, partition, fileName).toString()),
        config, schema, new SparkTaskContextSupplier())) {
      int seqId = 1;
      for (HoodieRecord record : records) {
        GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
        HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, currentInstantTime, String.valueOf(seqId++));
        HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), fileName);
        writer.writeAvro(record.getRecordKey(), avroRecord);
        filter.add(record.getRecordKey());
      }
    }

    return this;
  }

  public HoodieWriteableTestTable withLogAppends(HoodieRecord... records) throws Exception {
    return withLogAppends(Arrays.asList(records));
  }

  public HoodieWriteableTestTable withLogAppends(List<HoodieRecord> records) throws Exception {
    for (List<HoodieRecord> groupedRecords: records.stream()
        .collect(Collectors.groupingBy(HoodieRecord::getCurrentLocation)).values()) {
      appendRecordsToLogFile(groupedRecords);
    }
    return this;
  }

  private void appendRecordsToLogFile(List<HoodieRecord> groupedRecords) throws Exception {
    String partitionPath = groupedRecords.get(0).getPartitionPath();
    HoodieRecordLocation location = groupedRecords.get(0).getCurrentLocation();
    try (HoodieLogFormat.Writer logWriter = HoodieLogFormat.newWriterBuilder().onParentPath(new Path(basePath, partitionPath))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(location.getFileId())
        .overBaseCommit(location.getInstantTime()).withFs(fs).build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, location.getInstantTime());
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      logWriter.appendBlock(new HoodieAvroDataBlock(groupedRecords.stream().map(r -> {
        try {
          GenericRecord val = (GenericRecord) r.getData().getInsertValue(schema).get();
          HoodieAvroUtils.addHoodieKeyToRecord(val, r.getRecordKey(), r.getPartitionPath(), "");
          return (IndexedRecord) val;
        } catch (IOException e) {
          LOG.warn("Failed to convert record " + r.toString(), e);
          return null;
        }
      }).collect(Collectors.toList()), header));
    }
  }
}
