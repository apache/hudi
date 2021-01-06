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

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;

public class HoodieFlinkWriteableTestTable extends org.apache.hudi.common.testutils.HoodieTestTable {
  private static final org.apache.log4j.Logger LOG = org.apache.log4j.LogManager.getLogger(org.apache.hudi.testutils.HoodieFlinkWriteableTestTable.class);

  private final org.apache.avro.Schema schema;
  private final org.apache.hudi.common.bloom.BloomFilter filter;

  private HoodieFlinkWriteableTestTable(String basePath, org.apache.hadoop.fs.FileSystem fs, org.apache.hudi.common.table.HoodieTableMetaClient metaClient, org.apache.avro.Schema schema, org.apache.hudi.common.bloom.BloomFilter filter) {
    super(basePath, fs, metaClient);
    this.schema = schema;
    this.filter = filter;
  }

  public static org.apache.hudi.testutils.HoodieFlinkWriteableTestTable of(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, org.apache.avro.Schema schema, org.apache.hudi.common.bloom.BloomFilter filter) {
    return new org.apache.hudi.testutils.HoodieFlinkWriteableTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient, schema, filter);
  }

  public static org.apache.hudi.testutils.HoodieFlinkWriteableTestTable of(org.apache.hudi.common.table.HoodieTableMetaClient metaClient, org.apache.avro.Schema schema) {
    org.apache.hudi.common.bloom.BloomFilter filter = org.apache.hudi.common.bloom.BloomFilterFactory
        .createBloomFilter(10000, 0.0000001, -1, org.apache.hudi.common.bloom.BloomFilterTypeCode.SIMPLE.name());
    return of(metaClient, schema, filter);
  }

  public static org.apache.hudi.testutils.HoodieFlinkWriteableTestTable of(org.apache.hudi.table.HoodieTable hoodieTable, org.apache.avro.Schema schema) {
    org.apache.hudi.common.table.HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema);
  }

  public static org.apache.hudi.testutils.HoodieFlinkWriteableTestTable of(org.apache.hudi.table.HoodieTable hoodieTable, org.apache.avro.Schema schema, org.apache.hudi.common.bloom.BloomFilter filter) {
    org.apache.hudi.common.table.HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return of(metaClient, schema, filter);
  }

  @Override
  public org.apache.hudi.testutils.HoodieFlinkWriteableTestTable addCommit(String instantTime) throws Exception {
    return (org.apache.hudi.testutils.HoodieFlinkWriteableTestTable) super.addCommit(instantTime);
  }

  @Override
  public org.apache.hudi.testutils.HoodieFlinkWriteableTestTable forCommit(String instantTime) {
    return (org.apache.hudi.testutils.HoodieFlinkWriteableTestTable) super.forCommit(instantTime);
  }

  public String getFileIdWithInserts(String partition) throws Exception {
    return getFileIdWithInserts(partition, new org.apache.hudi.common.model.HoodieRecord[0]);
  }

  public String getFileIdWithInserts(String partition, org.apache.hudi.common.model.HoodieRecord... records) throws Exception {
    return getFileIdWithInserts(partition, java.util.Arrays.asList(records));
  }

  public String getFileIdWithInserts(String partition, java.util.List<org.apache.hudi.common.model.HoodieRecord> records) throws Exception {
    String fileId = java.util.UUID.randomUUID().toString();
    withInserts(partition, fileId, records);
    return fileId;
  }

  public org.apache.hudi.testutils.HoodieFlinkWriteableTestTable withInserts(String partition, String fileId) throws Exception {
    return withInserts(partition, fileId, new org.apache.hudi.common.model.HoodieRecord[0]);
  }

  public org.apache.hudi.testutils.HoodieFlinkWriteableTestTable withInserts(String partition, String fileId, org.apache.hudi.common.model.HoodieRecord... records) throws Exception {
    return withInserts(partition, fileId, java.util.Arrays.asList(records));
  }

  public org.apache.hudi.testutils.HoodieFlinkWriteableTestTable withInserts(String partition, String fileId, java.util.List<org.apache.hudi.common.model.HoodieRecord> records) throws Exception {
    org.apache.hudi.common.testutils.FileCreateUtils.createPartitionMetaFile(basePath, partition);
    String fileName = baseFileName(currentInstantTime, fileId);

    org.apache.hudi.avro.HoodieAvroWriteSupport writeSupport = new org.apache.hudi.avro.HoodieAvroWriteSupport(
        new org.apache.parquet.avro.AvroSchemaConverter().convert(schema), schema, filter);
    org.apache.hudi.io.storage.HoodieAvroParquetConfig config = new org.apache.hudi.io.storage.HoodieAvroParquetConfig(writeSupport, org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP,
        org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE, org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
        new org.apache.hadoop.conf.Configuration(), Double.parseDouble(org.apache.hudi.config.HoodieStorageConfig.DEFAULT_STREAM_COMPRESSION_RATIO));
    try (org.apache.hudi.io.storage.HoodieParquetWriter writer = new org.apache.hudi.io.storage.HoodieParquetWriter(
        currentInstantTime,
        new org.apache.hadoop.fs.Path(java.nio.file.Paths.get(basePath, partition, fileName).toString()),
        config, schema, new org.apache.hudi.client.FlinkTaskContextSupplier(null))) {
      int seqId = 1;
      for (org.apache.hudi.common.model.HoodieRecord record : records) {
        org.apache.avro.generic.GenericRecord avroRecord = (org.apache.avro.generic.GenericRecord) record.getData().getInsertValue(schema).get();
        org.apache.hudi.avro.HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, currentInstantTime, String.valueOf(seqId++));
        org.apache.hudi.avro.HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), fileName);
        writer.writeAvro(record.getRecordKey(), avroRecord);
        filter.add(record.getRecordKey());
      }
    }

    return this;
  }

  public org.apache.hudi.testutils.HoodieFlinkWriteableTestTable withLogAppends(java.util.List<org.apache.hudi.common.model.HoodieRecord> records) throws Exception {
    for (java.util.List<org.apache.hudi.common.model.HoodieRecord> groupedRecords: records.stream()
        .collect(java.util.stream.Collectors.groupingBy(org.apache.hudi.common.model.HoodieRecord::getCurrentLocation)).values()) {
      appendRecordsToLogFile(groupedRecords);
    }
    return this;
  }

  private void appendRecordsToLogFile(java.util.List<org.apache.hudi.common.model.HoodieRecord> groupedRecords) throws Exception {
    String partitionPath = groupedRecords.get(0).getPartitionPath();
    org.apache.hudi.common.model.HoodieRecordLocation location = groupedRecords.get(0).getCurrentLocation();
    try (org.apache.hudi.common.table.log.HoodieLogFormat.Writer logWriter = org.apache.hudi.common.table.log.HoodieLogFormat.newWriterBuilder().onParentPath(new org.apache.hadoop.fs.Path(basePath, partitionPath))
        .withFileExtension(org.apache.hudi.common.model.HoodieLogFile.DELTA_EXTENSION).withFileId(location.getFileId())
        .overBaseCommit(location.getInstantTime()).withFs(fs).build()) {
      java.util.Map<org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType, String> header = new java.util.HashMap<>();
      header.put(org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, location.getInstantTime());
      header.put(org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      logWriter.appendBlock(new org.apache.hudi.common.table.log.block.HoodieAvroDataBlock(groupedRecords.stream().map(r -> {
        try {
          org.apache.avro.generic.GenericRecord val = (org.apache.avro.generic.GenericRecord) r.getData().getInsertValue(schema).get();
          org.apache.hudi.avro.HoodieAvroUtils.addHoodieKeyToRecord(val, r.getRecordKey(), r.getPartitionPath(), "");
          return (org.apache.avro.generic.IndexedRecord) val;
        } catch (java.io.IOException e) {
          LOG.warn("Failed to convert record " + r.toString(), e);
          return null;
        }
      }).collect(java.util.stream.Collectors.toList()), header));
    }
  }
}
