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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
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
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;

public class HoodieWriteableTestTable extends HoodieTestTable {
  private static final Logger LOG = LogManager.getLogger(HoodieWriteableTestTable.class);

  protected final Schema schema;
  protected final BloomFilter filter;

  protected HoodieWriteableTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient, Schema schema, BloomFilter filter) {
    super(basePath, fs, metaClient);
    this.schema = schema;
    this.filter = filter;
  }

  @Override
  public HoodieWriteableTestTable addCommit(String instantTime) throws Exception {
    return (HoodieWriteableTestTable) super.addCommit(instantTime);
  }

  @Override
  public HoodieWriteableTestTable forCommit(String instantTime) {
    return (HoodieWriteableTestTable) super.forCommit(instantTime);
  }

  public HoodieWriteableTestTable withInserts(String partition, String fileId, List<HoodieRecord> records, TaskContextSupplier contextSupplier) throws Exception {
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
        config, schema, contextSupplier)) {
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
