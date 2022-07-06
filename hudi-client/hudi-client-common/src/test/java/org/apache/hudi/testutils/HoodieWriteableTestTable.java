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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.io.storage.HoodieAvroParquetWriter;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.io.storage.HoodieOrcConfig;
import org.apache.hudi.io.storage.HoodieOrcWriter;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileCreateUtils.baseFileName;

public class HoodieWriteableTestTable extends HoodieMetadataTestTable {
  private static final Logger LOG = LogManager.getLogger(HoodieWriteableTestTable.class);

  protected final Schema schema;
  protected final BloomFilter filter;
  protected final boolean populateMetaFields;

  protected HoodieWriteableTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient,
                                     Schema schema, BloomFilter filter) {
    this(basePath, fs, metaClient, schema, filter, null);
  }

  protected HoodieWriteableTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient, Schema schema,
                                     BloomFilter filter, HoodieTableMetadataWriter metadataWriter) {
    super(basePath, fs, metaClient, metadataWriter);
    this.schema = schema;
    this.filter = filter;
    this.populateMetaFields = metaClient.getTableConfig().populateMetaFields();
  }

  @Override
  public HoodieWriteableTestTable addCommit(String instantTime) throws Exception {
    return (HoodieWriteableTestTable) super.addCommit(instantTime);
  }

  @Override
  public HoodieWriteableTestTable forCommit(String instantTime) {
    return (HoodieWriteableTestTable) super.forCommit(instantTime);
  }

  public Path withInserts(String partition, String fileId, List<HoodieRecord> records, TaskContextSupplier contextSupplier) throws Exception {
    FileCreateUtils.createPartitionMetaFile(basePath, partition);
    String fileName = baseFileName(currentInstantTime, fileId);

    Path baseFilePath = new Path(Paths.get(basePath, partition, fileName).toString());
    if (this.fs.exists(baseFilePath)) {
      LOG.warn("Deleting the existing base file " + baseFilePath);
      this.fs.delete(baseFilePath, true);
    }

    if (HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().equals(HoodieFileFormat.PARQUET)) {
      HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
          new AvroSchemaConverter().convert(schema), schema, Option.of(filter));
      HoodieParquetConfig<HoodieAvroWriteSupport> config = new HoodieParquetConfig<>(writeSupport, CompressionCodecName.GZIP,
          ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
          new Configuration(), Double.parseDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue()));
      try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter<>(
          new Path(Paths.get(basePath, partition, fileName).toString()), config, currentInstantTime,
          contextSupplier, populateMetaFields)) {
        int seqId = 1;
        for (HoodieRecord record : records) {
          GenericRecord avroRecord = (GenericRecord) ((HoodieRecordPayload) record.getData()).getInsertValue(schema).get();
          if (populateMetaFields) {
            HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, currentInstantTime, String.valueOf(seqId++));
            HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), fileName);
            writer.writeAvro(record.getRecordKey(), avroRecord);
            filter.add(record.getRecordKey());
          } else {
            writer.writeAvro(record.getRecordKey(), avroRecord);
          }
        }
      }
    } else if (HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().equals(HoodieFileFormat.ORC)) {
      Configuration conf = new Configuration();
      int orcStripSize = Integer.parseInt(HoodieStorageConfig.ORC_STRIPE_SIZE.defaultValue());
      int orcBlockSize = Integer.parseInt(HoodieStorageConfig.ORC_BLOCK_SIZE.defaultValue());
      int maxFileSize = Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue());
      HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB, orcStripSize, orcBlockSize, maxFileSize, filter);
      try (HoodieOrcWriter writer = new HoodieOrcWriter(
          currentInstantTime,
          new Path(Paths.get(basePath, partition, fileName).toString()),
          config, schema, contextSupplier)) {
        int seqId = 1;
        for (HoodieRecord record : records) {
          GenericRecord avroRecord = (GenericRecord) ((HoodieRecordPayload) record.getData()).getInsertValue(schema).get();
          HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, currentInstantTime, String.valueOf(seqId++));
          HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), fileName);
          writer.writeAvro(record.getRecordKey(), avroRecord);
          filter.add(record.getRecordKey());
        }
      }
    }

    return baseFilePath;
  }

  public Map<String, List<HoodieLogFile>> withLogAppends(List<HoodieRecord> records) throws Exception {
    Map<String, List<HoodieLogFile>> partitionToLogfilesMap = new HashMap<>();
    for (List<HoodieRecord> groupedRecords : records.stream()
        .collect(Collectors.groupingBy(HoodieRecord::getCurrentLocation)).values()) {
      final Pair<String, HoodieLogFile> appendedLogFile = appendRecordsToLogFile(groupedRecords);
      partitionToLogfilesMap.computeIfAbsent(
          appendedLogFile.getKey(), k -> new ArrayList<>()).add(appendedLogFile.getValue());
    }
    return partitionToLogfilesMap;
  }

  private Pair<String, HoodieLogFile> appendRecordsToLogFile(List<HoodieRecord> groupedRecords) throws Exception {
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
          GenericRecord val = (GenericRecord) ((HoodieRecordPayload) r.getData()).getInsertValue(schema).get();
          HoodieAvroUtils.addHoodieKeyToRecord(val, r.getRecordKey(), r.getPartitionPath(), "");
          return (IndexedRecord) val;
        } catch (IOException e) {
          LOG.warn("Failed to convert record " + r.toString(), e);
          return null;
        }
      }).collect(Collectors.toList()), header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
      return Pair.of(partitionPath, logWriter.getLogFile());
    }
  }
}
