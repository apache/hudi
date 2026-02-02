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
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.hadoop.HoodieAvroOrcWriter;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.io.storage.HoodieOrcConfig;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.orc.CompressionKind;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.baseFileName;

@Slf4j
public class HoodieWriteableTestTable extends HoodieMetadataTestTable {

  protected final HoodieSchema schema;
  protected final Option<BloomFilter> filter;
  protected final boolean populateMetaFields;

  protected HoodieWriteableTestTable(String basePath, HoodieStorage storage,
                                     HoodieTableMetaClient metaClient,
                                     HoodieSchema schema, BloomFilter filter) {
    this(basePath, storage, metaClient, schema, filter, null);
  }

  protected HoodieWriteableTestTable(String basePath, HoodieStorage storage,
                                     HoodieTableMetaClient metaClient, HoodieSchema schema,
                                     BloomFilter filter, HoodieTableMetadataWriter metadataWriter) {
    this(basePath, storage, metaClient, schema, filter, metadataWriter, Option.empty());
  }

  public HoodieWriteableTestTable(String basePath, HoodieStorage storage,
                                     HoodieTableMetaClient metaClient, HoodieSchema schema,
                                     BloomFilter filter, HoodieTableMetadataWriter metadataWriter,
                                     Option<HoodieEngineContext> context) {
    super(basePath, storage, metaClient, metadataWriter, context);
    this.schema = schema;
    this.filter = Option.ofNullable(filter);
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

  public StoragePath withInserts(String partition, String fileId, List<HoodieRecord> records,
                                 TaskContextSupplier contextSupplier) throws Exception {
    FileCreateUtilsLegacy.createPartitionMetaFile(basePath, partition);
    String fileName = baseFileName(currentInstantTime, fileId);

    StoragePath baseFilePath = new StoragePath(Paths.get(basePath, partition, fileName).toString());
    if (storage.exists(baseFilePath)) {
      log.warn("Deleting the existing base file " + baseFilePath);
      storage.deleteFile(baseFilePath);
    }

    if (HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().equals(HoodieFileFormat.PARQUET)) {
      HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
          new AvroSchemaConverter().convert(schema.toAvroSchema()), schema, filter, new Properties());
      HoodieParquetConfig<HoodieAvroWriteSupport> config = new HoodieParquetConfig<>(writeSupport, CompressionCodecName.GZIP,
          ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, 120 * 1024 * 1024,
          storage.getConf(), Double.parseDouble(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION.defaultValue()), true);
      try (HoodieAvroParquetWriter writer = new HoodieAvroParquetWriter(
          new StoragePath(Paths.get(basePath, partition, fileName).toString()), config, currentInstantTime,
          contextSupplier, populateMetaFields)) {
        int seqId = 1;
        for (HoodieRecord record : records) {
          GenericRecord avroRecord = (GenericRecord) record.rewriteRecordWithNewSchema(schema, CollectionUtils.emptyProps(), schema).getData();
          if (populateMetaFields) {
            HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, currentInstantTime, String.valueOf(seqId++));
            HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), fileName);
            writer.writeAvro(record.getRecordKey(), avroRecord);
            filter.ifPresent(f -> f.add(record.getRecordKey()));
          } else {
            writer.writeAvro(record.getRecordKey(), avroRecord);
          }
        }
      }
    } else if (HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().equals(HoodieFileFormat.ORC)) {
      StorageConfiguration conf = storage.getConf().newInstance();
      int orcStripSize = Integer.parseInt(HoodieStorageConfig.ORC_STRIPE_SIZE.defaultValue());
      int orcBlockSize = Integer.parseInt(HoodieStorageConfig.ORC_BLOCK_SIZE.defaultValue());
      int maxFileSize = Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue());
      HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB, orcStripSize, orcBlockSize, maxFileSize, filter.orElse(null));
      try (HoodieAvroOrcWriter writer = new HoodieAvroOrcWriter(
          currentInstantTime,
          new StoragePath(Paths.get(basePath, partition, fileName).toString()),
          config, schema, contextSupplier)) {
        int seqId = 1;
        for (HoodieRecord record : records) {
          GenericRecord avroRecord = (GenericRecord) record.toIndexedRecord(schema, CollectionUtils.emptyProps()).get().getData();
          HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, currentInstantTime, String.valueOf(seqId++));
          HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), fileName);
          writer.writeAvro(record.getRecordKey(), avroRecord);
          filter.ifPresent(f -> f.add(record.getRecordKey()));
        }
      }
    }

    return baseFilePath;
  }

  public Map<String, List<HoodieLogFile>> withLogAppends(String partition, String fileId, List<HoodieRecord> records) throws Exception {
    Map<String, List<HoodieLogFile>> partitionToLogfilesMap = new HashMap<>();
    final Pair<String, HoodieLogFile> appendedLogFile = appendRecordsToLogFile(partition, fileId, records);
    partitionToLogfilesMap.computeIfAbsent(appendedLogFile.getKey(), k -> new ArrayList<>()).add(appendedLogFile.getValue());
    return partitionToLogfilesMap;
  }

  private Pair<String, HoodieLogFile> appendRecordsToLogFile(String partitionPath, String fileId, List<HoodieRecord> records) throws Exception {
    try (HoodieLogFormat.Writer logWriter = HoodieLogFormat.newWriterBuilder()
        .onParentPath(new StoragePath(basePath, partitionPath))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION).withFileId(fileId)
        .withInstantTime(currentInstantTime).withStorage(storage).build()) {
      Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, currentInstantTime);
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, schema.toString());
      logWriter.appendBlock(new HoodieAvroDataBlock(records, header, HoodieRecord.RECORD_KEY_METADATA_FIELD));
      return Pair.of(partitionPath, logWriter.getLogFile());
    }
  }
}
