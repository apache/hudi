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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
import static org.apache.hudi.common.model.HoodieRecordMerger.OVERWRITE_MERGER_STRATEGY_UUID;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGER_STRATEGY;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getLogFileListFromFileSlice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests {@link HoodieFileGroupReader} with different engines
 */
public abstract class TestHoodieFileGroupReaderBase<T> {
  @TempDir
  protected java.nio.file.Path tempDir;

  protected String customRecordMergerStrategy = HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;

  public abstract StorageConfiguration<?> getStorageConf();

  public abstract String getBasePath();

  public abstract HoodieReaderContext<T> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf);

  public abstract String getRecordPayloadForMergeMode(RecordMergeMode mergeMode);

  public abstract void commitToTable(List<HoodieRecord> recordList, String operation,
                                     Map<String, String> writeConfigs);

  public abstract void validateRecordsInFileGroup(String tablePath,
                                                  List<T> actualRecordList,
                                                  Schema schema,
                                                  FileSlice fileSlice,
                                                  boolean isSkipMerge);

  public void validateRecordsInFileGroup(String tablePath,
                                                  List<T> actualRecordList,
                                                  Schema schema,
                                                  FileSlice fileSlice) {
    validateRecordsInFileGroup(tablePath, actualRecordList, schema, fileSlice, false);
  }

  private static Stream<Arguments> testArguments() {
    return Stream.of(
        arguments(RecordMergeMode.OVERWRITE_WITH_LATEST, "avro"),
        arguments(RecordMergeMode.OVERWRITE_WITH_LATEST, "parquet"),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, "avro"),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, "parquet"),
        arguments(RecordMergeMode.CUSTOM, "avro"),
        arguments(RecordMergeMode.CUSTOM, "parquet")
    );
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReadFileGroupInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      commitToTable(dataGen.generateInserts("001", 100), INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), dataGen.getPartitionPaths(), true, 0, recordMergeMode);

      // Two commits; reading one file group containing a base file and a log file
      commitToTable(dataGen.generateUpdates("002", 100), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), dataGen.getPartitionPaths(), true, 1, recordMergeMode);

      // Three commits; reading one file group containing a base file and two log files
      commitToTable(dataGen.generateUpdates("003", 100), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), dataGen.getPartitionPaths(), true, 2, recordMergeMode);
    }
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReadLogFilesOnlyInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);
    // Use InMemoryIndex to generate log only mor table
    writeConfigs.put("hoodie.index.type", "INMEMORY");

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      commitToTable(dataGen.generateInserts("001", 100), INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), dataGen.getPartitionPaths(), false, 1, recordMergeMode);

      // Two commits; reading one file group containing a base file and a log file
      commitToTable(dataGen.generateUpdates("002", 100), UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), dataGen.getPartitionPaths(), false, 2, recordMergeMode);
    }
  }

  private Map<String, String> getCommonConfigs(RecordMergeMode recordMergeMode) {
    Map<String, String> configMapping = new HashMap<>();
    configMapping.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    configMapping.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    configMapping.put("hoodie.datasource.write.precombine.field", "timestamp");
    configMapping.put("hoodie.payload.ordering.field", "timestamp");
    configMapping.put(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, "hoodie_test");
    configMapping.put("hoodie.insert.shuffle.parallelism", "4");
    configMapping.put("hoodie.upsert.shuffle.parallelism", "4");
    configMapping.put("hoodie.bulkinsert.shuffle.parallelism", "2");
    configMapping.put("hoodie.delete.shuffle.parallelism", "1");
    configMapping.put("hoodie.merge.small.file.group.candidates.limit", "0");
    configMapping.put("hoodie.compact.inline", "false");
    configMapping.put(RECORD_MERGE_MODE.key(), recordMergeMode.name());
    configMapping.put("hoodie.datasource.write.payload.class", getRecordPayloadForMergeMode(recordMergeMode));
    switch (recordMergeMode) {
      case OVERWRITE_WITH_LATEST:
        configMapping.put("hoodie.datasource.write.record.merger.strategy", OVERWRITE_MERGER_STRATEGY_UUID);
        configMapping.put("hoodie.datasource.write.precombine.field", "");
        break;
      case CUSTOM:
        configMapping.put("hoodie.datasource.write.record.merger.strategy", customRecordMergerStrategy);
        break;
      case EVENT_TIME_ORDERING:
      default:
        configMapping.put("hoodie.datasource.write.record.merger.strategy", DEFAULT_MERGER_STRATEGY_UUID);
        break;
    }
    return configMapping;
  }

  private void validateOutputFromFileGroupReader(StorageConfiguration<?> storageConf,
                                                 String tablePath,
                                                 String[] partitionPaths,
                                                 boolean containsBaseFile,
                                                 int expectedLogFileNum,
                                                 RecordMergeMode recordMergeMode) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, tablePath);
    Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(storageConf);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(
        engineContext, FileSystemViewStorageConfig.newBuilder().build(),
        HoodieCommonConfig.newBuilder().build(),
        mc -> HoodieTableMetadata.create(
            engineContext, mc.getStorage(), metadataConfig, tablePath));
    SyncableFileSystemView fsView = viewManager.getFileSystemView(metaClient);
    FileSlice fileSlice = fsView.getAllFileSlices(partitionPaths[0]).findFirst().get();
    List<String> logFilePathList = getLogFileListFromFileSlice(fileSlice);
    assertEquals(expectedLogFileNum, logFilePathList.size());

    List<T> actualRecordList = new ArrayList<>();
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.precombine.field", "timestamp");
    props.setProperty("hoodie.payload.ordering.field", "timestamp");
    props.setProperty(RECORD_MERGER_STRATEGY.key(), RECORD_MERGER_STRATEGY.defaultValue());
    props.setProperty(RECORD_MERGE_MODE.key(), recordMergeMode.name());
    props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.defaultValue()));
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), metaClient.getTempFolderPath());
    props.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), ExternalSpillableMap.DiskMapType.ROCKS_DB.name());
    props.setProperty(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), "false");
    if (metaClient.getTableConfig().contains(PARTITION_FIELDS)) {
      props.setProperty(PARTITION_FIELDS.key(), metaClient.getTableConfig().getString(PARTITION_FIELDS));
    }
    assertEquals(containsBaseFile, fileSlice.getBaseFile().isPresent());
    if (shouldValidatePartialRead(fileSlice, avroSchema)) {
      assertThrows(IllegalArgumentException.class, () -> new HoodieFileGroupReader<>(
          getHoodieReaderContext(tablePath, avroSchema, storageConf),
          metaClient.getStorage(),
          tablePath,
          metaClient.getActiveTimeline().lastInstant().get().getRequestTime(),
          fileSlice,
          avroSchema,
          avroSchema,
          Option.empty(),
          metaClient,
          props,
          1,
          fileSlice.getTotalFileSize(),
          false));
    }
    HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
        getHoodieReaderContext(tablePath, avroSchema, storageConf),
        metaClient.getStorage(),
        tablePath,
        metaClient.getActiveTimeline().lastInstant().get().getRequestTime(),
        fileSlice,
        avroSchema,
        avroSchema,
        Option.empty(),
        metaClient,
        props,
        0,
        fileSlice.getTotalFileSize(),
        false);
    fileGroupReader.initRecordIterators();
    while (fileGroupReader.hasNext()) {
      actualRecordList.add(fileGroupReader.next());
    }
    fileGroupReader.close();

    validateRecordsInFileGroup(tablePath, actualRecordList, avroSchema, fileSlice);

    //validate skip merge
    actualRecordList.clear();
    props.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), HoodieReaderConfig.REALTIME_SKIP_MERGE);
    fileGroupReader = new HoodieFileGroupReader<>(
        getHoodieReaderContext(tablePath, avroSchema, storageConf),
        metaClient.getStorage(),
        tablePath,
        metaClient.getActiveTimeline().lastInstant().get().getRequestTime(),
        fileSlice,
        avroSchema,
        avroSchema,
        Option.empty(),
        metaClient,
        props,
        0,
        fileSlice.getTotalFileSize(),
        false);
    fileGroupReader.initRecordIterators();
    while (fileGroupReader.hasNext()) {
      actualRecordList.add(fileGroupReader.next());
    }
    fileGroupReader.close();

    validateRecordsInFileGroup(tablePath, actualRecordList, avroSchema, fileSlice, true);
  }

  private boolean shouldValidatePartialRead(FileSlice fileSlice, Schema requestedSchema) {
    if (fileSlice.getLogFiles().findAny().isPresent()) {
      return true;
    }
    if (fileSlice.getBaseFile().get().getBootstrapBaseFile().isPresent()) {
      //TODO: [HUDI-8169] this code path will not hit until we implement bootstrap tests
      Pair<List<Schema.Field>, List<Schema.Field>> dataAndMetaCols = HoodieFileGroupReaderSchemaHandler.getDataAndMetaCols(requestedSchema);
      return !dataAndMetaCols.getLeft().isEmpty() && !dataAndMetaCols.getRight().isEmpty();
    }
    return false;
  }
}
