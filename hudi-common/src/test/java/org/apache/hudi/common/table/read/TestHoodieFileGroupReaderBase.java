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
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_ORDERING_FIELD;
import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_PARTITION_PATH;
import static org.apache.hudi.common.engine.HoodieReaderContext.INTERNAL_META_RECORD_KEY;
import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getLogFileListFromFileSlice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests {@link HoodieFileGroupReader} with different engines
 */
public abstract class TestHoodieFileGroupReaderBase<T> {
  @TempDir
  protected java.nio.file.Path tempDir;

  public abstract StorageConfiguration<?> getStorageConf();

  public abstract String getBasePath();

  public abstract HoodieReaderContext<T> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf);

  public abstract String getCustomPayload();

  public abstract void bootstrapTable(List<HoodieRecord> recordList,
                                      Map<String, String> writeConfigs);

  public abstract void commitToTable(List<HoodieRecord> recordList, String operation,
                                     Map<String, String> writeConfigs);

  public abstract void validateRecordsInFileGroup(String tablePath,
                                                  List<T> actualRecordList,
                                                  Schema schema,
                                                  FileSlice fileSlice,
                                                  boolean skipMerge);

  public abstract void assertRecordsEqual(Schema schema, T expected, T actual);

  private static Stream<Arguments> testArguments() {
    return Stream.of(
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, "avro"),
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, "parquet"),
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

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReadFileGroupInBootstrapMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
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
  @EnumSource(value = ExternalSpillableMap.DiskMapType.class)
  public void testSpillableMapUsage(ExternalSpillableMap.DiskMapType diskMapType) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(RecordMergeMode.COMMIT_TIME_ORDERING));
    Option<Schema.Type> orderingFieldType = Option.of(Schema.Type.STRING);
    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      commitToTable(dataGen.generateInserts("001", 100), INSERT.value(), writeConfigs);
      String baseMapPath = Files.createTempDirectory(null).toString();
      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(getStorageConf(), getBasePath());
      Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
      FileSlice fileSlice = getFileSliceToRead(getStorageConf(), getBasePath(), metaClient, dataGen.getPartitionPaths(), true, 0);
      List<T> records = readRecordsFromFileGroup(
          getStorageConf(), getBasePath(), metaClient, fileSlice, fileSlice.getTotalFileSize(),
          avroSchema, RecordMergeMode.COMMIT_TIME_ORDERING, false);
      HoodieReaderContext<T> readerContext = getHoodieReaderContext(getBasePath(), avroSchema, getStorageConf());
      Comparable castedOrderingField = readerContext.castValue(100, Schema.Type.STRING);
      for (Boolean isCompressionEnabled : new boolean[] {true, false}) {
        try (ExternalSpillableMap<Serializable, Pair<Option<T>, Map<String, Object>>> spillableMap =
                 new ExternalSpillableMap<>(16L, baseMapPath, new DefaultSizeEstimator(),
                     new HoodieRecordSizeEstimator(avroSchema), diskMapType, isCompressionEnabled)) {
          Long position = 0L;
          for (T record : records) {
            String recordKey = readerContext.getRecordKey(record, avroSchema);
            //test key based
            spillableMap.put(recordKey,
                Pair.of(
                    Option.ofNullable(readerContext.seal(record)),
                    readerContext.generateMetadataForRecord(record, avroSchema)));

            //test position based
            spillableMap.put(position++,
                Pair.of(
                    Option.ofNullable(readerContext.seal(record)),
                    readerContext.generateMetadataForRecord(recordKey,
                        dataGen.getPartitionPaths()[0], 100, orderingFieldType)));
          }

          assertEquals(records.size() * 2, spillableMap.size());
          //Validate that everything is correct
          position = 0L;
          for (T record : records) {
            String recordKey = readerContext.getRecordKey(record, avroSchema);
            Pair<Option<T>, Map<String, Object>> keyBased = spillableMap.get(recordKey);
            assertNotNull(keyBased);
            Pair<Option<T>, Map<String, Object>> positionBased = spillableMap.get(position++);
            assertNotNull(positionBased);
            assertRecordsEqual(avroSchema, record, keyBased.getLeft().get());
            assertRecordsEqual(avroSchema, record, positionBased.getLeft().get());
            assertEquals(keyBased.getRight().get(INTERNAL_META_RECORD_KEY), recordKey);
            assertEquals(positionBased.getRight().get(INTERNAL_META_RECORD_KEY), recordKey);
            assertEquals(avroSchema, readerContext.getSchemaFromMetadata(keyBased.getRight()));
            assertEquals(dataGen.getPartitionPaths()[0], positionBased.getRight().get(INTERNAL_META_PARTITION_PATH));
            assertEquals(castedOrderingField, positionBased.getRight().get(INTERNAL_META_ORDERING_FIELD));
          }

        }
      }
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
    configMapping.put("hoodie.write.record.merge.mode", recordMergeMode.name());
    if (recordMergeMode.equals(RecordMergeMode.CUSTOM)) {
      configMapping.put("hoodie.datasource.write.payload.class", getCustomPayload());
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
    Schema tableSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    FileSlice fileSlice = getFileSliceToRead(storageConf, tablePath, metaClient, partitionPaths, containsBaseFile, expectedLogFileNum);
    if (containsBaseFile) {
      HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
      assertNotNull(baseFile.getPathInfo());
      assertTrue(baseFile.getFileSize() >= 0);
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        assertNotNull(baseFile.getBootstrapBaseFile().get().getPathInfo());
        assertTrue(baseFile.getBootstrapBaseFile().get().getFileLen() >= 0);
      }
    }
    if (fileSlice.getLogFiles().findAny().isPresent()) {
      assertFalse(fileSlice.getLogFiles().anyMatch(f -> f.getPathInfo() == null));
      assertFalse(fileSlice.getLogFiles().anyMatch(f -> f.getFileSize() < 0));
    }
    // Set the length of bytes to read in the base file the same as the file size;
    // all records should be read from the base file
    List<T> actualRecordList = readRecordsFromFileGroup(
        storageConf, tablePath, metaClient, fileSlice, fileSlice.getTotalFileSize(),
        tableSchema, recordMergeMode, false);
    validateRecordsInFileGroup(tablePath, actualRecordList, tableSchema, fileSlice, false);
    // Set the length of bytes to read in the base file to be -1, while the file slice still
    // contains the base file size; all records should be read from the base file
    actualRecordList = readRecordsFromFileGroup(
        storageConf, tablePath, metaClient, fileSlice, -1, tableSchema, recordMergeMode, false);
    validateRecordsInFileGroup(tablePath, actualRecordList, tableSchema, fileSlice, false);
    // Set the length of bytes to read in the base file to be -1, and the file slice
    // does not contain the base file size; the file group reader should fetch the base file
    // length and all records should be read from the base file
    FileSlice fileSliceWithNoPathInfo = new FileSlice(
        fileSlice.getFileGroupId(), fileSlice.getBaseInstantTime(),
        containsBaseFile
            ? new HoodieBaseFile(fileSlice.getBaseFile().get().getPath(),
            fileSlice.getBaseFile().get().getBootstrapBaseFile().isPresent()
                ? new BaseFile(fileSlice.getBaseFile().get().getBootstrapBaseFile().get().getPath())
                : null)
            : null,
        fileSlice.getLogFiles().map(
            f -> new HoodieLogFile(f.getPath())).collect(Collectors.toList()));
    if (containsBaseFile) {
      HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
      assertNull(fileSliceWithNoPathInfo.getBaseFile().get().getPathInfo());
      assertTrue(fileSliceWithNoPathInfo.getBaseFile().get().getFileSize() < 0);
      if (baseFile.getBootstrapBaseFile().isPresent()) {
        assertNull(baseFile.getBootstrapBaseFile().get().getPathInfo());
        assertTrue(baseFile.getBootstrapBaseFile().get().getFileLen() < 0);
      }
    }
    if (fileSliceWithNoPathInfo.getLogFiles().findAny().isPresent()) {
      assertFalse(fileSliceWithNoPathInfo.getLogFiles().anyMatch(f -> f.getPathInfo() != null));
      assertFalse(fileSliceWithNoPathInfo.getLogFiles().anyMatch(f -> f.getFileSize() >= 0));
    }
    actualRecordList = readRecordsFromFileGroup(
        storageConf, tablePath, metaClient, fileSliceWithNoPathInfo, -1, tableSchema, recordMergeMode, false);
    validateRecordsInFileGroup(tablePath, actualRecordList, tableSchema, fileSlice, false);
    actualRecordList = readRecordsFromFileGroup(
        storageConf, tablePath, metaClient, fileSlice, fileSlice.getTotalFileSize(),
        tableSchema, recordMergeMode, true);
    validateRecordsInFileGroup(tablePath, actualRecordList, tableSchema, fileSlice, true);
  }

  private FileSlice getFileSliceToRead(StorageConfiguration<?> storageConf,
                                       String tablePath,
                                       HoodieTableMetaClient metaClient,
                                       String[] partitionPaths,
                                       boolean containsBaseFile,
                                       int expectedLogFileNum) {
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
    assertEquals(containsBaseFile, fileSlice.getBaseFile().isPresent());
    return fileSlice;
  }

  private TypedProperties getProps(HoodieTableMetaClient metaClient,
                                   RecordMergeMode recordMergeMode,
                                   boolean isSkipMerge) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.precombine.field", "timestamp");
    props.setProperty("hoodie.payload.ordering.field", "timestamp");
    props.setProperty(RECORD_MERGE_MODE.key(), recordMergeMode.name());
    if (recordMergeMode.equals(RecordMergeMode.CUSTOM)) {
      props.setProperty(RECORD_MERGE_STRATEGY_ID.key(), PAYLOAD_BASED_MERGE_STRATEGY_UUID);
      props.setProperty(PAYLOAD_CLASS_NAME.key(), getCustomPayload());
    }
    props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.defaultValue()));
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), metaClient.getTempFolderPath());
    props.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), ExternalSpillableMap.DiskMapType.ROCKS_DB.name());
    props.setProperty(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(), "false");
    if (metaClient.getTableConfig().contains(PARTITION_FIELDS)) {
      props.setProperty(PARTITION_FIELDS.key(), metaClient.getTableConfig().getString(PARTITION_FIELDS));
    }
    if (isSkipMerge) {
      props.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), HoodieReaderConfig.REALTIME_SKIP_MERGE);
    }
    return props;
  }

  private List<T> readRecordsFromFileGroup(StorageConfiguration<?> storageConf,
                                           String tablePath,
                                           HoodieTableMetaClient metaClient,
                                           FileSlice fileSlice,
                                           long length,
                                           Schema tableSchema,
                                           RecordMergeMode recordMergeMode,
                                           boolean isSkipMerge) throws Exception {
    List<T> actualRecordList = new ArrayList<>();
    TypedProperties props = getProps(metaClient, recordMergeMode, isSkipMerge);
    if (shouldValidatePartialRead(fileSlice, tableSchema)) {
      assertThrows(IllegalArgumentException.class, () -> new HoodieFileGroupReader<>(
          getHoodieReaderContext(tablePath, tableSchema, storageConf),
          metaClient.getStorage(),
          tablePath,
          metaClient.getActiveTimeline().lastInstant().get().requestedTime(),
          fileSlice,
          tableSchema,
          tableSchema,
          Option.empty(),
          metaClient,
          props,
          1,
          length,
          false));
    }
    try (HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
        getHoodieReaderContext(tablePath, tableSchema, storageConf),
        metaClient.getStorage(),
        tablePath,
        metaClient.getActiveTimeline().lastInstant().get().requestedTime(),
        fileSlice,
        tableSchema,
        tableSchema,
        Option.empty(),
        metaClient,
        props,
        0,
        length,
        false)) {
      fileGroupReader.initRecordIterators();
      while (fileGroupReader.hasNext()) {
        actualRecordList.add(fileGroupReader.next());
      }
    }
    return actualRecordList;
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
