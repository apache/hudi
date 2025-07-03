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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StorageConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getLogFileListFromFileSlice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests {@link HoodieFileGroupReader} with different engines
 */
public abstract class TestHoodieFileGroupReaderBase<T> {
  private static final String KEY_FIELD_NAME = "_row_key";
  private static final String PRECOMBINE_FIELD_NAME = "timestamp";
  private static final String PARTITION_FIELD_NAME = "partition_path";
  private static final String RIDER_FIELD_NAME = "rider";
  @TempDir
  protected java.nio.file.Path tempDir;

  public abstract StorageConfiguration<?> getStorageConf();

  public abstract String getBasePath();

  public abstract HoodieReaderContext<T> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf, HoodieTableMetaClient metaClient);

  public abstract String getCustomPayload();

  public abstract void commitToTable(List<HoodieRecord> recordList, String operation,
                                     Map<String, String> writeConfigs);

  public abstract void assertRecordsEqual(Schema schema, T expected, T actual);

  private static Stream<Arguments> testArguments() {
    return Stream.of(
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, "avro", false),
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, "parquet", true),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, "avro", true),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, "parquet", true),
        arguments(RecordMergeMode.CUSTOM, "avro", false),
        arguments(RecordMergeMode.CUSTOM, "parquet", true)
    );
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReadFileGroupInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat, boolean populateMetaFields) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, populateMetaFields));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      List<HoodieRecord> initialRecords = dataGen.generateInserts("001", 100);
      commitToTable(initialRecords, INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 0, recordMergeMode,
          initialRecords, initialRecords);

      // Two commits; reading one file group containing a base file and a log file
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      List<HoodieRecord> unmergedRecords = CollectionUtils.combine(initialRecords, updates);
      commitToTable(updates, UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 1, recordMergeMode,
          allRecords, unmergedRecords);

      // Three commits; reading one file group containing a base file and two log files
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates("003", 100);
      List<HoodieRecord> finalRecords = mergeRecordLists(updates2, allRecords);
      commitToTable(updates2, UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 2, recordMergeMode,
          finalRecords, CollectionUtils.combine(unmergedRecords, updates2));
    }
  }

  private static Stream<Arguments> logFileOnlyCases() {
    return Stream.of(
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, "avro"),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, "parquet"),
        arguments(RecordMergeMode.CUSTOM, "avro"));
  }

  @ParameterizedTest
  @MethodSource("logFileOnlyCases")
  public void testReadLogFilesOnlyInMergeOnReadTable(RecordMergeMode recordMergeMode, String logDataBlockFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, true));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);
    // Use InMemoryIndex to generate log only mor table
    writeConfigs.put("hoodie.index.type", "INMEMORY");

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a log file only
      List<HoodieRecord> initialRecords = dataGen.generateInserts("001", 100);
      commitToTable(initialRecords, INSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 1, recordMergeMode,
          initialRecords, initialRecords);

      // Two commits; reading one file group containing two log files
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      commitToTable(updates, UPSERT.value(), writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 2, recordMergeMode,
          allRecords, CollectionUtils.combine(initialRecords, updates));
    }
  }

  @Test
  public void testReadFileGroupInBootstrapMergeOnReadTable() throws Exception {
    Path zipOutput = Paths.get(new URI(getBasePath()));
    extract(zipOutput);
    ObjectMapper objectMapper = new ObjectMapper();
    Path basePath = zipOutput.resolve("bootstrap_data");
    List<HoodieTestDataGenerator.RecordIdentifier> expectedRecords = new ArrayList<>();
    objectMapper.reader().forType(HoodieTestDataGenerator.RecordIdentifier.class).<HoodieTestDataGenerator.RecordIdentifier>readValues(basePath.resolve("merged_records.json").toFile())
        .forEachRemaining(expectedRecords::add);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedUnMergedRecords = new ArrayList<>();
    objectMapper.reader().forType(HoodieTestDataGenerator.RecordIdentifier.class).<HoodieTestDataGenerator.RecordIdentifier>readValues(basePath.resolve("unmerged_records.json").toFile())
        .forEachRemaining(expectedUnMergedRecords::add);
    validateOutputFromFileGroupReaderWithExistingRecords(getStorageConf(), basePath.toString(), true, 1, RecordMergeMode.EVENT_TIME_ORDERING,
        expectedRecords, expectedUnMergedRecords);
  }

  @ParameterizedTest
  @EnumSource(value = ExternalSpillableMap.DiskMapType.class)
  public void testSpillableMapUsage(ExternalSpillableMap.DiskMapType diskMapType) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(RecordMergeMode.COMMIT_TIME_ORDERING, true));
    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      commitToTable(dataGen.generateInserts("001", 100), INSERT.value(), writeConfigs);
      String baseMapPath = Files.createTempDirectory(null).toString();
      HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(getStorageConf(), getBasePath());
      Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
      List<FileSlice> fileSlices = getFileSlicesToRead(getStorageConf(), getBasePath(), metaClient, true, 0);
      List<T> records = readRecordsFromFileGroup(getStorageConf(), getBasePath(), metaClient, fileSlices,
          avroSchema, RecordMergeMode.COMMIT_TIME_ORDERING, false, false);
      HoodieReaderContext<T> readerContext = getHoodieReaderContext(getBasePath(), avroSchema, getStorageConf(), metaClient);
      for (Boolean isCompressionEnabled : new boolean[] {true, false}) {
        try (ExternalSpillableMap<Serializable, BufferedRecord<T>> spillableMap =
                 new ExternalSpillableMap<>(16L, baseMapPath, new DefaultSizeEstimator(),
                     new HoodieRecordSizeEstimator(avroSchema), diskMapType, new DefaultSerializer<>(), isCompressionEnabled, getClass().getSimpleName())) {
          Long position = 0L;
          for (T record : records) {
            String recordKey = readerContext.getRecordKey(record, avroSchema);
            //test key based
            BufferedRecord<T> bufferedRecord = BufferedRecord.forRecordWithContext(record, avroSchema, readerContext, Option.of("timestamp"), false);
            spillableMap.put(recordKey, bufferedRecord.toBinary(readerContext));

            //test position based
            spillableMap.put(position++, bufferedRecord.toBinary(readerContext));
          }

          assertEquals(records.size() * 2, spillableMap.size());
          //Validate that everything is correct
          position = 0L;
          for (T record : records) {
            String recordKey = readerContext.getRecordKey(record, avroSchema);
            BufferedRecord<T> keyBased = spillableMap.get(recordKey);
            assertNotNull(keyBased);
            BufferedRecord<T> positionBased = spillableMap.get(position++);
            assertNotNull(positionBased);
            assertRecordsEqual(avroSchema, record, keyBased.getRecord());
            assertRecordsEqual(avroSchema, record, positionBased.getRecord());
            assertEquals(keyBased.getRecordKey(), recordKey);
            assertEquals(positionBased.getRecordKey(), recordKey);
            assertEquals(avroSchema, readerContext.getSchemaFromBufferRecord(keyBased));
            // generate field value is hardcoded as 0 for ordering field: timestamp, see HoodieTestDataGenerator#generateRandomValue
            assertEquals(readerContext.convertValueToEngineType(0L), positionBased.getOrderingValue());
          }
        }
      }
    }
  }

  protected Map<String, String> getCommonConfigs(RecordMergeMode recordMergeMode, boolean populateMetaFields) {
    Map<String, String> configMapping = new HashMap<>();
    configMapping.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), KEY_FIELD_NAME);
    configMapping.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), PARTITION_FIELD_NAME);
    configMapping.put("hoodie.datasource.write.precombine.field", PRECOMBINE_FIELD_NAME);
    configMapping.put("hoodie.payload.ordering.field", PRECOMBINE_FIELD_NAME);
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
    configMapping.put("hoodie.populate.meta.fields", Boolean.toString(populateMetaFields));
    return configMapping;
  }

  protected void validateOutputFromFileGroupReader(StorageConfiguration<?> storageConf,
                                                 String tablePath,
                                                 boolean containsBaseFile,
                                                 int expectedLogFileNum,
                                                 RecordMergeMode recordMergeMode,
                                                 List<HoodieRecord> expectedHoodieRecords,
                                                 List<HoodieRecord> expectedHoodieUnmergedRecords) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, tablePath);
    Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    List<HoodieTestDataGenerator.RecordIdentifier> expectedRecords = convertHoodieRecords(expectedHoodieRecords, avroSchema);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedUnmergedRecords = convertHoodieRecords(expectedHoodieUnmergedRecords, avroSchema);
    validateOutputFromFileGroupReaderWithExistingRecords(
        storageConf, tablePath, containsBaseFile, expectedLogFileNum, recordMergeMode,
        expectedRecords, expectedUnmergedRecords);
  }

  private void validateOutputFromFileGroupReaderWithExistingRecords(StorageConfiguration<?> storageConf,
                                                                    String tablePath,
                                                                    boolean containsBaseFile,
                                                                    int expectedLogFileNum,
                                                                    RecordMergeMode recordMergeMode,
                                                                    List<HoodieTestDataGenerator.RecordIdentifier> expectedRecords,
                                                                    List<HoodieTestDataGenerator.RecordIdentifier> expectedUnmergedRecords) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, tablePath);
    Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    // use reader context for conversion to engine specific objects
    HoodieReaderContext<T> readerContext = getHoodieReaderContext(tablePath, avroSchema, getStorageConf(), metaClient);
    List<FileSlice> fileSlices = getFileSlicesToRead(storageConf, tablePath, metaClient, containsBaseFile, expectedLogFileNum);
    boolean sortOutput = !containsBaseFile;
    List<HoodieTestDataGenerator.RecordIdentifier> actualRecordList = convertEngineRecords(
        readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode, false, sortOutput),
        avroSchema, readerContext);
    // validate size is equivalent to ensure no duplicates are returned
    assertEquals(expectedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedRecords), new HashSet<>(actualRecordList));
    // validate records can be read from file group as HoodieRecords
    actualRecordList = convertHoodieRecords(
        readHoodieRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode),
        avroSchema, readerContext);
    assertEquals(expectedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedRecords), new HashSet<>(actualRecordList));
    // validate unmerged records
    actualRecordList = convertEngineRecords(
        readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode, true, false),
        avroSchema, readerContext);
    assertEquals(expectedUnmergedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedUnmergedRecords), new HashSet<>(actualRecordList));
  }

  private List<FileSlice> getFileSlicesToRead(StorageConfiguration<?> storageConf,
                                              String tablePath,
                                              HoodieTableMetaClient metaClient,
                                              boolean containsBaseFile,
                                              int expectedLogFileNum) {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(storageConf);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().build();
    FileSystemViewManager viewManager = FileSystemViewManager.createViewManager(
        engineContext,
        metadataConfig,
        FileSystemViewStorageConfig.newBuilder().build(),
        HoodieCommonConfig.newBuilder().build(),
        mc -> metaClient.getTableFormat().getMetadataFactory().create(
            engineContext, mc.getStorage(), metadataConfig, tablePath));
    HoodieTableFileSystemView fsView =
        (HoodieTableFileSystemView) viewManager.getFileSystemView(metaClient);
    List<String> relativePartitionPathList = FSUtils.getAllPartitionPaths(engineContext, metaClient, metadataConfig);
    List<FileSlice> fileSlices =
        relativePartitionPathList.stream().flatMap(fsView::getAllFileSlices)
            .collect(Collectors.toList());
    fileSlices.forEach(fileSlice -> {
      if (fileSlice.hasBootstrapBase()) {
        // bootstrap file points to an absolute path
        // Since the dataset is copied to a new tempDir for testing, we need to manipulate this path
        HoodieBaseFile baseFile = fileSlice.getBaseFile().get();
        String bootstrapPath = baseFile.getBootstrapBaseFile().get().getPath();
        String newBootstrapPath = tablePath + "/" + bootstrapPath.substring(bootstrapPath.indexOf("bootstrap_table"));
        baseFile.setBootstrapBaseFile(new BaseFile(newBootstrapPath));
      }
      List<String> logFilePathList = getLogFileListFromFileSlice(fileSlice);
      assertEquals(expectedLogFileNum, logFilePathList.size());
      assertEquals(containsBaseFile, fileSlice.getBaseFile().isPresent());
    });
    return fileSlices;
  }

  private List<T> readRecordsFromFileGroup(StorageConfiguration<?> storageConf,
                                           String tablePath,
                                           HoodieTableMetaClient metaClient,
                                           List<FileSlice> fileSlices,
                                           Schema avroSchema,
                                           RecordMergeMode recordMergeMode,
                                           boolean isSkipMerge,
                                           boolean sortOutput) {

    List<T> actualRecordList = new ArrayList<>();
    TypedProperties props = buildProperties(metaClient, recordMergeMode);
    if (isSkipMerge) {
      props.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), HoodieReaderConfig.REALTIME_SKIP_MERGE);
    }
    fileSlices.forEach(fileSlice -> {
      if (shouldValidatePartialRead(fileSlice, avroSchema)) {
        assertThrows(IllegalArgumentException.class, () -> getHoodieFileGroupReader(storageConf, tablePath, metaClient, avroSchema, fileSlice, 1, props, sortOutput));
      }
      try (HoodieFileGroupReader<T> fileGroupReader = getHoodieFileGroupReader(storageConf, tablePath, metaClient, avroSchema, fileSlice, 0, props, sortOutput)) {
        readWithFileGroupReader(fileGroupReader, actualRecordList, avroSchema, getHoodieReaderContext(tablePath, avroSchema, storageConf, metaClient), sortOutput);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    });
    return actualRecordList;
  }

  private HoodieFileGroupReader<T> getHoodieFileGroupReader(StorageConfiguration<?> storageConf, String tablePath, HoodieTableMetaClient metaClient, Schema avroSchema, FileSlice fileSlice,
                                                            int start, TypedProperties props, boolean sortOutput) {
    return HoodieFileGroupReader.<T>newBuilder()
        .withReaderContext(getHoodieReaderContext(tablePath, avroSchema, storageConf, metaClient))
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(metaClient.getActiveTimeline().lastInstant().get().requestedTime())
        .withFileSlice(fileSlice)
        .withDataSchema(avroSchema)
        .withRequestedSchema(avroSchema)
        .withProps(props)
        .withStart(start)
        .withLength(fileSlice.getTotalFileSize())
        .withShouldUseRecordPosition(false)
        .withAllowInflightInstants(false)
        .withSortOutput(sortOutput)
        .build();
  }

  protected void readWithFileGroupReader(
      HoodieFileGroupReader<T> fileGroupReader,
      List<T> recordList,
      Schema avroSchema,
      HoodieReaderContext<T> readerContext,
      boolean sortOutput) throws IOException {
    String lastKey = null;
    try (ClosableIterator<T> fileGroupReaderIterator = fileGroupReader.getClosableIterator()) {
      while (fileGroupReaderIterator.hasNext()) {
        T next = fileGroupReaderIterator.next();
        if (sortOutput) {
          String currentKey = readerContext.getRecordKey(next, avroSchema);
          assertTrue(lastKey == null || lastKey.compareTo(currentKey) < 0, "Record keys should be sorted within the file group");
          lastKey = currentKey;
        }
        recordList.add(next);
      }
    }
  }

  private List<HoodieRecord<T>> readHoodieRecordsFromFileGroup(StorageConfiguration<?> storageConf,
                                                               String tablePath,
                                                               HoodieTableMetaClient metaClient,
                                                               List<FileSlice> fileSlices,
                                                               Schema avroSchema,
                                                               RecordMergeMode recordMergeMode) {

    List<HoodieRecord<T>> actualRecordList = new ArrayList<>();
    TypedProperties props = buildProperties(metaClient, recordMergeMode);
    fileSlices.forEach(fileSlice -> {
      try (HoodieFileGroupReader<T> fileGroupReader = getHoodieFileGroupReader(storageConf, tablePath, metaClient, avroSchema, fileSlice, 0, props, false);
           ClosableIterator<HoodieRecord<T>> iter = fileGroupReader.getClosableHoodieRecordIterator()) {
        while (iter.hasNext()) {
          actualRecordList.add(iter.next());
        }
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    });
    return actualRecordList;
  }

  private TypedProperties buildProperties(HoodieTableMetaClient metaClient, RecordMergeMode recordMergeMode) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.precombine.field", PRECOMBINE_FIELD_NAME);
    props.setProperty("hoodie.payload.ordering.field", PRECOMBINE_FIELD_NAME);
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
    return props;
  }

  private boolean shouldValidatePartialRead(FileSlice fileSlice, Schema requestedSchema) {
    if (fileSlice.getLogFiles().findAny().isPresent()) {
      return true;
    }
    if (fileSlice.getBaseFile().get().getBootstrapBaseFile().isPresent()) {
      //TODO: [HUDI-8169] this code path will not hit until we implement bootstrap tests
      Pair<List<Schema.Field>, List<Schema.Field>> dataAndMetaCols = FileGroupReaderSchemaHandler.getDataAndMetaCols(requestedSchema);
      return !dataAndMetaCols.getLeft().isEmpty() && !dataAndMetaCols.getRight().isEmpty();
    }
    return false;
  }

  protected List<HoodieRecord> mergeRecordLists(List<HoodieRecord> updates, List<HoodieRecord> existing) {
    Set<String> updatedKeys = updates.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
    return Stream.concat(updates.stream(), existing.stream().filter(record -> !updatedKeys.contains(record.getRecordKey())))
            .collect(Collectors.toList());
  }

  private List<HoodieTestDataGenerator.RecordIdentifier> convertHoodieRecords(List<HoodieRecord> records, Schema schema) {
    return records.stream().map(record -> {
      RawTripTestPayload payload = (RawTripTestPayload) record.getData();
      return HoodieTestDataGenerator.RecordIdentifier.fromTripTestPayload(payload);
    }).collect(Collectors.toList());
  }

  private List<HoodieTestDataGenerator.RecordIdentifier> convertEngineRecords(List<T> records, Schema schema, HoodieReaderContext<T> readerContext) {
    return records.stream()
        .map(record -> new HoodieTestDataGenerator.RecordIdentifier(
            readerContext.getValue(record, schema, KEY_FIELD_NAME).toString(),
            readerContext.getValue(record, schema, PARTITION_FIELD_NAME).toString(),
            readerContext.getValue(record, schema, PRECOMBINE_FIELD_NAME).toString(),
            readerContext.getValue(record, schema, RIDER_FIELD_NAME).toString()))
        .collect(Collectors.toList());
  }

  private List<HoodieTestDataGenerator.RecordIdentifier> convertHoodieRecords(List<HoodieRecord<T>> records, Schema schema, HoodieReaderContext<T> readerContext) {
    return records.stream()
        .map(record -> new HoodieTestDataGenerator.RecordIdentifier(
            record.getRecordKey(),
            removeHiveStylePartition(record.getPartitionPath()),
            record.getOrderingValue(schema, new TypedProperties()).toString(),
            readerContext.getValue(record.getData(), schema, RIDER_FIELD_NAME).toString()))
        .collect(Collectors.toList());
  }

  private static String removeHiveStylePartition(String partitionPath) {
    int indexOf = partitionPath.indexOf("=");
    if (indexOf > 0) {
      return partitionPath.substring(indexOf + 1);
    }
    return partitionPath;
  }

  private void extract(Path target) throws IOException {
    try (ZipInputStream zip = new ZipInputStream(this.getClass().getClassLoader().getResourceAsStream("file-group-reader/bootstrap_data.zip"))) {
      ZipEntry entry;

      while ((entry = zip.getNextEntry()) != null) {
        File file = target.resolve(entry.getName()).toFile();
        if (entry.isDirectory()) {
          file.mkdirs();
          continue;
        }
        byte[] buffer = new byte[10000];
        file.getParentFile().mkdirs();
        try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(file.toPath()))) {
          int count;
          while ((count = zip.read(buffer)) != -1) {
            out.write(buffer, 0, count);
          }
        }
      }
    }
  }
}
