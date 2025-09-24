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

import org.apache.hudi.avro.HoodieAvroUtils;
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
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StorageConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.table.HoodieTableConfig.PARTITION_FIELDS;
import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_MODE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_STRATEGY_ID;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
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
  private static final List<HoodieFileFormat> DEFAULT_SUPPORTED_FILE_FORMATS = Arrays.asList(HoodieFileFormat.PARQUET, HoodieFileFormat.ORC);
  protected static List<HoodieFileFormat> supportedFileFormats;
  private static final String KEY_FIELD_NAME = "_row_key";
  protected static final String ORDERING_FIELD_NAME = "timestamp";
  private static final String PARTITION_FIELD_NAME = "partition_path";
  private static final String RIDER_FIELD_NAME = "rider";
  @TempDir
  protected java.nio.file.Path tempDir;

  public abstract StorageConfiguration<?> getStorageConf();

  public abstract String getBasePath();

  public abstract HoodieReaderContext<T> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf, HoodieTableMetaClient metaClient);

  public abstract String getCustomPayload();

  public abstract void commitToTable(List<HoodieRecord> recordList,
                                     String operation,
                                     boolean firstCommit,
                                     Map<String, String> writeConfigs,
                                     String schemaStr);

  public void commitToTable(List<HoodieRecord> recordList,
                            String operation,
                            boolean firstCommit,
                            Map<String, String> writeConfigs) {
    commitToTable(recordList, operation, firstCommit, writeConfigs, TRIP_EXAMPLE_SCHEMA);
  }

  public abstract void assertRecordsEqual(Schema schema, T expected, T actual);

  public abstract void assertRecordMatchesSchema(Schema schema, T record);

  public abstract HoodieTestDataGenerator.SchemaEvolutionConfigs getSchemaEvolutionConfigs();

  private static Stream<Arguments> supportedBaseFileFormatArgs() {
    return supportedFileFormats.stream()
        .map(Arguments::of);
  }

  private static Stream<Arguments> testArguments() {
    boolean supportsORC = supportedFileFormats.contains(HoodieFileFormat.ORC);
    return Stream.of(
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, supportsORC ? HoodieFileFormat.ORC : HoodieFileFormat.PARQUET, "avro", false),
        arguments(RecordMergeMode.COMMIT_TIME_ORDERING, HoodieFileFormat.PARQUET, "parquet", true),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, supportsORC ? HoodieFileFormat.ORC : HoodieFileFormat.PARQUET, "avro", true),
        arguments(RecordMergeMode.EVENT_TIME_ORDERING, HoodieFileFormat.PARQUET, "parquet", true),
        arguments(RecordMergeMode.CUSTOM, HoodieFileFormat.PARQUET, "avro", false),
        arguments(RecordMergeMode.CUSTOM, HoodieFileFormat.PARQUET, "parquet", true)
    );
  }

  @BeforeAll
  public static void setUpClass() throws IOException {
    supportedFileFormats = new ArrayList<>(DEFAULT_SUPPORTED_FILE_FORMATS);
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testReadFileGroupInMergeOnReadTable(RecordMergeMode recordMergeMode, HoodieFileFormat baseFileFormat, String logDataBlockFormat, boolean populateMetaFields) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, populateMetaFields));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logDataBlockFormat);
    writeConfigs.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.name());

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // One commit; reading one file group containing a base file only
      List<HoodieRecord> initialRecords = dataGen.generateInserts("001", 100);
1      commitToTable(initialRecords, INSERT.value(), true, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 0, recordMergeMode,
          initialRecords, initialRecords, new String[]{ORDERING_FIELD_NAME});

      // Two commits; reading one file group containing a base file and a log file
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      List<HoodieRecord> unmergedRecords = CollectionUtils.combine(initialRecords, updates);
      commitToTable(updates, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 1, recordMergeMode,
          allRecords, unmergedRecords, new String[]{ORDERING_FIELD_NAME});

      // Three commits; reading one file group containing a base file and two log files
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates("003", 100);
      List<HoodieRecord> finalRecords = mergeRecordLists(updates2, allRecords);
      commitToTable(updates2, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 2, recordMergeMode,
          finalRecords, CollectionUtils.combine(unmergedRecords, updates2), new String[]{ORDERING_FIELD_NAME});
    }
  }

  @Test
  public void testReadFileGroupWithMultipleOrderingFields() throws Exception {
    RecordMergeMode recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, true));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro");
    writeConfigs.put("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name());
    // Use two precombine values - combination of timestamp and rider
    String orderingFields = "timestamp,rider";
    writeConfigs.put(HoodieTableConfig.ORDERING_FIELDS.key(), orderingFields);
    writeConfigs.put("hoodie.payload.ordering.field", orderingFields);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // Initial commit. rider column gets value of rider-002
      List<HoodieRecord> initialRecords = dataGen.generateInserts("002", 100);
      long initialTs = (long) ((GenericRecord) initialRecords.get(0).getData()).get("timestamp");
      commitToTable(initialRecords, INSERT.value(), true, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 0, recordMergeMode,
          initialRecords, initialRecords, orderingFields.split(","));

      // The updates have rider values as rider-001 and the existing records have rider values as rider-002
      // timestamp is 0 for all records so will not be considered
      // All updates in this batch will be ignored as rider values are smaller and timestamp value is same
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("001", 5, initialTs);
      List<HoodieRecord> allRecords = initialRecords;
      List<HoodieRecord> unmergedRecords = CollectionUtils.combine(updates, allRecords);
      commitToTable(updates, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 1, recordMergeMode,
          allRecords, unmergedRecords, orderingFields.split(","));

      // The updates have rider values as rider-003 and the existing records have rider values as rider-002
      // timestamp is 0 for all records so will not be considered
      // All updates in this batch will reflect in the final records
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates("003", 10, initialTs);
      List<HoodieRecord> finalRecords = mergeRecordLists(updates2, allRecords);
      commitToTable(updates2, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 2, recordMergeMode,
          finalRecords, CollectionUtils.combine(unmergedRecords, updates2), orderingFields.split(","));
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
      commitToTable(initialRecords, INSERT.value(), true, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 1, recordMergeMode,
          initialRecords, initialRecords, new String[]{ORDERING_FIELD_NAME});

      // Two commits; reading one file group containing two log files
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      commitToTable(updates, INSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 2, recordMergeMode,
          allRecords, CollectionUtils.combine(initialRecords, updates), new String[]{ORDERING_FIELD_NAME});
    }
  }

  private static List<Pair<String, IndexedRecord>> hoodieRecordsToIndexedRecords(List<HoodieRecord> hoodieRecords, Schema schema) {
    return hoodieRecords.stream().map(r -> {
      try {
        return r.toIndexedRecord(schema, CollectionUtils.emptyProps());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).filter(Option::isPresent).map(Option::get).map(r -> Pair.of(r.getRecordKey(), r.getData())).collect(Collectors.toList());
  }

  /**
   * Write a base file with schema A, then write another base file with schema B.
   */
  @ParameterizedTest
  @MethodSource("supportedBaseFileFormatArgs")
  public void testSchemaEvolutionWhenBaseFilesWithDifferentSchema(HoodieFileFormat fileFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(
        getCommonConfigs(RecordMergeMode.EVENT_TIME_ORDERING, true));
    HoodieTestDataGenerator.SchemaEvolutionConfigs schemaEvolutionConfigs = getSchemaEvolutionConfigs();
    writeConfigs.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.name());
    if (fileFormat == HoodieFileFormat.ORC) {
      // ORC can support reading float as string, but it converts float to double to string causing differences in precision
      schemaEvolutionConfigs.floatToDoubleSupport = false;
      schemaEvolutionConfigs.floatToStringSupport = false;
    }

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(TRIP_EXAMPLE_SCHEMA, 0xDEEF)) {
      dataGen.extendSchemaBeforeEvolution(schemaEvolutionConfigs);

      // Write a base file with schema A
      List<HoodieRecord> firstRecords = dataGen.generateInsertsForPartition("001", 5, "any_partition");
      List<Pair<String, IndexedRecord>> firstIndexedRecords = hoodieRecordsToIndexedRecords(firstRecords, dataGen.getExtendedSchema());
      commitToTable(firstRecords, INSERT.value(), true, writeConfigs, dataGen.getExtendedSchema().toString());
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 0, RecordMergeMode.EVENT_TIME_ORDERING,
          firstIndexedRecords);

      // Evolve schema
      dataGen.extendSchemaAfterEvolution(schemaEvolutionConfigs);

      // Write another base file with schema B
      List<HoodieRecord> secondRecords = dataGen.generateInsertsForPartition("002", 5, "new_partition");
      List<Pair<String, IndexedRecord>> secondIndexedRecords = hoodieRecordsToIndexedRecords(secondRecords, dataGen.getExtendedSchema());
      commitToTable(secondRecords, INSERT.value(), false, writeConfigs, dataGen.getExtendedSchema().toString());
      List<Pair<String, IndexedRecord>> mergedRecords = CollectionUtils.combine(firstIndexedRecords, secondIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 0, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);
    }
  }

  private static Stream<Arguments> testArgsForDifferentBaseAndLogFormats() {
    boolean supportsORC = supportedFileFormats.contains(HoodieFileFormat.ORC);
    List<Arguments> args = new ArrayList<>();
    
    if (supportsORC) {
      args.add(arguments(HoodieFileFormat.ORC, "avro"));
    }
    
    args.add(arguments(HoodieFileFormat.PARQUET, "avro"));
    args.add(arguments(HoodieFileFormat.PARQUET, "parquet"));
    
    return args.stream();
  }
  
  /**
   * Write a base file with schema A, then write a log file with schema A, then write another base file with schema B.
   */
  @ParameterizedTest
  @MethodSource("testArgsForDifferentBaseAndLogFormats")
  public void testSchemaEvolutionWhenBaseFileHasDifferentSchemaThanLogFiles(HoodieFileFormat fileFormat, String logFileFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(
        getCommonConfigs(RecordMergeMode.EVENT_TIME_ORDERING, true));
    writeConfigs.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.name());
    writeConfigs.put(HoodieTableConfig.LOG_FILE_FORMAT.key(), logFileFormat);
    HoodieTestDataGenerator.SchemaEvolutionConfigs schemaEvolutionConfigs = getSchemaEvolutionConfigs();
    if (fileFormat == HoodieFileFormat.ORC) {
      // ORC can support reading float as string, but it converts float to double to string causing differences in precision
      schemaEvolutionConfigs.floatToDoubleSupport = false;
      schemaEvolutionConfigs.floatToStringSupport = false;
    }

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(TRIP_EXAMPLE_SCHEMA, 0xDEEF)) {
      dataGen.extendSchemaBeforeEvolution(schemaEvolutionConfigs);

      // Write a base file with schema A
      List<HoodieRecord> firstRecords = dataGen.generateInsertsForPartition("001", 10, "any_partition");
      List<Pair<String, IndexedRecord>> firstIndexedRecords = hoodieRecordsToIndexedRecords(firstRecords, dataGen.getExtendedSchema());
      commitToTable(firstRecords, INSERT.value(), true, writeConfigs, dataGen.getExtendedSchema().toString());
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 0, RecordMergeMode.EVENT_TIME_ORDERING,
          firstIndexedRecords);

      // Write a log file with schema A
      List<HoodieRecord> secondRecords = dataGen.generateUniqueUpdates("002", 5);
      List<Pair<String, IndexedRecord>> secondIndexedRecords = hoodieRecordsToIndexedRecords(secondRecords, dataGen.getExtendedSchema());
      commitToTable(secondRecords, UPSERT.value(), false, writeConfigs, dataGen.getExtendedSchema().toString());
      List<Pair<String, IndexedRecord>> mergedRecords = mergeIndexedRecordLists(secondIndexedRecords, firstIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 1, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);

      // Evolve schema
      dataGen.extendSchemaAfterEvolution(schemaEvolutionConfigs);

      // Write another base file with schema B
      List<HoodieRecord> thirdRecords = dataGen.generateInsertsForPartition("003", 5, "new_partition");
      List<Pair<String, IndexedRecord>> thirdIndexedRecords = hoodieRecordsToIndexedRecords(thirdRecords, dataGen.getExtendedSchema());
      commitToTable(thirdRecords, INSERT.value(), false, writeConfigs, dataGen.getExtendedSchema().toString());
      mergedRecords = CollectionUtils.combine(mergedRecords, thirdIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          // use -1 to prevent validation of numlogfiles because one fg has a log file but the other doesn't
          true, -1, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);
    }
  }

  /**
   * Write a base file with schema A, then write a log file with schema A, then write another log file with schema B.
   */
  @ParameterizedTest
  @MethodSource("supportedBaseFileFormatArgs")
  public void testSchemaEvolutionWhenLogFilesWithDifferentSchema(HoodieFileFormat fileFormat) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(
        getCommonConfigs(RecordMergeMode.EVENT_TIME_ORDERING, true));
    writeConfigs.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), fileFormat.name());
    HoodieTestDataGenerator.SchemaEvolutionConfigs schemaEvolutionConfigs = getSchemaEvolutionConfigs();
    if (fileFormat == HoodieFileFormat.ORC) {
      // ORC can support reading float as string, but it converts float to double to string causing differences in precision
      schemaEvolutionConfigs.floatToDoubleSupport = false;
      schemaEvolutionConfigs.floatToStringSupport = false;
    }

    try (HoodieTestDataGenerator baseFileDataGen =
             new HoodieTestDataGenerator(TRIP_EXAMPLE_SCHEMA, 0xDEEF)) {
      baseFileDataGen.extendSchemaBeforeEvolution(schemaEvolutionConfigs);

      // Write base file with schema A
      List<HoodieRecord> firstRecords = baseFileDataGen.generateInserts("001", 100);
      List<Pair<String, IndexedRecord>> firstIndexedRecords = hoodieRecordsToIndexedRecords(firstRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(firstRecords, INSERT.value(), true, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 0, RecordMergeMode.EVENT_TIME_ORDERING,
          firstIndexedRecords);

      // Write log file with schema A
      List<HoodieRecord> secondRecords = baseFileDataGen.generateUniqueUpdates("002", 50);
      List<Pair<String, IndexedRecord>> secondIndexedRecords = hoodieRecordsToIndexedRecords(secondRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(secondRecords, UPSERT.value(), false, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      List<Pair<String, IndexedRecord>> mergedRecords = mergeIndexedRecordLists(secondIndexedRecords, firstIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 1, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);

      // Evolve schema
      baseFileDataGen.extendSchemaAfterEvolution(schemaEvolutionConfigs);

      // Write log file with schema B
      List<HoodieRecord> thirdRecords = baseFileDataGen.generateUniqueUpdates("003", 50);
      List<Pair<String, IndexedRecord>> thirdIndexedRecords = hoodieRecordsToIndexedRecords(thirdRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(thirdRecords, UPSERT.value(), false, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      mergedRecords = mergeIndexedRecordLists(thirdIndexedRecords, mergedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 2, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);
    }
  }

  /**
   * Write a base file with schema A, then write a log file with schema A, then write another log file with schema B. Then write a different base file with schema C.
   */
  @Test
  public void testSchemaEvolutionWhenLogFilesWithDifferentSchemaAndTableSchemaDiffers() throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(
        getCommonConfigs(RecordMergeMode.EVENT_TIME_ORDERING, true));

    try (HoodieTestDataGenerator baseFileDataGen =
             new HoodieTestDataGenerator(TRIP_EXAMPLE_SCHEMA, 0xDEEF)) {
      baseFileDataGen.extendSchemaBeforeEvolution(getSchemaEvolutionConfigs());

      // Write base file with schema A
      List<HoodieRecord> firstRecords = baseFileDataGen.generateInserts("001", 100);
      List<Pair<String, IndexedRecord>> firstIndexedRecords = hoodieRecordsToIndexedRecords(firstRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(firstRecords, INSERT.value(), true, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 0, RecordMergeMode.EVENT_TIME_ORDERING,
          firstIndexedRecords);

      // Write log file with schema A
      List<HoodieRecord> secondRecords = baseFileDataGen.generateUniqueUpdates("002", 50);
      List<Pair<String, IndexedRecord>> secondIndexedRecords = hoodieRecordsToIndexedRecords(secondRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(secondRecords, UPSERT.value(), false, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      List<Pair<String, IndexedRecord>> mergedRecords = mergeIndexedRecordLists(secondIndexedRecords, firstIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 1, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);

      // Evolve schema
      HoodieTestDataGenerator.SchemaEvolutionConfigs schemaEvolutionConfigs = getSchemaEvolutionConfigs();
      boolean addNewFieldSupport = schemaEvolutionConfigs.addNewFieldSupport;
      schemaEvolutionConfigs.addNewFieldSupport = false;
      baseFileDataGen.extendSchemaAfterEvolution(schemaEvolutionConfigs);

      // Write log file with schema B
      List<HoodieRecord> thirdRecords = baseFileDataGen.generateUniqueUpdates("003", 50);
      List<Pair<String, IndexedRecord>> thirdIndexedRecords = hoodieRecordsToIndexedRecords(thirdRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(thirdRecords, UPSERT.value(), false, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      mergedRecords = mergeIndexedRecordLists(thirdIndexedRecords, mergedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 2, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);

      // Evolve schema again
      schemaEvolutionConfigs = getSchemaEvolutionConfigs();
      schemaEvolutionConfigs.addNewFieldSupport = addNewFieldSupport;
      baseFileDataGen.extendSchemaAfterEvolution(schemaEvolutionConfigs);

      // Write another base file with schema C
      List<HoodieRecord> fourthRecords = baseFileDataGen.generateInsertsForPartition("004", 5, "new_partition");
      List<Pair<String, IndexedRecord>> fourthIndexedRecords = hoodieRecordsToIndexedRecords(fourthRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(fourthRecords, INSERT.value(), false, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      mergedRecords = CollectionUtils.combine(mergedRecords, fourthIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          // use -1 to prevent validation of numlogfiles because one fg has log files but the other doesn't
          true, -1, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);
    }
  }

  /**
   * Write a base file with schema A, then write a log file with schema B
   */
  @Test
  public void testSchemaEvolutionWhenBaseFilesWithDifferentSchemaFromLogFiles() throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(
        getCommonConfigs(RecordMergeMode.EVENT_TIME_ORDERING, true));

    try (HoodieTestDataGenerator baseFileDataGen =
             new HoodieTestDataGenerator(TRIP_EXAMPLE_SCHEMA, 0xDEEF)) {
      baseFileDataGen.extendSchemaBeforeEvolution(getSchemaEvolutionConfigs());

      // Write base file with schema A
      List<HoodieRecord> firstRecords = baseFileDataGen.generateInserts("001", 100);
      List<Pair<String, IndexedRecord>> firstIndexedRecords = hoodieRecordsToIndexedRecords(firstRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(firstRecords, INSERT.value(), true, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 0, RecordMergeMode.EVENT_TIME_ORDERING,
          firstIndexedRecords);

      //Evolve schema
      baseFileDataGen.extendSchemaAfterEvolution(getSchemaEvolutionConfigs());

      // Write log file with schema B
      List<HoodieRecord> secondRecords = baseFileDataGen.generateUniqueUpdates("002", 50);
      List<Pair<String, IndexedRecord>> secondIndexedRecords = hoodieRecordsToIndexedRecords(secondRecords, baseFileDataGen.getExtendedSchema());
      commitToTable(secondRecords, UPSERT.value(), false, writeConfigs, baseFileDataGen.getExtendedSchema().toString());
      List<Pair<String, IndexedRecord>> mergedRecords = mergeIndexedRecordLists(secondIndexedRecords, firstIndexedRecords);
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, 1, RecordMergeMode.EVENT_TIME_ORDERING,
          mergedRecords);
    }
  }

  @Test
  public void testReadFileGroupInBootstrapMergeOnReadTable() throws Exception {
    Path zipOutput = Paths.get(new URI(getBasePath()));
    HoodieTestUtils.extractZipToDirectory("file-group-reader/bootstrap_data.zip", zipOutput, getClass());
    ObjectMapper objectMapper = new ObjectMapper();
    Path basePath = zipOutput.resolve("bootstrap_data");
    List<HoodieTestDataGenerator.RecordIdentifier> expectedRecords = new ArrayList<>();
    objectMapper.reader().forType(HoodieTestDataGenerator.RecordIdentifier.class).<HoodieTestDataGenerator.RecordIdentifier>readValues(basePath.resolve("merged_records.json").toFile())
        .forEachRemaining(expectedRecords::add);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedUnMergedRecords = new ArrayList<>();
    objectMapper.reader().forType(HoodieTestDataGenerator.RecordIdentifier.class).<HoodieTestDataGenerator.RecordIdentifier>readValues(basePath.resolve("unmerged_records.json").toFile())
        .forEachRemaining(expectedUnMergedRecords::add);
    expectedRecords = expectedRecords.stream()
        .map(recordIdentifier -> HoodieTestDataGenerator.RecordIdentifier.clone(recordIdentifier, recordIdentifier.getOrderingVal()))
        .collect(Collectors.toList());
    expectedUnMergedRecords = expectedUnMergedRecords.stream()
        .map(recordIdentifier -> HoodieTestDataGenerator.RecordIdentifier.clone(recordIdentifier, recordIdentifier.getOrderingVal()))
        .collect(Collectors.toList());
    validateOutputFromFileGroupReaderWithExistingRecords(getStorageConf(), basePath.toString(), true, 1, RecordMergeMode.EVENT_TIME_ORDERING,
        expectedRecords, expectedUnMergedRecords);
  }

  @ParameterizedTest
  @EnumSource(value = ExternalSpillableMap.DiskMapType.class)
  public void testSpillableMapUsage(ExternalSpillableMap.DiskMapType diskMapType) throws Exception {
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(RecordMergeMode.COMMIT_TIME_ORDERING, true));
    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      List<HoodieRecord> recordList = dataGen.generateInserts("001", 100);
      long timestamp = (long) ((GenericRecord) recordList.get(0).getData()).get("timestamp");
      commitToTable(recordList, INSERT.value(), true, writeConfigs);
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
            String recordKey = readerContext.getRecordContext().getRecordKey(record, avroSchema);
            //test key based
            BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(record, avroSchema, readerContext.getRecordContext(), Collections.singletonList("timestamp"), false);
            spillableMap.put(recordKey, bufferedRecord.toBinary(readerContext.getRecordContext()));

            //test position based
            spillableMap.put(position++, bufferedRecord.toBinary(readerContext.getRecordContext()));
          }

          assertEquals(records.size() * 2, spillableMap.size());
          //Validate that everything is correct
          position = 0L;
          for (T record : records) {
            String recordKey = readerContext.getRecordContext().getRecordKey(record, avroSchema);
            BufferedRecord<T> keyBased = spillableMap.get(recordKey);
            assertNotNull(keyBased);
            BufferedRecord<T> positionBased = spillableMap.get(position++);
            assertNotNull(positionBased);
            assertRecordsEqual(avroSchema, record, keyBased.getRecord());
            assertRecordsEqual(avroSchema, record, positionBased.getRecord());
            assertEquals(keyBased.getRecordKey(), recordKey);
            assertEquals(positionBased.getRecordKey(), recordKey);
            assertEquals(avroSchema, readerContext.getRecordContext().getSchemaFromBufferRecord(keyBased));
            assertEquals(readerContext.getRecordContext().convertValueToEngineType(timestamp), positionBased.getOrderingValue());
          }
        }
      }
    }
  }

  protected Map<String, String> getCommonConfigs(RecordMergeMode recordMergeMode, boolean populateMetaFields) {
    Map<String, String> configMapping = new HashMap<>();
    configMapping.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), KEY_FIELD_NAME);
    configMapping.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), PARTITION_FIELD_NAME);
    configMapping.put(HoodieTableConfig.ORDERING_FIELDS.key(), recordMergeMode != RecordMergeMode.COMMIT_TIME_ORDERING ? ORDERING_FIELD_NAME : "");
    configMapping.put("hoodie.payload.ordering.field", ORDERING_FIELD_NAME);
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

  private void validateOutputFromFileGroupReaderWithNativeRecords(StorageConfiguration<?> storageConf,
                                                                    String tablePath,
                                                                    boolean containsBaseFile,
                                                                    int expectedLogFileNum,
                                                                    RecordMergeMode recordMergeMode,
                                                                    List<Pair<String, IndexedRecord>> expectedRecords) throws Exception {
    Set<String> metaCols = new HashSet<>(HoodieRecord.HOODIE_META_COLUMNS);
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, tablePath);
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Schema avroSchema = resolver.getTableAvroSchema();
    Schema avroSchemaWithoutMeta = resolver.getTableAvroSchema(false);
    // use reader context for conversion to engine specific objects
    HoodieReaderContext<T> readerContext = getHoodieReaderContext(tablePath, avroSchema, getStorageConf(), metaClient);
    List<FileSlice> fileSlices = getFileSlicesToRead(storageConf, tablePath, metaClient, containsBaseFile, expectedLogFileNum);
    boolean sortOutput = !containsBaseFile;
    List<T> actualRecordList =
        readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode, false, sortOutput);
    assertEquals(expectedRecords.size(), actualRecordList.size());
    actualRecordList.forEach(r -> assertRecordMatchesSchema(avroSchema, r));
    Set<GenericRecord> actualRecordSet = actualRecordList.stream().map(r ->  readerContext.getRecordContext().convertToAvroRecord(r, avroSchema))
        .map(r -> HoodieAvroUtils.removeFields(r, metaCols))
        .collect(Collectors.toSet());
    Set<GenericRecord> expectedRecordSet = expectedRecords.stream()
        .map(r -> resetByteBufferPosition((GenericRecord) r.getRight()))
        .map(r -> HoodieAvroUtils.rewriteRecordWithNewSchema(r, avroSchemaWithoutMeta))
        .collect(Collectors.toSet());
    compareRecordSets(expectedRecordSet, actualRecordSet);
  }

  private void compareRecordSets(Set<GenericRecord> expectedRecordSet, Set<GenericRecord> actualRecordSet) {
    Map<String, GenericRecord> expectedMap = new HashMap<>(expectedRecordSet.size());
    for (GenericRecord expectedRecord : expectedRecordSet) {
      expectedMap.put(expectedRecord.get("_row_key").toString(), expectedRecord);
    }
    Map<String, GenericRecord> actualMap = new HashMap<>(actualRecordSet.size());
    for (GenericRecord actualRecord : actualRecordSet) {
      actualMap.put(actualRecord.get("_row_key").toString(), actualRecord);
    }
    assertEquals(expectedMap.keySet(), actualMap.keySet());
    for (String key : actualMap.keySet()) {
      GenericRecord expectedRecord = expectedMap.get(key);
      GenericRecord actualRecord = actualMap.get(key);
      assertEquals(expectedRecord, actualRecord);
    }
  }

  protected void validateOutputFromFileGroupReader(StorageConfiguration<?> storageConf,
                                                   String tablePath,
                                                   boolean containsBaseFile,
                                                   int expectedLogFileNum,
                                                   RecordMergeMode recordMergeMode,
                                                   List<HoodieRecord> expectedHoodieRecords,
                                                   List<HoodieRecord> expectedHoodieUnmergedRecords,
                                                   String[] orderingFields) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, tablePath);
    Schema avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    expectedHoodieRecords = getExpectedHoodieRecordsWithOrderingValue(expectedHoodieRecords, metaClient, avroSchema);
    expectedHoodieUnmergedRecords = getExpectedHoodieRecordsWithOrderingValue(expectedHoodieUnmergedRecords, metaClient, avroSchema);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedRecords = convertHoodieRecords(expectedHoodieRecords, avroSchema, orderingFields);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedUnmergedRecords = convertHoodieRecords(expectedHoodieUnmergedRecords, avroSchema, orderingFields);
    validateOutputFromFileGroupReaderWithExistingRecords(
        storageConf, tablePath, containsBaseFile, expectedLogFileNum, recordMergeMode,
        expectedRecords, expectedUnmergedRecords);
  }

  private static List<HoodieRecord> getExpectedHoodieRecordsWithOrderingValue(List<HoodieRecord> expectedHoodieRecords, HoodieTableMetaClient metaClient, Schema avroSchema) {
    return expectedHoodieRecords.stream().map(rec -> {
      List<String> orderingFields = metaClient.getTableConfig().getOrderingFields();
      HoodieAvroIndexedRecord avroRecord = ((HoodieAvroIndexedRecord) rec);
      Comparable orderingValue = OrderingValues.create(orderingFields, field -> (Comparable) avroRecord.getColumnValueAsJava(avroSchema, field, new TypedProperties()));
      return new HoodieAvroIndexedRecord(rec.getKey(), avroRecord.getData(), orderingValue);
    }).collect(Collectors.toList());
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
        avroSchema, readerContext, metaClient.getTableConfig().getOrderingFields());
    // validate size is equivalent to ensure no duplicates are returned
    assertEquals(expectedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedRecords), new HashSet<>(actualRecordList));
    // validate records can be read from file group as HoodieRecords
    actualRecordList = convertHoodieRecords(
        readHoodieRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode),
        avroSchema, readerContext, metaClient.getTableConfig().getOrderingFields());
    assertEquals(expectedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedRecords), new HashSet<>(actualRecordList));
    // validate unmerged records
    actualRecordList = convertEngineRecords(
        readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode, true, false),
        avroSchema, readerContext, metaClient.getTableConfig().getOrderingFields());
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
      if (expectedLogFileNum >= 0) {
        assertEquals(expectedLogFileNum, logFilePathList.size());
      }
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

  private HoodieFileGroupReader<T> getHoodieFileGroupReader(StorageConfiguration<?> storageConf,
                                                            String tablePath,
                                                            HoodieTableMetaClient metaClient,
                                                            Schema avroSchema,
                                                            FileSlice fileSlice,
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
          String currentKey = readerContext.getRecordContext().getRecordKey(next, avroSchema);
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
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), metaClient.getTableConfig().getOrderingFieldsStr().orElse(""));
    props.setProperty("hoodie.payload.ordering.field", metaClient.getTableConfig().getOrderingFieldsStr().orElse(""));
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

  protected List<Pair<String, IndexedRecord>> mergeIndexedRecordLists(List<Pair<String, IndexedRecord>> updates, List<Pair<String, IndexedRecord>> existing) {
    Set<String> updatedKeys = updates.stream().map(Pair::getLeft).collect(Collectors.toSet());
    return Stream.concat(updates.stream(), existing.stream().filter(record -> !updatedKeys.contains(record.getLeft())))
        .collect(Collectors.toList());
  }

  protected List<HoodieRecord> mergeRecordLists(List<HoodieRecord> updates, List<HoodieRecord> existing) {
    Set<String> updatedKeys = updates.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet());
    return Stream.concat(updates.stream(), existing.stream().filter(record -> !updatedKeys.contains(record.getRecordKey())))
            .collect(Collectors.toList());
  }

  private List<HoodieTestDataGenerator.RecordIdentifier> convertHoodieRecords(List<HoodieRecord> records, Schema schema, String[] orderingFields) {
    return records.stream().map(record -> HoodieTestDataGenerator.RecordIdentifier.fromTripTestPayload((HoodieAvroIndexedRecord) record, orderingFields)).collect(Collectors.toList());
  }

  private List<HoodieTestDataGenerator.RecordIdentifier> convertEngineRecords(List<T> records, Schema schema, HoodieReaderContext<T> readerContext, List<String> preCombineFields) {
    return records.stream()
        .map(record -> new HoodieTestDataGenerator.RecordIdentifier(
            readerContext.getRecordContext().getValue(record, schema, KEY_FIELD_NAME).toString(),
            readerContext.getRecordContext().getValue(record, schema, PARTITION_FIELD_NAME).toString(),
            OrderingValues.create(preCombineFields,
                    field -> (Comparable) readerContext.getRecordContext().getValue(record, schema, field))
                .toString(),
            readerContext.getRecordContext().getValue(record, schema, RIDER_FIELD_NAME).toString()))
        .collect(Collectors.toList());
  }

  private List<HoodieTestDataGenerator.RecordIdentifier> convertHoodieRecords(List<HoodieRecord<T>> records, Schema schema, HoodieReaderContext<T> readerContext,
                                                                              List<String> orderingFields) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), String.join(",", orderingFields));
    return records.stream()
        .map(record -> {
          T data = readerContext.getRecordContext().extractDataFromRecord(record, schema, props);
          return new HoodieTestDataGenerator.RecordIdentifier(
              record.getRecordKey(),
              removeHiveStylePartition(record.getPartitionPath()),
              record.getOrderingValue(schema, props, orderingFields.toArray(new String[0])).toString(),
              readerContext.getRecordContext().getValue(data, schema, RIDER_FIELD_NAME).toString());
        })
        .collect(Collectors.toList());
  }

  private static String removeHiveStylePartition(String partitionPath) {
    int indexOf = partitionPath.indexOf("=");
    if (indexOf > 0) {
      return partitionPath.substring(indexOf + 1);
    }
    return partitionPath;
  }

  private static IndexedRecord resetByteBufferPosition(IndexedRecord record) {
    for (Schema.Field field : record.getSchema().getFields()) {
      Object value = record.get(field.pos());
      resetByteBufferField(value, field.schema());
    }
    return record;
  }

  private static void resetByteBufferField(Object value, Schema fieldSchema) {
    if (value == null) {
      return;
    }
    Schema.Type fieldType = HoodieAvroUtils.unwrapNullable(fieldSchema).getType();
    if (fieldType == Schema.Type.BYTES || fieldType == Schema.Type.FIXED) {
      // Reset position of ByteBuffer or Fixed type fields
      if (value instanceof ByteBuffer) {
        ((ByteBuffer) value).rewind();
      }
    } else if (fieldType == Schema.Type.RECORD) {
      resetByteBufferPosition((IndexedRecord) value);
    } else if (fieldType == Schema.Type.ARRAY) {
      ((List<Object>) value).forEach(element -> resetByteBufferField(element, fieldSchema.getElementType()));
    } else if (fieldType == Schema.Type.MAP) {
      ((Map<Object, Object>) value).values().forEach(element -> resetByteBufferField(element, fieldSchema.getValueType()));
    }
  }
}
