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

import org.apache.hudi.avro.AvroSchemaTestUtils;
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
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.testutils.SchemaEvolutionTestUtilsBase;
import org.apache.hudi.common.testutils.SchemaOnReadEvolutionTestUtils;
import org.apache.hudi.common.testutils.SchemaOnWriteEvolutionTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
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
  private static final String PRECOMBINE_FIELD_NAME = "timestamp";
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

  public abstract void commitSchemaToTable(InternalSchema schema, Map<String, String> writeConfigs, String historySchemaStr);

  public abstract void assertRecordsEqual(Schema schema, T expected, T actual);

  public abstract void assertRecordMatchesSchema(Schema schema, T record);

  public abstract SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs getSchemaOnWriteConfigs();

  public abstract SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs getSchemaOnReadConfigs();

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
      commitToTable(initialRecords, INSERT.value(), true, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 0, recordMergeMode,
          initialRecords, initialRecords);

      // Two commits; reading one file group containing a base file and a log file
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      List<HoodieRecord> unmergedRecords = CollectionUtils.combine(initialRecords, updates);
      commitToTable(updates, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 1, recordMergeMode,
          allRecords, unmergedRecords);

      // Three commits; reading one file group containing a base file and two log files
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates("003", 100);
      List<HoodieRecord> finalRecords = mergeRecordLists(updates2, allRecords);
      commitToTable(updates2, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 2, recordMergeMode,
          finalRecords, CollectionUtils.combine(unmergedRecords, updates2));
    }
  }

  @Test
  public void testReadFileGroupWithMultipleOrderingFields() throws Exception {
    RecordMergeMode recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    Map<String, String> writeConfigs = new HashMap<>(getCommonConfigs(recordMergeMode, true));
    writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro");
    writeConfigs.put("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name());
    // Use two precombine values - combination of timestamp and rider
    String orderingValues = "timestamp,rider";
    writeConfigs.put("hoodie.datasource.write.precombine.field", orderingValues);
    writeConfigs.put("hoodie.payload.ordering.field", orderingValues);

    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEEF)) {
      // Initial commit. rider column gets value of rider-002
      List<HoodieRecord> initialRecords = dataGen.generateInserts("002", 100);
      commitToTable(initialRecords, INSERT.value(), true, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 0, recordMergeMode,
          initialRecords, initialRecords);

      // The updates have rider values as rider-001 and the existing records have rider values as rider-002
      // timestamp is 0 for all records so will not be considered
      // All updates in this batch will be ignored as rider values are smaller and timestamp value is same
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("001", 5);
      List<HoodieRecord> allRecords = initialRecords;
      List<HoodieRecord> unmergedRecords = CollectionUtils.combine(updates, allRecords);
      commitToTable(updates, UPSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), true, 1, recordMergeMode,
          allRecords, unmergedRecords);

      // The updates have rider values as rider-003 and the existing records have rider values as rider-002
      // timestamp is 0 for all records so will not be considered
      // All updates in this batch will reflect in the final records
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates("003", 10);
      List<HoodieRecord> finalRecords = mergeRecordLists(updates2, allRecords);
      commitToTable(updates2, UPSERT.value(), false, writeConfigs);
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
      commitToTable(initialRecords, INSERT.value(), true, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 1, recordMergeMode,
          initialRecords, initialRecords);

      // Two commits; reading one file group containing two log files
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates("002", 50);
      List<HoodieRecord> allRecords = mergeRecordLists(updates, initialRecords);
      commitToTable(updates, INSERT.value(), false, writeConfigs);
      validateOutputFromFileGroupReader(
          getStorageConf(), getBasePath(), false, 2, recordMergeMode,
          allRecords, CollectionUtils.combine(initialRecords, updates));
    }
  }

  @ParameterizedTest
  @EnumSource(value = SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType.class)
  public void testSchemaOnRead(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType) throws Exception {
    try (SchemaOnReadTestExecutor executor = new SchemaOnReadTestExecutor(testType,
        getSchemaOnReadConfigs(),
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue())) {
      executor.execute();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro", "parquet"})
  public void testSchemaOnReadLogBlocks(String logDataBlockFormat) throws Exception {
    try (SchemaOnReadTestExecutor executor = new SchemaOnReadTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType.BASE_FILE_HAS_DIFFERENT_SCHEMA_THAN_LOG_FILES,
        getSchemaOnReadConfigs(),
        HoodieFileFormat.PARQUET,
        Option.of(logDataBlockFormat))) {
      executor.execute();
    }
  }

  @ParameterizedTest
  @EnumSource(value = SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType.class)
  public void testSchemaOnWrite(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType) throws Exception {
    try (SchemaOnWriteTestExecutor executor = new SchemaOnWriteTestExecutor(testType,
        getSchemaOnWriteConfigs(),
        HoodieTableConfig.BASE_FILE_FORMAT.defaultValue())) {
      executor.execute();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"avro", "parquet"})
  public void testSchemaOnWriteLogBlocks(String logDataBlockFormat) throws Exception {
    try (SchemaOnWriteTestExecutor executor = new SchemaOnWriteTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType.BASE_FILE_HAS_DIFFERENT_SCHEMA_THAN_LOG_FILES,
        getSchemaOnWriteConfigs(),
        HoodieFileFormat.PARQUET,
        Option.of(logDataBlockFormat))) {
      executor.execute();
    }
  }

  @Test
  public void testSchemaOnWriteOrc() throws Exception {
    if (supportedFileFormats.contains(HoodieFileFormat.ORC)) {
      try (SchemaOnWriteTestExecutor executor = new SchemaOnWriteTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType.BASE_FILES_WITH_DIFFERENT_SCHEMA,
          getSchemaOnWriteConfigs(),
          HoodieFileFormat.ORC)) {
        executor.execute();
      }
    }
  }

  // Base class for executing schema evolution tests
  public abstract class AbstractSchemaEvolutionTestExecutor<C extends SchemaEvolutionTestUtilsBase.SchemaEvolutionConfigBase> implements SchemaEvolutionTestUtilsBase.SchemaEvolutionTestExecutor {

    protected final int maxIterations;
    private final SchemaEvolutionTestUtilsBase.SchemaEvolutionScenario scenario;
    protected C evolutionConfigs;
    protected final Map<String, String> writeConfigs;
    private final HoodieTestDataGenerator dataGen;
    protected List<Pair<String, IndexedRecord>> allRecords = new ArrayList<>();
    protected Schema extendedSchema;
    private boolean first = true;

    public AbstractSchemaEvolutionTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType,
                                               C evolutionConfigs,
                                               HoodieFileFormat baseFileFormat,
                                               Option<String> logFileFormat) {
      this.maxIterations = testType.getScenario().getMaxIterations();
      this.scenario = testType.getScenario();
      this.dataGen = new HoodieTestDataGenerator(TRIP_EXAMPLE_SCHEMA, 0xDEEF);
      this.evolutionConfigs = evolutionConfigs;
      if (baseFileFormat.equals(HoodieFileFormat.ORC)) {
        this.evolutionConfigs.floatToDoubleSupport = false;
        this.evolutionConfigs.floatToStringSupport = false;
      }
      this.writeConfigs = new HashMap<>(getCommonConfigs(RecordMergeMode.COMMIT_TIME_ORDERING, true));
      writeConfigs.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.name());
      if (logFileFormat.isPresent()) {
        writeConfigs.put(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logFileFormat.get());
      }
      initializeSchema();
      dataGen.addExtendedSchema(extendedSchema);
    }

    // create schema for iteration 0
    protected abstract void initializeSchema();

    // evolve the schema
    protected abstract void doEvolveSchema(int iteration);

    // does `allRecords` schema matches the current table schema
    protected abstract boolean areExpectedRecordsInFinalSchema();

    private List<Pair<String, IndexedRecord>> hoodieRecordsToIndexedRecords(List<HoodieRecord> hoodieRecords, Schema schema) {
      return hoodieRecords.stream().map(r -> {
        try {
          return r.toIndexedRecord(schema, CollectionUtils.emptyProps());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).filter(Option::isPresent).map(Option::get).map(r -> Pair.of(r.getRecordKey(), r.getData())).collect(Collectors.toList());
    }

    @Override
    public void writeData(SchemaEvolutionTestUtilsBase.WriteDataConfig dataConfig) throws Exception {
      if (dataConfig.isBaseFile) {
        List<HoodieRecord> records = dataConfig.partition == null
            ? dataGen.generateInserts(dataConfig.commitId, dataConfig.recordCount)
            : dataGen.generateInsertsForPartition(dataConfig.commitId, dataConfig.recordCount, dataConfig.partition);
        List<Pair<String, IndexedRecord>> indexedRecords = hoodieRecordsToIndexedRecords(records, extendedSchema);
        allRecords.addAll(indexedRecords);
        commitToTable(records, INSERT.value(), first, writeConfigs, extendedSchema.toString());
      } else {
        List<HoodieRecord> records = dataGen.generateUniqueUpdates(dataConfig.commitId, dataConfig.recordCount);
        List<Pair<String, IndexedRecord>> indexedRecords = hoodieRecordsToIndexedRecords(records, extendedSchema);
        allRecords = mergeIndexedRecordLists(indexedRecords, allRecords);
        commitToTable(records, UPSERT.value(), first, writeConfigs, extendedSchema.toString());
      }
      first = false;
    }

    @Override
    public void validate(int expectedLogFiles) throws Exception {
      validateOutputFromFileGroupReaderWithNativeRecords(
          getStorageConf(), getBasePath(),
          true, expectedLogFiles, RecordMergeMode.EVENT_TIME_ORDERING,
          allRecords, areExpectedRecordsInFinalSchema());
    }

    @Override
    public void close() throws Exception {
      dataGen.close();
      allRecords.clear();
    }

    // Abstract method for schema evolution - implementation varies by subclass
    @Override
    public void evolveSchema(int iteration) throws Exception {
      doEvolveSchema(iteration);
      dataGen.addExtendedSchema(extendedSchema);
    }

    public void execute() throws Exception {
      SchemaEvolutionTestUtilsBase.executeTest(this, scenario);
    }
  }

  // single use class for testing schema on read
  private class SchemaOnReadTestExecutor extends AbstractSchemaEvolutionTestExecutor<SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs> {
    private InternalSchema extendedInternalSchema;
    private String historySchema = "";

    public SchemaOnReadTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType,
                                    SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs configs,
                                    HoodieFileFormat baseFileFormat,
                                    Option<String> logFileFormat) {
      super(testType, configs, baseFileFormat, logFileFormat);
      writeConfigs.put(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
      // TODO fix schema evolution on col stats
      writeConfigs.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "false");
      writeConfigs.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    }

    public SchemaOnReadTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType,
                                    SchemaOnReadEvolutionTestUtils.SchemaOnReadConfigs configs,
                                    HoodieFileFormat baseFileFormat) {
      this(testType, configs, baseFileFormat, Option.empty());
    }

    @Override
    protected void initializeSchema() {
      updateSchemas(0);
    }

    @Override
    public void doEvolveSchema(int iteration) {
      updateSchemas(iteration);
      commitSchemaToTable(extendedInternalSchema, writeConfigs, historySchema);
      Map<String, String> renameCols = SchemaOnReadEvolutionTestUtils.generateColumnNameChanges(evolutionConfigs, iteration, maxIterations);
      allRecords = allRecords.stream()
          .map(r -> Pair.of(r.getLeft(),
              (IndexedRecord) HoodieAvroUtils.rewriteRecordWithNewSchema(r.getRight(), extendedSchema, renameCols, true)))
          .collect(Collectors.toList());
    }

    @Override
    protected boolean areExpectedRecordsInFinalSchema() {
      // in doEvolveSchema we update `allRecords` to match the table schema since we need to deal with name changes
      return true;
    }

    private void updateSchemas(int iteration) {
      // inherit from the previous schema
      if (extendedInternalSchema != null) {
        historySchema = SerDeHelper.inheritSchemas(extendedInternalSchema, historySchema);
      }
      extendedInternalSchema = SchemaOnReadEvolutionTestUtils.generateExtendedSchema(evolutionConfigs, iteration, maxIterations);
      extendedSchema = HoodieAvroUtils.removeMetadataFields(
          AvroInternalSchemaConverter.convert(extendedInternalSchema, evolutionConfigs.schema.getName()));
    }
  }

  // single use class for testing schema on write
  private class SchemaOnWriteTestExecutor extends AbstractSchemaEvolutionTestExecutor<SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs> {

    private boolean hasEvolvedSchema = false;

    public SchemaOnWriteTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType,
                                     SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs configs,
                                     HoodieFileFormat baseFileFormat) {
      this(testType, configs, baseFileFormat, Option.empty());
    }

    public SchemaOnWriteTestExecutor(SchemaEvolutionTestUtilsBase.SchemaEvolutionScenarioType testType,
                                     SchemaOnWriteEvolutionTestUtils.SchemaOnWriteConfigs configs,
                                     HoodieFileFormat baseFileFormat,
                                     Option<String> logFileFormat) {
      super(testType, configs, baseFileFormat, logFileFormat);
    }

    @Override
    protected void initializeSchema() {
      doEvolveSchema(0);
    }

    @Override
    public void doEvolveSchema(int iteration)  {
      if (iteration > 0) {
        hasEvolvedSchema = true;
      }
      extendedSchema = SchemaOnWriteEvolutionTestUtils.generateExtendedSchema(evolutionConfigs, iteration, maxIterations);
    }

    @Override
    protected boolean areExpectedRecordsInFinalSchema() {
      return !hasEvolvedSchema;
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
      commitToTable(dataGen.generateInserts("001", 100), INSERT.value(), true, writeConfigs);
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
            // generate field value is hardcoded as 0 for ordering field: timestamp, see HoodieTestDataGenerator#generateRandomValue
            assertEquals(readerContext.getRecordContext().convertValueToEngineType(0L), positionBased.getOrderingValue());
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

  // validate records involved in schema evolution
  private void validateOutputFromFileGroupReaderWithNativeRecords(StorageConfiguration<?> storageConf,
                                                                  String tablePath,
                                                                  boolean containsBaseFile,
                                                                  int expectedLogFileNum,
                                                                  RecordMergeMode recordMergeMode,
                                                                  List<Pair<String, IndexedRecord>> expectedRecords,
                                                                  boolean expectedHasCorrectSchema) throws Exception {
    Set<String> metaCols = new HashSet<>(HoodieRecord.HOODIE_META_COLUMNS);
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storageConf, tablePath);
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    Schema avroSchema = resolver.getTableAvroSchema();
    Schema avroSchemaWithoutMeta = resolver.getTableAvroSchema(false);
    Option<InternalSchema> internalSchemaOption = resolver.getTableInternalSchemaFromCommitMetadata();
    if (internalSchemaOption.isPresent()) {
      getStorageConf().set("hoodie.tablePath", getBasePath());
      InstantFileNameGenerator instantFileNameGenerator = metaClient.getTimelineLayout().getInstantFileNameGenerator();
      String validCommits = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants()
          .getInstants().stream().map(instantFileNameGenerator::getFileName).collect(Collectors.joining(","));
      getStorageConf().set("hoodie.valid.commits.list", validCommits);
    }
    // use reader context for conversion to engine specific objects
    HoodieReaderContext<T> readerContext = getHoodieReaderContext(tablePath, avroSchema, getStorageConf(), metaClient);
    List<FileSlice> fileSlices = getFileSlicesToRead(storageConf, tablePath, metaClient, containsBaseFile, expectedLogFileNum);
    boolean sortOutput = !containsBaseFile;
    List<T> actualRecordList =
        readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, internalSchemaOption, recordMergeMode, false, sortOutput);
    assertEquals(expectedRecords.size(), actualRecordList.size());
    actualRecordList.forEach(r -> assertRecordMatchesSchema(avroSchema, r));
    Set<GenericRecord> actualRecordSet = actualRecordList.stream().map(r ->  readerContext.getRecordContext().convertToAvroRecord(r, avroSchema))
        .map(r -> HoodieAvroUtils.removeFields(r, metaCols))
        .collect(Collectors.toSet());
    Set<GenericRecord> expectedRecordSet = expectedRecords.stream()
        .map(r -> (GenericRecord) r.getRight())
        .map(r -> {
          if (!expectedHasCorrectSchema) {
            return HoodieAvroUtils.rewriteRecordWithNewSchema(r, avroSchemaWithoutMeta);
          } else {
            return r;
          }
        }).collect(Collectors.toSet());
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
      AvroSchemaTestUtils.validateRecordsHaveSameData(expectedRecord, actualRecord);
    }
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
    expectedHoodieRecords = getExpectedHoodieRecordsWithOrderingValue(expectedHoodieRecords, metaClient, avroSchema);
    expectedHoodieUnmergedRecords = getExpectedHoodieRecordsWithOrderingValue(expectedHoodieUnmergedRecords, metaClient, avroSchema);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedRecords = convertHoodieRecords(expectedHoodieRecords, avroSchema);
    List<HoodieTestDataGenerator.RecordIdentifier> expectedUnmergedRecords = convertHoodieRecords(expectedHoodieUnmergedRecords, avroSchema);
    validateOutputFromFileGroupReaderWithExistingRecords(
        storageConf, tablePath, containsBaseFile, expectedLogFileNum, recordMergeMode,
        expectedRecords, expectedUnmergedRecords);
  }

  private static List<HoodieRecord> getExpectedHoodieRecordsWithOrderingValue(List<HoodieRecord> expectedHoodieRecords, HoodieTableMetaClient metaClient, Schema avroSchema) {
    return expectedHoodieRecords.stream().map(rec -> {
      RawTripTestPayload oldPayload = (RawTripTestPayload) rec.getData();
      try {
        List<String> orderingFields = metaClient.getTableConfig().getPreCombineFields();
        HoodieAvroRecord avroRecord = ((HoodieAvroRecord) rec);
        Comparable orderingValue = OrderingValues.create(orderingFields, field -> (Comparable) avroRecord.getColumnValueAsJava(avroSchema, field, new TypedProperties()));
        RawTripTestPayload newPayload = new RawTripTestPayload(Option.ofNullable(oldPayload.getJsonData()), oldPayload.getRowKey(), oldPayload.getPartitionPath(), null, false, orderingValue);
        return new HoodieAvroRecord<>(rec.getKey(), newPayload);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
        avroSchema, readerContext, metaClient.getTableConfig().getPreCombineFields());
    // validate size is equivalent to ensure no duplicates are returned
    assertEquals(expectedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedRecords), new HashSet<>(actualRecordList));
    // validate records can be read from file group as HoodieRecords
    actualRecordList = convertHoodieRecords(
        readHoodieRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode),
        avroSchema, readerContext, metaClient.getTableConfig().getPreCombineFields());
    assertEquals(expectedRecords.size(), actualRecordList.size());
    assertEquals(new HashSet<>(expectedRecords), new HashSet<>(actualRecordList));
    // validate unmerged records
    actualRecordList = convertEngineRecords(
        readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, recordMergeMode, true, false),
        avroSchema, readerContext, metaClient.getTableConfig().getPreCombineFields());
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
    return readRecordsFromFileGroup(storageConf, tablePath, metaClient, fileSlices, avroSchema, Option.empty(), recordMergeMode, isSkipMerge, sortOutput);
  }

  private List<T> readRecordsFromFileGroup(StorageConfiguration<?> storageConf,
                                           String tablePath,
                                           HoodieTableMetaClient metaClient,
                                           List<FileSlice> fileSlices,
                                           Schema avroSchema,
                                           Option<InternalSchema> internalSchemaOpt,
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
        assertThrows(IllegalArgumentException.class, () -> getHoodieFileGroupReader(storageConf, tablePath, metaClient, avroSchema, fileSlice, 1, props, sortOutput, internalSchemaOpt));
      }
      try (HoodieFileGroupReader<T> fileGroupReader = getHoodieFileGroupReader(storageConf, tablePath, metaClient, avroSchema, fileSlice, 0, props, sortOutput, internalSchemaOpt)) {
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
    return getHoodieFileGroupReader(storageConf, tablePath, metaClient, avroSchema, fileSlice, start, props, sortOutput, Option.empty());
  }

  private HoodieFileGroupReader<T> getHoodieFileGroupReader(StorageConfiguration<?> storageConf,
                                                            String tablePath,
                                                            HoodieTableMetaClient metaClient,
                                                            Schema avroSchema,
                                                            FileSlice fileSlice,
                                                            int start, TypedProperties props, boolean sortOutput,
                                                            Option<InternalSchema> internalSchemaOpt) {
    return HoodieFileGroupReader.<T>newBuilder()
        .withReaderContext(getHoodieReaderContext(tablePath, avroSchema, storageConf, metaClient))
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(metaClient.getActiveTimeline().lastInstant().get().requestedTime())
        .withFileSlice(fileSlice)
        .withDataSchema(avroSchema)
        .withRequestedSchema(avroSchema)
        .withInternalSchema(internalSchemaOpt)
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
    props.setProperty("hoodie.datasource.write.precombine.field", metaClient.getTableConfig().getPreCombineFieldsStr().orElse(""));
    props.setProperty("hoodie.payload.ordering.field", metaClient.getTableConfig().getPreCombineFieldsStr().orElse(""));
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

  private List<HoodieTestDataGenerator.RecordIdentifier> convertHoodieRecords(List<HoodieRecord> records, Schema schema) {
    return records.stream().map(record -> {
      RawTripTestPayload payload = (RawTripTestPayload) record.getData();
      return HoodieTestDataGenerator.RecordIdentifier.fromTripTestPayload(payload);
    }).collect(Collectors.toList());
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
                                                                              List<String> preCombineFields) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.precombine.field", String.join(",", preCombineFields));
    return records.stream()
        .map(record -> new HoodieTestDataGenerator.RecordIdentifier(
            record.getRecordKey(),
            removeHiveStylePartition(record.getPartitionPath()),
            record.getOrderingValue(schema, props, preCombineFields.toArray(new String[0])).toString(),
            readerContext.getRecordContext().getValue(record.getData(), schema, RIDER_FIELD_NAME).toString()))
        .collect(Collectors.toList());
  }

  private static String removeHiveStylePartition(String partitionPath) {
    int indexOf = partitionPath.indexOf("=");
    if (indexOf > 0) {
      return partitionPath.substring(indexOf + 1);
    }
    return partitionPath;
  }

}
