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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.DefaultSparkRecordMerger;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchema.TimePrecision;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.Type;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.common.schema.internal.utils.SchemaChangeUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.CustomKeyGenerator;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.ingestion.HoodieIngestionException;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.sources.TestParquetDFSSourceEmptyBatch;
import org.apache.hudi.utilities.sources.helpers.TestMercifulJsonToRowConverterBase;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.testutils.sources.AbstractBaseTestSource;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.DummyAvroPayload;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.NullValueSchemaProvider;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TestFileBasedSchemaProviderNullTargetSchema;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TestIdentityTransformer;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TripsWithEvolvedOptionalFieldTransformer;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.assertCheckpointVersion;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.prepareJsonKafkaDFSFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Schema and type coverage for {@link HoodieDeltaStreamer}: schema evolution, logical types and the repair of
 * corrupt logical-type metadata, payload classes, ordering fields and de-duplication.
 * Shared helpers and the transformer, key generator and payload classes these tests name live in
 * {@link TestHoodieDeltaStreamer}.
 */
@Slf4j
public class TestHoodieDeltaStreamerSchemaAndTypes extends HoodieDeltaStreamerTestBase {

  @AfterEach
  public void perTestAfterEach() {
    testNum++;
  }

  // Per-field verdict for the corrupt logical-repair fixtures: relabel ts_millis to millis and
  // attach the local-timestamp logical types that 0.x dropped. ts_micros is already micros.
  private static final String LOGICAL_REPAIR_TS_OVERRIDES =
      "ts_millis:timestamp-millis,local_ts_millis:local-timestamp-millis,local_ts_micros:local-timestamp-micros";

  /**
   * args for schema evolution test.
   *
   * @return
   */
  private static Stream<Arguments> schemaEvolArgs() {
    return Stream.of(
        Arguments.of(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL(), true, HoodieRecordType.AVRO),
        Arguments.of(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL(), false, HoodieRecordType.AVRO),
        Arguments.of(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL(), true, HoodieRecordType.AVRO),
        Arguments.of(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL(), false, HoodieRecordType.AVRO),

        Arguments.of(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL(), true, HoodieRecordType.SPARK),
        Arguments.of(DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL(), false, HoodieRecordType.SPARK),
        Arguments.of(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL(), true, HoodieRecordType.SPARK),
        Arguments.of(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL(), false, HoodieRecordType.SPARK));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  public void testPartitionKeyFieldsBasedOnVersion(HoodieTableVersion version) throws IOException {
    String tablePath = basePath + "/partition_key_fields_meta_client" + version.versionCode();
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tablePath, WriteOperationType.INSERT);
    cfg.configs.add(HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=" + version.versionCode());
    cfg.configs.add(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() + "=" + CustomKeyGenerator.class.getName());
    cfg.configs.add("hoodie.datasource.write.partitionpath.field=partition_path:simple");
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamer.getIngestionService().ingestOnce();
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(context, tablePath);
    String expectedPartitionFields = version.equals(HoodieTableVersion.SIX) ? "partition_path" : "partition_path:simple";
    assertEquals(expectedPartitionFields, metaClient.getTableConfig().getString(HoodieTableConfig.PARTITION_FIELDS));
    deltaStreamer.shutdownGracefully();
  }

  // TODO add tests w/ disabled reconciliation
  @ParameterizedTest
  @MethodSource("schemaEvolArgs")
  public void testSchemaEvolution(String tableType, boolean useUserProvidedSchema, HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/test_table_schema_evolution" + tableType + "_" + useUserProvidedSchema;
    defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
    // Insert data produced with Schema A, pass Schema A
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, Collections.singletonList(TestIdentityTransformer.class.getName()),
        PROPS_FILENAME_TEST_SOURCE, false, true, false, null, tableType);
    addRecordMerger(recordType, cfg.configs);
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    cfg.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + basePath + "/source.avsc");
    cfg.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + basePath + "/source.avsc");
    cfg.configs.add(DataSourceWriteOptions.RECONCILE_SCHEMA().key() + "=true");

    syncOnce(cfg);
    assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));

    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Upsert data produced with Schema B, pass Schema B
    cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, Collections.singletonList(TripsWithEvolvedOptionalFieldTransformer.class.getName()),
        PROPS_FILENAME_TEST_SOURCE, false, true, false, null, tableType);
    addRecordMerger(recordType, cfg.configs);
    cfg.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + basePath + "/source.avsc");
    cfg.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + basePath + "/source_evolved.avsc");
    cfg.configs.add(DataSourceWriteOptions.RECONCILE_SCHEMA().key() + "=true");
    syncOnce(cfg);
    assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
    // out of 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    assertRecordCount(1450, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1450, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    sqlContext.read().format("org.apache.hudi").load(tableBasePath).createOrReplaceTempView("tmp_trips");
    long recordCount =
        sqlContext.sparkSession().sql("select * from tmp_trips where evoluted_optional_union_field is not NULL").count();
    assertEquals(950, recordCount);

    // Upsert data produced with Schema A, pass Schema B
    if (!useUserProvidedSchema) {
      defaultSchemaProviderClassName = TestFileBasedSchemaProviderNullTargetSchema.class.getName();
    }
    cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, Collections.singletonList(TestIdentityTransformer.class.getName()),
        PROPS_FILENAME_TEST_SOURCE, false, true, false, null, tableType);
    addRecordMerger(recordType, cfg.configs);
    cfg.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + basePath + "/source.avsc");
    if (useUserProvidedSchema) {
      cfg.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + basePath + "/source_evolved.avsc");
    }
    cfg.configs.add(DataSourceWriteOptions.RECONCILE_SCHEMA().key() + "=true");
    syncOnce(cfg);
    // again, 1000 new records, 500 are inserts, 450 are updates and 50 are deletes.
    assertRecordCount(1900, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00002", tableBasePath, 3);
    counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1900, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(
        HoodieTestUtils.createMetaClient(storage, tableBasePath));
    HoodieSchema tableSchema = tableSchemaResolver.getTableSchema(false);
    assertNotNull(tableSchema);

    HoodieSchema expectedSchema = HoodieSchema.parse(fs.open(new Path(basePath + "/source_evolved.avsc")));
    assertEquals(expectedSchema, tableSchema);

    // clean up and reinit
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
    UtilitiesTestBase.Helpers.deleteFileFromDfs(
        HadoopFSUtils.getFs(cfg.targetBasePath, jsc.hadoopConfiguration()),
        basePath + "/" + PROPS_FILENAME_TEST_SOURCE);
    writeCommonPropsToFile(storage, basePath);
    defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
  }

  @Test
  public void testTimestampMillis() throws Exception {
    String tableBasePath = basePath + "/testTimestampMillis";
    defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
    // Insert data produced with Schema A, pass Schema A
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, Collections.singletonList(TestIdentityTransformer.class.getName()),
        PROPS_FILENAME_TEST_SOURCE, false, true, false, null, HoodieTableType.MERGE_ON_READ.name());
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    cfg.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + basePath + "/source-timestamp-millis.avsc");
    cfg.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + basePath + "/source-timestamp-millis.avsc");
    cfg.configs.add(String.format("%s=%s", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"));
    cfg.configs.add("hoodie.datasource.write.row.writer.enable=false");


    syncOnce(cfg);
    assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(
        HoodieTestUtils.createMetaClient(storage, tableBasePath));
    HoodieSchema tableSchema = tableSchemaResolver.getTableSchema(false);
    Option<HoodieSchemaField> currentTsFieldOpt = tableSchema.getField("current_ts");
    assertTrue(currentTsFieldOpt.isPresent());
    HoodieSchema.Timestamp currentTsSchema = (HoodieSchema.Timestamp) currentTsFieldOpt.get().schema();
    assertEquals(HoodieSchemaType.TIMESTAMP, currentTsSchema.getType());
    assertEquals(TimePrecision.MILLIS, currentTsSchema.getPrecision());
    assertEquals(1000, sqlContext.read().options(hudiOpts).format("org.apache.hudi").load(tableBasePath).filter("current_ts > '1980-01-01'").count());

    cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, Collections.singletonList(TestIdentityTransformer.class.getName()),
        PROPS_FILENAME_TEST_SOURCE, false, true, false, null, HoodieTableType.MERGE_ON_READ.name());
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    cfg.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + basePath + "/source-timestamp-millis.avsc");
    cfg.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + basePath + "/source-timestamp-millis.avsc");
    cfg.configs.add(String.format("%s=%s", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"));
    cfg.configs.add("hoodie.datasource.write.row.writer.enable=false");

    syncOnce(cfg);
    assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
    assertRecordCount(1450, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    tableSchemaResolver = new TableSchemaResolver(
        HoodieTestUtils.createMetaClient(storage, tableBasePath));
    tableSchema = tableSchemaResolver.getTableSchema(false);
    currentTsFieldOpt = tableSchema.getField("current_ts");
    assertTrue(currentTsFieldOpt.isPresent());
    currentTsSchema = (HoodieSchema.Timestamp) currentTsFieldOpt.get().schema();
    assertEquals(HoodieSchemaType.TIMESTAMP, currentTsSchema.getType());
    assertEquals(TimePrecision.MILLIS, currentTsSchema.getPrecision());
    sqlContext.clearCache();
    assertEquals(1450, sqlContext.read().options(hudiOpts).format("org.apache.hudi").load(tableBasePath).filter("current_ts > '1980-01-01'").count());
    assertEquals(1450, sqlContext.read().options(hudiOpts).format("org.apache.hudi").load(tableBasePath).filter("current_ts < '2080-01-01'").count());
    assertEquals(0, sqlContext.read().options(hudiOpts).format("org.apache.hudi").load(tableBasePath).filter("current_ts < '1980-01-01'").count());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testLongToTimestampPromotionGated(boolean setNullForMissingColumns) throws Exception {
    // Promoting a plain long column to a timestamp logical type is override-gated: rejected without a
    // per-field override for every target type, and applied with one. A bare long carries no precision
    // signal, so the override is the explicit verdict that authorizes the promotion. One bare-long seed
    // is reused: the rejection cases all throw (table stays bare long), and the accepted case runs last.
    String tableBasePath = basePath + "/testLongToTs" + setNullForMissingColumns;
    defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();

    // Sync 0: seed the table with `seconds_since_epoch` stored as a bare long.
    HoodieDeltaStreamer.Config seed = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT,
        Collections.singletonList(TestIdentityTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE,
        false, true, false, null, HoodieTableType.COPY_ON_WRITE.name());
    seed.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + basePath + "/source-timestamp-millis.avsc");
    seed.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + basePath + "/source-timestamp-millis.avsc");
    seed.configs.add("hoodie.datasource.write.row.writer.enable=false");
    seed.configs.add(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() + "=" + setNullForMissingColumns);
    new HoodieDeltaStreamer(seed, jsc).sync();

    Schema tableSchema = new TableSchemaResolver(HoodieTestUtils.createMetaClient(storage, tableBasePath))
        .getTableSchema(false).toAvroSchema();
    assertNull(tableSchema.getField("seconds_since_epoch").schema().getLogicalType(),
        "seconds_since_epoch must be seeded as a bare long in the table");
    Schema baseSchema = new Schema.Parser().parse(fs.open(new Path(basePath + "/source-timestamp-millis.avsc")));

    // Every target type is rejected without a per-field override.
    for (LogicalType targetType : new LogicalType[] {LogicalTypes.timestampMillis(), LogicalTypes.timestampMicros(),
        LogicalTypes.localTimestampMillis(), LogicalTypes.localTimestampMicros()}) {
      String schemaFile = writePromotedSchema(baseSchema, targetType, setNullForMissingColumns);
      HoodieDeltaStreamer.Config reject = promoteConfig(tableBasePath, schemaFile, setNullForMissingColumns, null);
      HoodieDeltaStreamer streamer = new HoodieDeltaStreamer(reject, jsc);
      // sync() wraps the guard's SchemaCompatibilityException in a HoodieIngestionException, so walk
      // the cause chain to assert on the underlying exception.
      Throwable thrown = assertThrows(Exception.class, streamer::sync,
          "long -> " + targetType.getName() + " must be rejected without an override");
      Throwable cause = thrown;
      while (cause != null && !(cause instanceof SchemaCompatibilityException)) {
        cause = cause.getCause();
      }
      assertTrue(cause instanceof SchemaCompatibilityException,
          "Expected a SchemaCompatibilityException in the cause chain, got: " + thrown);
      Type toType = SchemaChangeUtils.parseTimestampLogicalTypeOverrides("field:" + targetType.getName()).get("field");
      assertEquals(AvroSchemaEvolutionUtils.timestampPrecisionChangeError(
          "seconds_since_epoch", Types.LongType.get(), toType).getMessage(), cause.getMessage());
    }

    // With an override the promotion is authorized (local promotions are covered end-to-end by
    // testCOWLogicalRepair / testMORLogicalRepair); verify a UTC promotion succeeds and lands on the
    // table schema.
    String utcSchemaFile = writePromotedSchema(baseSchema, LogicalTypes.timestampMicros(), setNullForMissingColumns);
    HoodieDeltaStreamer.Config accept = promoteConfig(tableBasePath, utcSchemaFile, setNullForMissingColumns,
        "seconds_since_epoch:timestamp-micros");
    new HoodieDeltaStreamer(accept, jsc).sync();
    Schema evolved = new TableSchemaResolver(HoodieTestUtils.createMetaClient(storage, tableBasePath))
        .getTableSchema(false).toAvroSchema();
    assertEquals("timestamp-micros", evolved.getField("seconds_since_epoch").schema().getLogicalType().getName());
  }

  private String writePromotedSchema(Schema baseSchema, LogicalType targetType, boolean setNull) throws IOException {
    Schema incoming = replaceFieldType(baseSchema, "seconds_since_epoch",
        targetType.addToSchema(Schema.create(Schema.Type.LONG)));
    String schemaFile = basePath + "/promote-" + targetType.getName() + "-nul" + setNull + ".avsc";
    UtilitiesTestBase.Helpers.saveStringsToDFS(new String[] {incoming.toString()}, storage, schemaFile);
    return schemaFile;
  }

  private HoodieDeltaStreamer.Config promoteConfig(String tableBasePath, String schemaFile,
                                                   boolean setNullForMissingColumns, String override) {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        Collections.singletonList(TestIdentityTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE,
        false, true, false, null, HoodieTableType.COPY_ON_WRITE.name());
    cfg.configs.add("hoodie.streamer.schemaprovider.source.schema.file=" + schemaFile);
    cfg.configs.add("hoodie.streamer.schemaprovider.target.schema.file=" + schemaFile);
    cfg.configs.add("hoodie.datasource.write.row.writer.enable=false");
    cfg.configs.add(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key() + "=" + setNullForMissingColumns);
    if (override != null) {
      cfg.configs.add(HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key() + "=" + override);
    }
    return cfg;
  }

  private static Schema replaceFieldType(Schema recordSchema, String fieldName, Schema newFieldType) {
    List<Schema.Field> fields = new ArrayList<>();
    for (Schema.Field field : recordSchema.getFields()) {
      Schema fieldSchema = field.name().equals(fieldName) ? newFieldType : field.schema();
      fields.add(new Schema.Field(field.name(), fieldSchema, field.doc(), field.defaultVal()));
    }
    return Schema.createRecord(recordSchema.getName(), recordSchema.getDoc(), recordSchema.getNamespace(), false, fields);
  }

  @Test
  public void testLogicalTypes() throws Exception {
    try {
      String tableBasePath = basePath + "/testTimestampMillis";
      defaultSchemaProviderClassName = TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.class.getName();

      if (HoodieSparkUtils.isSpark3_3()) {
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.sourceSchema = HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS;
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.targetSchema = HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS;
        AbstractBaseTestSource.schemaStr = HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS;
      } else {
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.sourceSchema = HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA;
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.targetSchema = HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA;
        AbstractBaseTestSource.schemaStr = HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA;
      }

      // Insert data produced with Schema A, pass Schema A
      HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, Collections.singletonList(TestIdentityTransformer.class.getName()),
          PROPS_FILENAME_TEST_SOURCE, false, true, false, null, HoodieTableType.MERGE_ON_READ.name());
      cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
      cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
      cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
      cfg.configs.add(String.format("%s=%s", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"));
      cfg.configs.add("hoodie.datasource.write.row.writer.enable=false");

      syncOnce(cfg);
      assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
      assertRecordCount(1000, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);
      TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(
          HoodieTestUtils.createMetaClient(storage, tableBasePath));
      HoodieSchema tableSchema = tableSchemaResolver.getTableSchema(false);
      Map<String, String> hudiOpts = new HashMap<>();
      hudiOpts.put("hoodie.datasource.write.recordkey.field", "id");
      logicalAssertions(tableSchema, tableBasePath, hudiOpts, HoodieTableVersion.current().versionCode());


      cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, Collections.singletonList(TestIdentityTransformer.class.getName()),
          PROPS_FILENAME_TEST_SOURCE, false, true, false, null, HoodieTableType.MERGE_ON_READ.name());
      cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
      cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
      cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
      cfg.configs.add(String.format("%s=%s", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"));
      cfg.configs.add("hoodie.datasource.write.row.writer.enable=false");

      syncOnce(cfg);
      assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
      assertRecordCount(1450, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
      tableSchemaResolver = new TableSchemaResolver(
          HoodieTestUtils.createMetaClient(storage, tableBasePath));
      tableSchema = tableSchemaResolver.getTableSchema(false);
      logicalAssertions(tableSchema, tableBasePath, hudiOpts, HoodieTableVersion.current().versionCode());
    } finally {
      defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
      AbstractBaseTestSource.schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
    }
  }

  /**
   * Arguments for testLogicalTypesWithJsonSource.
   * Parameters: hasTransformer, orderingField, recordType, tableType
   */
  private static Stream<Arguments> logicalTypesWithJsonSourceArgs() {
    return Stream.of(
        // Test with timestamp ordering field (long type)
        Arguments.of(true, "timestamp", HoodieRecordType.AVRO, HoodieTableType.MERGE_ON_READ),
        Arguments.of(false, "timestamp", HoodieRecordType.AVRO, HoodieTableType.MERGE_ON_READ),
        Arguments.of(true, "timestamp", HoodieRecordType.SPARK, HoodieTableType.MERGE_ON_READ),
        Arguments.of(false, "timestamp", HoodieRecordType.SPARK, HoodieTableType.MERGE_ON_READ),
        Arguments.of(true, "timestamp", HoodieRecordType.AVRO, HoodieTableType.COPY_ON_WRITE),
        Arguments.of(false, "timestamp", HoodieRecordType.AVRO, HoodieTableType.COPY_ON_WRITE),
        Arguments.of(true, "timestamp", HoodieRecordType.SPARK, HoodieTableType.COPY_ON_WRITE),
        Arguments.of(false, "timestamp", HoodieRecordType.SPARK, HoodieTableType.COPY_ON_WRITE),
        // Test with rider ordering field (string type)
        Arguments.of(true, "rider", HoodieRecordType.AVRO, HoodieTableType.MERGE_ON_READ),
        Arguments.of(false, "rider", HoodieRecordType.AVRO, HoodieTableType.MERGE_ON_READ),
        Arguments.of(true, "rider", HoodieRecordType.SPARK, HoodieTableType.MERGE_ON_READ),
        Arguments.of(false, "rider", HoodieRecordType.SPARK, HoodieTableType.MERGE_ON_READ),
        Arguments.of(true, "rider", HoodieRecordType.AVRO, HoodieTableType.COPY_ON_WRITE),
        Arguments.of(false, "rider", HoodieRecordType.AVRO, HoodieTableType.COPY_ON_WRITE),
        Arguments.of(true, "rider", HoodieRecordType.SPARK, HoodieTableType.COPY_ON_WRITE),
        Arguments.of(false, "rider", HoodieRecordType.SPARK, HoodieTableType.COPY_ON_WRITE));
  }

  @ParameterizedTest
  @MethodSource("logicalTypesWithJsonSourceArgs")
  void testLogicalTypesWithJsonSource(boolean hasTransformer, String orderingField,
                                      HoodieRecordType recordType, HoodieTableType tableType) throws Exception {
    // Fix a seed so we can generate repeated rows for updates
    final long seed = 123L;
    final int numPartitions = 2;

    try {
      //use v6 schema because decimal parsing iso 8859-1 support not available currently
      String schemaStr;
      if (HoodieSparkUtils.isSpark3_3()) {
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.sourceSchema =
            HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS_V6;
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.targetSchema =
            HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS_V6;
        schemaStr = HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA_NO_LTS_V6;
      } else {
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.sourceSchema =
            HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA_V6;
        TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.targetSchema =
            HoodieTestDataGenerator.HOODIE_SCHEMA_TRIP_LOGICAL_TYPES_SCHEMA_V6;
        schemaStr = HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA_V6;
      }
      defaultSchemaProviderClassName =
          TestHoodieDeltaStreamerSchemaEvolutionBase.TestSchemaProvider.class.getName();
      String tableBasePath = basePath + "testTimestampMillis_" + orderingField + "_" + recordType + "_" + tableType.name();
      prepareJsonKafkaDFSSource(
          PROPS_FILENAME_TEST_JSON_KAFKA, "earliest", topicName);

      // Insert data produced with Schema A, pass Schema A
      prepareJsonKafkaDFSFiles(1000, true, topicName, numPartitions, schemaStr, seed);
      HoodieDeltaStreamer.Config cfg = getConfigForLogicalTypesWithJsonSource(
          tableBasePath, WriteOperationType.INSERT, hasTransformer, orderingField, recordType, tableType);
      syncOnce(cfg);
      // Validate.
      assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
      assertRecordCount(1000, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata(topicName + ",0:500,1:500", tableBasePath, 1);
      TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(
          HoodieTestUtils.createMetaClient(storage, tableBasePath));
      HoodieSchema tableSchema = tableSchemaResolver.getTableSchema(false);
      Map<String, String> hudiOpts = new HashMap<>();
      hudiOpts.put("hoodie.datasource.write.recordkey.field", "id");
      logicalAssertions(tableSchema, tableBasePath, hudiOpts, HoodieTableVersion.EIGHT.versionCode());

      // Update data, since we are using the same seed, 1000 rows will be updated, and 500 new rows will be inserted
      prepareJsonKafkaDFSFiles(1500, false, topicName, numPartitions, schemaStr, seed);
      cfg = getConfigForLogicalTypesWithJsonSource(
          tableBasePath, WriteOperationType.UPSERT, hasTransformer, orderingField, recordType, tableType);
      syncOnce(cfg);
      // Validate.
      assertCheckpointVersion(HoodieTestUtils.createMetaClient(storage, tableBasePath));
      assertRecordCount(1500, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata(topicName + ",0:1250,1:1250", tableBasePath, 2);
      tableSchemaResolver = new TableSchemaResolver(
          HoodieTestUtils.createMetaClient(storage, tableBasePath));
      tableSchema = tableSchemaResolver.getTableSchema(false);
      logicalAssertions(tableSchema, tableBasePath, hudiOpts, HoodieTableVersion.EIGHT.versionCode());
    } finally {
      defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
    }
  }

  private static HoodieDeltaStreamer.Config getConfigForLogicalTypesWithJsonSource(
      String tableBasePath, WriteOperationType operationType, boolean hasTransformer,
      String orderingField, HoodieRecordType recordType, HoodieTableType tableType) {
    List<String> transformerClassNames;
    if (hasTransformer) {
      transformerClassNames = Collections.singletonList(TestIdentityTransformer.class.getName());
    } else {
      transformerClassNames = Collections.emptyList();
    }
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(
        tableBasePath, operationType, JsonKafkaSource.class.getName(),
        transformerClassNames,
        PROPS_FILENAME_TEST_JSON_KAFKA,
        false, true, 100000, false, null,
        tableType.name(),
        orderingField, null);
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    cfg.configs.add(String.format("%s=%s", HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), "0"));
    cfg.configs.add("hoodie.datasource.write.row.writer.enable=false");
    cfg.configs.add("hoodie.streamer.source.sanitize.invalid.schema.field.names=true");
    // Configure record merger based on record type
    if (recordType == HoodieRecordType.SPARK) {
      cfg.configs.add(String.format("%s=%s", HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(),
          DefaultSparkRecordMerger.class.getName()));
      cfg.configs.add(String.format("%s=%s", HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "parquet"));
    } else {
      cfg.configs.add(String.format("%s=%s", HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(),
          HoodieAvroRecordMerger.class.getName()));
      cfg.configs.add(String.format("%s=%s", HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), "avro"));
    }
    return cfg;
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  public void testBackwardsCompatibility(HoodieTableVersion version) throws Exception {
    TestMercifulJsonToRowConverterBase.timestampNTZCompatibility(() -> {
      String dirName = "colstats-upgrade-test-v" + version.versionCode();
      String dataPath = basePath + "/" + dirName;
      java.nio.file.Path zipOutput = Paths.get(new URI(dataPath));
      HoodieTestUtils.extractZipToDirectory("col-stats/" + dirName + ".zip", zipOutput, getClass());
      String tableBasePath = zipOutput.resolve("trips_logical_types_json").toString();

      TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(
          HoodieTestUtils.createMetaClient(storage, tableBasePath));
      HoodieSchema tableSchema = tableSchemaResolver.getTableSchema(false);
      Map<String, String> hudiOpts = new HashMap<>();
      hudiOpts.put("hoodie.datasource.write.recordkey.field", "id");
      logicalAssertions(tableSchema, tableBasePath, hudiOpts, version.versionCode());

      HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, Collections.emptyList(),
          "placeholder", false, true, false, null, HoodieTableType.MERGE_ON_READ.name());
      cfg.propsFilePath = zipOutput + "/hudi.properties";
      cfg.schemaProviderClassName = "org.apache.hudi.utilities.schema.FilebasedSchemaProvider";
      cfg.sourceOrderingFields = "timestamp";
      cfg.sourceClassName = "org.apache.hudi.utilities.sources.JsonDFSSource";
      cfg.targetTableName = "trips_logical_types_json";
      cfg.configs.add("hoodie.streamer.source.dfs.root=" + zipOutput + "/data/data_6/");
      cfg.configs.add(String.format(("%s=%s"), HoodieWriteConfig.WRITE_TABLE_VERSION.key(), version.versionCode()));
      cfg.configs.add(String.format(("%s=%s"), HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "100"));
      String schemaPath = zipOutput + "/schema.avsc";
      cfg.configs.add(String.format(("%s=%s"), "hoodie.streamer.schemaprovider.source.schema.file", schemaPath));
      cfg.configs.add(String.format(("%s=%s"), "hoodie.streamer.schemaprovider.target.schema.file", schemaPath));
      // The v6/v8 col-stats fixture reuses the same trips_logical_types_json corrupt schema as
      // the logical-repair tests — 0.x collapsed ts_millis to timestamp-micros and dropped the
      // local-timestamp logical types entirely. Provide the same explicit verdict so the guard
      // in HoodieSchemaUtils.deduceWriterSchema authorizes the repair rather than rejecting the
      // unverified precision change.
      cfg.configs.add(String.format(("%s=%s"),
          HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key(), LOGICAL_REPAIR_TS_OVERRIDES));
      cfg.forceDisableCompaction = true;
      cfg.sourceLimit = 100_000;
      cfg.ignoreCheckpoint = "12345";
      syncOnce(cfg);
      logicalAssertions(tableSchema, tableBasePath, hudiOpts, version.versionCode());
    });
  }

  private void logicalAssertions(HoodieSchema tableSchema, String tableBasePath, Map<String, String> hudiOpts, int tableVersion) {
    if (tableVersion > 8) {
      Option<HoodieSchemaField> tsMillisFieldOpt = tableSchema.getField("ts_millis");
      assertTrue(tsMillisFieldOpt.isPresent());
      HoodieSchema.Timestamp tsMillisFieldSchema = (HoodieSchema.Timestamp) tsMillisFieldOpt.get().schema();
      assertEquals(HoodieSchemaType.TIMESTAMP, tsMillisFieldSchema.getType());
      assertEquals(TimePrecision.MILLIS, tsMillisFieldSchema.getPrecision());
      assertTrue(tsMillisFieldSchema.isUtcAdjusted());
    }
    Option<HoodieSchemaField> tsMicrosFieldOpt = tableSchema.getField("ts_micros");
    assertTrue(tsMicrosFieldOpt.isPresent());
    HoodieSchema.Timestamp tsMicrosFieldSchema = (HoodieSchema.Timestamp) tsMicrosFieldOpt.get().schema();
    assertEquals(HoodieSchemaType.TIMESTAMP, tsMicrosFieldSchema.getType());
    assertEquals(TimePrecision.MICROS, tsMicrosFieldSchema.getPrecision());
    assertTrue(tsMicrosFieldSchema.isUtcAdjusted());
    if (tableVersion > 8 && !HoodieSparkUtils.isSpark3_3()) {
      Option<HoodieSchemaField> localTsMillisFieldOpt = tableSchema.getField("local_ts_millis");
      assertTrue(localTsMillisFieldOpt.isPresent());
      HoodieSchema.Timestamp localTsMillisFieldSchema = (HoodieSchema.Timestamp) localTsMillisFieldOpt.get().schema();
      assertEquals(HoodieSchemaType.TIMESTAMP, localTsMillisFieldSchema.getType());
      assertEquals(TimePrecision.MILLIS, localTsMillisFieldSchema.getPrecision());
      assertFalse(localTsMillisFieldSchema.isUtcAdjusted());

      Option<HoodieSchemaField> localTsMicrosFieldOpt = tableSchema.getField("local_ts_micros");
      assertTrue(localTsMicrosFieldOpt.isPresent());
      HoodieSchema.Timestamp localTsMicrosFieldSchema = (HoodieSchema.Timestamp) localTsMicrosFieldOpt.get().schema();
      assertEquals(HoodieSchemaType.TIMESTAMP, localTsMicrosFieldSchema.getType());
      assertEquals(TimePrecision.MICROS, localTsMicrosFieldSchema.getPrecision());
      assertFalse(localTsMicrosFieldSchema.isUtcAdjusted());
    }
    Option<HoodieSchemaField> eventDateFieldOpt = tableSchema.getField("event_date");
    assertTrue(eventDateFieldOpt.isPresent());
    assertEquals(HoodieSchemaType.DATE, eventDateFieldOpt.get().schema().getType());

    if (tableVersion > 8) {
      Option<HoodieSchemaField> decPlainLargeFieldOpt = tableSchema.getField("dec_plain_large");
      assertTrue(decPlainLargeFieldOpt.isPresent());
      HoodieSchema.Decimal decPlainLargeSchema = (HoodieSchema.Decimal) decPlainLargeFieldOpt.get().schema();
      // decimal backed by bytes (are not fixed length byte arrays)
      assertFalse(decPlainLargeSchema.isFixed());
      assertEquals(HoodieSchemaType.DECIMAL, decPlainLargeSchema.getType());
      assertEquals(20, decPlainLargeSchema.getPrecision());
      assertEquals(10, decPlainLargeSchema.getScale());
    }
    Option<HoodieSchemaField> decFixedSmallOpt = tableSchema.getField("dec_fixed_small");
    assertTrue(decFixedSmallOpt.isPresent());
    HoodieSchema.Decimal decFixedSmallSchema = (HoodieSchema.Decimal) decFixedSmallOpt.get().schema();
    assertTrue(decFixedSmallSchema.isFixed());
    assertEquals(3, decFixedSmallSchema.getFixedSize());
    assertEquals(HoodieSchemaType.DECIMAL, decFixedSmallSchema.getType());
    assertEquals(5, decFixedSmallSchema.getPrecision());
    assertEquals(2, decFixedSmallSchema.getScale());

    Option<HoodieSchemaField> decFixedLargeOpt = tableSchema.getField("dec_fixed_large");
    assertTrue(decFixedLargeOpt.isPresent());
    HoodieSchema.Decimal decFixedLargeSchema = (HoodieSchema.Decimal) decFixedLargeOpt.get().schema();
    assertTrue(decFixedLargeSchema.isFixed());
    assertEquals(8, decFixedLargeSchema.getFixedSize());
    assertEquals(HoodieSchemaType.DECIMAL, decFixedLargeSchema.getType());
    assertEquals(18, decFixedLargeSchema.getPrecision());
    assertEquals(9, decFixedLargeSchema.getScale());

    sqlContext.clearCache();
    Dataset<Row> df = sqlContext.read()
        .options(hudiOpts)
        .format("org.apache.hudi")
        .load(tableBasePath);

    long totalCount = df.count();
    long expectedHalf = totalCount / 2;
    long tolerance = totalCount / 20;
    if (totalCount < 100) {
      tolerance = totalCount / 4;
    }

    if (tableVersion > 8) {
      assertHalfSplit(df, "ts_millis > timestamp('2020-01-01 00:00:00Z')", expectedHalf, tolerance, "ts_millis > threshold");
      assertHalfSplit(df, "ts_millis < timestamp('2020-01-01 00:00:00Z')", expectedHalf, tolerance, "ts_millis < threshold");
      assertBoundaryCounts(df, "ts_millis > timestamp('2020-01-01 00:00:00.001Z')", "ts_millis <= timestamp('2020-01-01 00:00:00.001Z')", totalCount);
      assertBoundaryCounts(df, "ts_millis < timestamp('2019-12-31 23:59:59.999Z')", "ts_millis >= timestamp('2019-12-31 23:59:59.999Z')", totalCount);
    }

    assertHalfSplit(df, "ts_micros > timestamp('2020-06-01 12:00:00Z')", expectedHalf, tolerance, "ts_micros > threshold");
    assertHalfSplit(df, "ts_micros < timestamp('2020-06-01 12:00:00Z')", expectedHalf, tolerance, "ts_micros < threshold");
    assertBoundaryCounts(df, "ts_micros > timestamp('2020-06-01 12:00:00.000001Z')", "ts_micros <= timestamp('2020-06-01 12:00:00.000001Z')", totalCount);
    assertBoundaryCounts(df, "ts_micros < timestamp('2020-06-01 11:59:59.999999Z')", "ts_micros >= timestamp('2020-06-01 11:59:59.999999Z')", totalCount);

    if (tableVersion > 8 && !HoodieSparkUtils.isSpark3_3()) {
      assertHalfSplit(df, "local_ts_millis > CAST('2015-05-20 12:34:56' AS TIMESTAMP_NTZ)", expectedHalf, tolerance, "local_ts_millis > threshold");
      assertHalfSplit(df, "local_ts_millis < CAST('2015-05-20 12:34:56' AS TIMESTAMP_NTZ)", expectedHalf, tolerance, "local_ts_millis < threshold");
      assertBoundaryCounts(df, "local_ts_millis > CAST('2015-05-20 12:34:56.001' AS TIMESTAMP_NTZ)", "local_ts_millis <= CAST('2015-05-20 12:34:56.001' AS TIMESTAMP_NTZ)", totalCount);
      assertBoundaryCounts(df, "local_ts_millis < CAST('2015-05-20 12:34:55.999' AS TIMESTAMP_NTZ)", "local_ts_millis >= CAST('2015-05-20 12:34:55.999' AS TIMESTAMP_NTZ)", totalCount);

      assertHalfSplit(df, "local_ts_micros > CAST('2017-07-07 07:07:07' AS TIMESTAMP_NTZ)", expectedHalf, tolerance, "local_ts_micros > threshold");
      assertHalfSplit(df, "local_ts_micros < CAST('2017-07-07 07:07:07' AS TIMESTAMP_NTZ)", expectedHalf, tolerance, "local_ts_micros < threshold");
      assertBoundaryCounts(df, "local_ts_micros > CAST('2017-07-07 07:07:07.000001' AS TIMESTAMP_NTZ)", "local_ts_micros <= CAST('2017-07-07 07:07:07.000001' AS TIMESTAMP_NTZ)", totalCount);
      assertBoundaryCounts(df, "local_ts_micros < CAST('2017-07-07 07:07:06.999999' AS TIMESTAMP_NTZ)", "local_ts_micros >= CAST('2017-07-07 07:07:06.999999' AS TIMESTAMP_NTZ)", totalCount);

    }

    assertHalfSplit(df, "event_date > date('2000-01-01')", expectedHalf, tolerance, "event_date > threshold");
    assertHalfSplit(df, "event_date < date('2000-01-01')", expectedHalf, tolerance, "event_date < threshold");
    assertBoundaryCounts(df, "event_date > date('2000-01-02')", "event_date <= date('2000-01-02')", totalCount);
    assertBoundaryCounts(df, "event_date < date('1999-12-31')", "event_date >= date('1999-12-31')", totalCount);

    if (tableVersion > 8) {
      assertHalfSplit(df, "dec_plain_large < 1234567890.0987654321", expectedHalf, tolerance, "dec_plain_large < threshold");
      assertHalfSplit(df, "dec_plain_large > 1234567890.0987654321", expectedHalf, tolerance, "dec_plain_large > threshold");
      assertBoundaryCounts(df, "dec_plain_large < 1234567890.0987654320", "dec_plain_large >= 1234567890.0987654320", totalCount);
      assertBoundaryCounts(df, "dec_plain_large > 1234567890.0987654322", "dec_plain_large <= 1234567890.0987654322", totalCount);
    }

    assertHalfSplit(df, "dec_fixed_small < 543.21", expectedHalf, tolerance, "dec_fixed_small < threshold");
    assertHalfSplit(df, "dec_fixed_small > 543.21", expectedHalf, tolerance, "dec_fixed_small > threshold");
    assertBoundaryCounts(df, "dec_fixed_small < 543.20", "dec_fixed_small >= 543.20", totalCount);
    assertBoundaryCounts(df, "dec_fixed_small > 543.22", "dec_fixed_small <= 543.22", totalCount);

    assertHalfSplit(df, "dec_fixed_large < 987654321.123456789", expectedHalf, tolerance, "dec_fixed_large < threshold");
    assertHalfSplit(df, "dec_fixed_large > 987654321.123456789", expectedHalf, tolerance, "dec_fixed_large > threshold");
    assertBoundaryCounts(df, "dec_fixed_large < 987654321.123456788", "dec_fixed_large >= 987654321.123456788", totalCount);
    assertBoundaryCounts(df, "dec_fixed_large > 987654321.123456790", "dec_fixed_large <= 987654321.123456790", totalCount);
  }

  private void assertHalfSplit(Dataset<Row> df, String filterExpr, long expectedHalf, long tolerance, String msg) {
    long count = df.filter(filterExpr).count();
    assertTrue(Math.abs(count - expectedHalf) <= tolerance, msg + " (got=" + count + ", expected=" + expectedHalf + ")");
  }

  private void assertBoundaryCounts(Dataset<Row> df, String exprZero, String exprTotal, long totalCount) {
    assertEquals(0, df.filter(exprZero).count(), exprZero);
    assertEquals(totalCount, df.filter(exprTotal).count(), exprTotal);
  }

  @ParameterizedTest
  @CsvSource(value = {
      // Repair succeeds when a per-field verdict is set, on the default (non-reconcile) write path...
      "SIX,AVRO,CLUSTER,false,true", "EIGHT,AVRO,CLUSTER,false,true",
      "CURRENT,AVRO,NONE,false,true", "CURRENT,AVRO,CLUSTER,false,true",
      "CURRENT,SPARK,NONE,false,true", "CURRENT,SPARK,CLUSTER,false,true",
      // ...and on the reconcile path (setNullForMissingColumns=true).
      "SIX,AVRO,CLUSTER,true,true", "EIGHT,AVRO,CLUSTER,true,true", "CURRENT,AVRO,CLUSTER,true,true",
      // Guard: with no verdict, the mislabeled timestamp/local-timestamp columns must be rejected on
      // the first sync, on both the reconcile path and the default path.
      "SIX,AVRO,CLUSTER,true,false", "SIX,AVRO,CLUSTER,false,false"})
  void testCOWLogicalRepair(String tableVersion, String recordType, String operation,
                            boolean setNullForMissingColumns,
                            boolean setTimestampOverride) throws Exception {
    TestMercifulJsonToRowConverterBase.timestampNTZCompatibility(() -> {
      String dirName = "trips_logical_types_json_cow_write";
      String dataPath = basePath + "/" + dirName;
      java.nio.file.Path zipOutput = Paths.get(new URI(dataPath));
      HoodieTestUtils.extractZipToDirectory("logical-repair/" + dirName + ".zip", zipOutput, getClass());
      String tableBasePath = zipOutput.toString();

      TypedProperties properties = new TypedProperties();
      String schemaPath = getClass().getClassLoader().getResource("logical-repair/schema.avsc").toURI().toString();
      properties.setProperty("hoodie.streamer.schemaprovider.source.schema.file", schemaPath);
      properties.setProperty("hoodie.streamer.schemaprovider.target.schema.file", schemaPath);
      String inputDataPath = getClass().getClassLoader().getResource("logical-repair/cow_write_updates/2").toURI().toString();
      properties.setProperty("hoodie.streamer.source.dfs.root", inputDataPath);

      String mergerClass = getMergerClassForRecordType(recordType);
      String tableVersionString = getTableVersionCode(tableVersion);

      properties.setProperty(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), mergerClass);
      properties.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
      properties.setProperty("hoodie.datasource.write.precombine.field", "timestamp");
      properties.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
      properties.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
      properties.setProperty("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS");
      properties.setProperty("hoodie.compact.inline", "false");
      properties.setProperty("hoodie.metatata.enable", "true");
      properties.setProperty("hoodie.parquet.small.file.limit", "-1");
      properties.setProperty("hoodie.cleaner.commits.retained", "10");
      properties.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersionString);
      properties.setProperty(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(),
          Boolean.toString(setNullForMissingColumns));
      if (setTimestampOverride) {
        // Per-field verdict authorizing the repair: relabel ts_millis to millis and attach the
        // local-timestamp logical types 0.x dropped. ts_micros stays micros (no entry needed).
        properties.setProperty(HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key(),
            LOGICAL_REPAIR_TS_OVERRIDES);
      }

      Option<TypedProperties> propt = Option.of(properties);

      if (!setTimestampOverride) {
        // No per-field verdict. The mislabeled timestamp/local-timestamp columns must be rejected on
        // the first sync rather than silently flipped, on both the reconcile and default write paths.
        // syncOnce wraps the guard's SchemaCompatibilityException in a HoodieIngestionException, so
        // walk the cause chain to assert on the underlying exception.
        Throwable thrown = assertThrows(Exception.class,
            () -> syncOnce(prepCfgForCowLogicalRepair(tableBasePath, "456"), propt));
        Throwable cause = thrown;
        while (cause != null && !(cause instanceof SchemaCompatibilityException)) {
          cause = cause.getCause();
        }
        assertTrue(cause instanceof SchemaCompatibilityException,
            "Expected a SchemaCompatibilityException in the cause chain, got: " + thrown);
        assertTrue(cause.getMessage().contains("column 'ts_millis'")
                && cause.getMessage().contains("without an explicit"),
            "Unexpected message: " + cause.getMessage());
        return;
      }

      syncOnce(prepCfgForCowLogicalRepair(tableBasePath, "456"), propt);

      inputDataPath = getClass().getClassLoader().getResource("logical-repair/cow_write_updates/3").toURI().toString();
      propt.get().setProperty("hoodie.streamer.source.dfs.root", inputDataPath);
      if ("CLUSTER".equals(operation)) {
        propt.get().setProperty("hoodie.clustering.inline", "true");
        propt.get().setProperty("hoodie.clustering.inline.max.commits", "1");
        propt.get().setProperty("hoodie.clustering.plan.strategy.single.group.clustering.enabled", "true");
        propt.get().setProperty("hoodie.clustering.plan.strategy.sort.columns", "ts_millis,_row_key");
      }
      syncOnce(prepCfgForCowLogicalRepair(tableBasePath, "789"), propt);

      String prevTimezone = sparkSession.conf().get("spark.sql.session.timeZone");
      try {
        sparkSession.conf().set("spark.sql.session.timeZone", "UTC");
        Dataset<Row> df = sparkSession.read().format("hudi").load(tableBasePath);

        assertDataframe(df, 15, 15);

        if ("CLUSTER".equals(operation)) {
          // after we cluster, the raw parquet should be correct

          // Validate raw parquet files
          HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
              .setConf(storage.getConf())
              .setBasePath(tableBasePath)
              .build();

          HoodieTimeline completedCommitsTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
          Option<HoodieInstant> latestInstant = completedCommitsTimeline.lastInstant();
          assertTrue(latestInstant.isPresent(), "No completed commits found");

          List<String> baseFilePaths = collectLatestBaseFilePaths(metaClient);

          assertEquals(4, baseFilePaths.size());

          // Read raw parquet files
          Dataset<Row> rawParquetDf = sparkSession.read().parquet(baseFilePaths.toArray(new String[0]));
          assertDataframe(rawParquetDf, 15, 15);
        }
      } finally {
        sparkSession.conf().set("spark.sql.session.timeZone", prevTimezone);
      }
    });
  }

  @ParameterizedTest
  @CsvSource(value = {"SIX,AVRO,CLUSTER,AVRO,false,true", "EIGHT,AVRO,CLUSTER,AVRO,false,true",
      "CURRENT,AVRO,NONE,AVRO,false,true", "CURRENT,AVRO,CLUSTER,AVRO,false,true", "CURRENT,AVRO,COMPACT,AVRO,false,true",
      "CURRENT,AVRO,NONE,PARQUET,false,true", "CURRENT,AVRO,CLUSTER,PARQUET,false,true", "CURRENT,AVRO,COMPACT,PARQUET,false,true",
      "CURRENT,SPARK,NONE,PARQUET,false,true", "CURRENT,SPARK,CLUSTER,PARQUET,false,true", "CURRENT,SPARK,COMPACT,PARQUET,false,true",
      // Variants that exercise the schema-reconcile path (setNullForMissingColumns=true) with a verdict.
      "SIX,AVRO,CLUSTER,AVRO,true,true", "EIGHT,AVRO,CLUSTER,AVRO,true,true", "CURRENT,AVRO,CLUSTER,AVRO,true,true",
      // Guard: with no verdict, the first sync must throw, on both the reconcile and default paths.
      "SIX,AVRO,CLUSTER,AVRO,true,false", "SIX,AVRO,CLUSTER,AVRO,false,false"})
  void testMORLogicalRepair(String tableVersion, String recordType, String operation, String logBlockType,
                            boolean setNullForMissingColumns,
                            boolean setTimestampOverride) throws Exception {
    TestMercifulJsonToRowConverterBase.timestampNTZCompatibility(() -> {
      String tableSuffix;
      String logFormatValue;
      if ("AVRO".equals(logBlockType)) {
        logFormatValue = "avro";
        tableSuffix = "avro_log";
      } else {
        logFormatValue = "parquet";
        tableSuffix = "parquet_log";
      }

      String dirName = "trips_logical_types_json_mor_write_" + tableSuffix;
      String dataPath = basePath + "/" + dirName;
      java.nio.file.Path zipOutput = Paths.get(new URI(dataPath));
      HoodieTestUtils.extractZipToDirectory("logical-repair/" + dirName + ".zip", zipOutput, getClass());
      String tableBasePath = zipOutput.toString();

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(storage.getConf())
          .setBasePath(tableBasePath)
          .build();

      // validate no compaction and clustering instants present in the timeline
      HoodieTimeline completedTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
      assertFalse(completedTimeline.getInstants().stream().anyMatch(i -> i.getAction().equals(HoodieTimeline.COMPACTION_ACTION)));
      assertFalse(completedTimeline.getInstants().stream().anyMatch(i -> i.getAction().equals(HoodieTimeline.CLUSTERING_ACTION)));

      TypedProperties properties = new TypedProperties();
      String schemaPath = getClass().getClassLoader().getResource("logical-repair/schema.avsc").toURI().toString();
      properties.setProperty("hoodie.streamer.schemaprovider.source.schema.file", schemaPath);
      properties.setProperty("hoodie.streamer.schemaprovider.target.schema.file", schemaPath);
      String inputDataPath = getClass().getClassLoader().getResource("logical-repair/mor_write_updates/5").toURI().toString();
      properties.setProperty("hoodie.streamer.source.dfs.root", inputDataPath);
      String mergerClass = getMergerClassForRecordType(recordType);
      String tableVersionString = getTableVersionCode(tableVersion);

      properties.setProperty(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), mergerClass);
      properties.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
      properties.setProperty("hoodie.datasource.write.precombine.field", "timestamp");
      properties.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
      properties.setProperty("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
      properties.setProperty("hoodie.cleaner.policy", "KEEP_LATEST_COMMITS");
      properties.setProperty("hoodie.metatata.enable", "true");
      properties.setProperty("hoodie.parquet.small.file.limit", "-1");
      properties.setProperty("hoodie.cleaner.commits.retained", "10");
      properties.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersionString);
      properties.setProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), logFormatValue);
      properties.setProperty(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(),
          Boolean.toString(setNullForMissingColumns));
      if (setTimestampOverride) {
        properties.setProperty(HoodieCommonConfig.TIMESTAMP_LOGICAL_TYPE_OVERRIDES.key(),
            LOGICAL_REPAIR_TS_OVERRIDES);
      }

      boolean disableCompaction;
      if ("COMPACT".equals(operation)) {
        properties.setProperty("hoodie.compact.inline", "true");
        properties.setProperty("hoodie.compact.inline.max.delta.commits", "1");
        disableCompaction = false;
        // validate that there are no completed compaction (commit) instants in timeline.
      } else {
        properties.setProperty("hoodie.compact.inline", "false");
        disableCompaction = true;
      }

      if ("CLUSTER".equals(operation)) {
        properties.setProperty("hoodie.clustering.inline", "true");
        properties.setProperty("hoodie.clustering.inline.max.commits", "1");
        properties.setProperty("hoodie.clustering.plan.strategy.single.group.clustering.enabled", "true");
        properties.setProperty("hoodie.clustering.plan.strategy.sort.columns", "ts_millis,_row_key");
      }

      Option<TypedProperties> propt = Option.of(properties);

      if (!setTimestampOverride) {
        // No per-field verdict. The mislabeled timestamp/local-timestamp columns must be rejected on
        // the first sync rather than silently flipped, on both the reconcile and default write paths.
        // syncOnce wraps the guard's SchemaCompatibilityException in a HoodieIngestionException, so
        // walk the cause chain to assert on the underlying exception.
        Throwable thrown = assertThrows(Exception.class,
            () -> syncOnce(prepCfgForMorLogicalRepair(tableBasePath, dirName, "123", disableCompaction), propt));
        Throwable cause = thrown;
        while (cause != null && !(cause instanceof SchemaCompatibilityException)) {
          cause = cause.getCause();
        }
        assertTrue(cause instanceof SchemaCompatibilityException,
            "Expected a SchemaCompatibilityException in the cause chain, got: " + thrown);
        assertTrue(cause.getMessage().contains("column 'ts_millis'")
                && cause.getMessage().contains("without an explicit"),
            "Unexpected message: " + cause.getMessage());
        return;
      }

      syncOnce(prepCfgForMorLogicalRepair(tableBasePath, dirName, "123", disableCompaction), propt);

      String prevTimezone = sparkSession.conf().get("spark.sql.session.timeZone");
      try {
        if (!HoodieSparkUtils.gteqSpark3_5()) {
          sparkSession.conf().set("spark.sql.parquet.enableVectorizedReader", "false");
        }
        sparkSession.conf().set("spark.sql.session.timeZone", "UTC");
        Dataset<Row> df = sparkSession.read().format("hudi").load(tableBasePath);

        assertDataframe(df, 12, 14);

        metaClient = HoodieTableMetaClient.builder()
            .setConf(storage.getConf())
            .setBasePath(tableBasePath)
            .build();

        if ("CLUSTER".equals(operation)) {
          // after we cluster, the raw parquet should be correct

          // Validate raw parquet files
          HoodieTimeline completedCommitsTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
          Option<HoodieInstant> latestInstant = completedCommitsTimeline.lastInstant();
          assertTrue(latestInstant.isPresent(), "No completed commits found");

          List<String> baseFilePaths = collectLatestBaseFilePaths(metaClient);

          assertEquals(3, baseFilePaths.size());

          // Read raw parquet files
          Dataset<Row> rawParquetDf = sparkSession.read().parquet(baseFilePaths.toArray(new String[0]));
          assertDataframe(rawParquetDf, 12, 14);
        } else if ("COMPACT".equals(operation)) {
          // after compaction some files should be ok

          // Validate raw parquet files
          HoodieTimeline completedCommitsTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
          Option<HoodieInstant> latestInstant = completedCommitsTimeline.lastInstant();
          assertTrue(latestInstant.isPresent(), "No completed commits found");

          List<String> baseFilePaths = collectLatestBaseFilePaths(metaClient);

          assertEquals(7, baseFilePaths.size());

          // Read raw parquet files
          Dataset<Row> rawParquetDf = sparkSession.read().parquet(baseFilePaths.stream()
              // only read the compacted ones, the others are still incorrect
              .filter(path -> path.contains(latestInstant.get().requestedTime()))
              .toArray(String[]::new));
          assertDataframe(rawParquetDf, 2, 3);
        }
      } finally {
        sparkSession.conf().set("spark.sql.session.timeZone", prevTimezone);
        if (!HoodieSparkUtils.gteqSpark3_5()) {
          sparkSession.conf().set("spark.sql.parquet.enableVectorizedReader", "true");
        }
      }
    });
  }

  public static void assertDataframe(Dataset<Row> df, int above, int below) {
    List<Row> rows = df.collectAsList();
    assertEquals(above + below, rows.size());

    for (Row row : rows) {
      String val = row.getString(6);
      int hash = val.hashCode();

      if ((hash & 1) == 0) {
        assertEquals("2020-01-01T00:00:00.001Z", row.getTimestamp(15).toInstant().toString());
        assertEquals("2020-06-01T12:00:00.000001Z", row.getTimestamp(16).toInstant().toString());
        assertEquals("2015-05-20T12:34:56.001", row.get(17).toString());
        assertEquals("2017-07-07T07:07:07.000001", row.get(18).toString());
      } else {
        assertEquals("2019-12-31T23:59:59.999Z", row.getTimestamp(15).toInstant().toString());
        assertEquals("2020-06-01T11:59:59.999999Z", row.getTimestamp(16).toInstant().toString());
        assertEquals("2015-05-20T12:34:55.999", row.get(17).toString());
        assertEquals("2017-07-07T07:07:06.999999", row.get(18).toString());
      }
    }

    assertEquals(above, df.filter("ts_millis > timestamp('2020-01-01 00:00:00Z')").count());
    assertEquals(below, df.filter("ts_millis < timestamp('2020-01-01 00:00:00Z')").count());
    assertEquals(0, df.filter("ts_millis > timestamp('2020-01-01 00:00:00.001Z')").count());
    assertEquals(0, df.filter("ts_millis < timestamp('2019-12-31 23:59:59.999Z')").count());

    assertEquals(above, df.filter("ts_micros > timestamp('2020-06-01 12:00:00Z')").count());
    assertEquals(below, df.filter("ts_micros < timestamp('2020-06-01 12:00:00Z')").count());
    assertEquals(0, df.filter("ts_micros > timestamp('2020-06-01 12:00:00.000001Z')").count());
    assertEquals(0, df.filter("ts_micros < timestamp('2020-06-01 11:59:59.999999Z')").count());

    assertEquals(above, df.filter("local_ts_millis > CAST('2015-05-20 12:34:56' AS TIMESTAMP_NTZ)").count());
    assertEquals(below, df.filter("local_ts_millis < CAST('2015-05-20 12:34:56' AS TIMESTAMP_NTZ)").count());
    assertEquals(0, df.filter("local_ts_millis > CAST('2015-05-20 12:34:56.001' AS TIMESTAMP_NTZ)").count());
    assertEquals(0, df.filter("local_ts_millis < CAST('2015-05-20 12:34:55.999' AS TIMESTAMP_NTZ)").count());

    assertEquals(above, df.filter("local_ts_micros > CAST('2017-07-07 07:07:07' AS TIMESTAMP_NTZ)").count());
    assertEquals(below, df.filter("local_ts_micros < CAST('2017-07-07 07:07:07' AS TIMESTAMP_NTZ)").count());
    assertEquals(0, df.filter("local_ts_micros > CAST('2017-07-07 07:07:07.000001' AS TIMESTAMP_NTZ)").count());
    assertEquals(0, df.filter("local_ts_micros < CAST('2017-07-07 07:07:06.999999' AS TIMESTAMP_NTZ)").count());
  }

  private List<String> collectLatestBaseFilePaths(HoodieTableMetaClient metaClient) {
    List<String> baseFilePaths = new ArrayList<>();
    try (HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(
        new HoodieLocalEngineContext(metaClient.getStorageConf()),
        metaClient,
        HoodieMetadataConfig.newBuilder().enable(false).build())) {

      fsView.loadAllPartitions();
      fsView.getPartitionNames().forEach(partitionName ->
          fsView.getLatestFileSlices(partitionName).forEach(fileSlice -> {
            assertFalse(fileSlice.hasLogFiles(), "File slice should not have log files");
            Option<HoodieBaseFile> latestBaseFile = fileSlice.getBaseFile();
            assertTrue(latestBaseFile.isPresent(), "Base file should be present");
            baseFilePaths.add(latestBaseFile.get().getPath());
          }));
    }
    return baseFilePaths;
  }

  private String getMergerClassForRecordType(String recordType) {
    switch (recordType) {
      case "AVRO":
        return HoodieAvroRecordMerger.class.getName();
      case "SPARK":
        return DefaultSparkRecordMerger.class.getName();
      default:
        throw new IllegalArgumentException("Invalid record type: " + recordType);
    }
  }

  private String getTableVersionCode(String tableVersion) {
    switch (tableVersion) {
      case "SIX":
        return String.valueOf(HoodieTableVersion.SIX.versionCode());
      case "EIGHT":
        return String.valueOf(HoodieTableVersion.EIGHT.versionCode());
      case "CURRENT":
        return String.valueOf(HoodieTableVersion.current().versionCode());
      default:
        throw new IllegalArgumentException("Invalid table version: " + tableVersion);
    }
  }

  private HoodieStreamer.Config prepCfgForCowLogicalRepair(String tableBasePath,
                                                           String ignoreCheckpoint) throws Exception {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tableBasePath;
    cfg.tableType = "COPY_ON_WRITE";
    cfg.targetTableName = "trips_logical_types_json_cow_write";
    cfg.sourceClassName = "org.apache.hudi.utilities.sources.JsonDFSSource";
    cfg.schemaProviderClassName = "org.apache.hudi.utilities.schema.FilebasedSchemaProvider";
    cfg.sourceOrderingFields = "timestamp";
    cfg.ignoreCheckpoint = ignoreCheckpoint;
    cfg.operation = WriteOperationType.UPSERT;
    cfg.forceDisableCompaction = true;
    return cfg;
  }

  private HoodieStreamer.Config prepCfgForMorLogicalRepair(String tableBasePath,
                                                           String tableName,
                                                           String ignoreCheckpoint,
                                                           boolean disableCompaction) throws Exception {
    HoodieStreamer.Config cfg = new HoodieStreamer.Config();
    cfg.targetBasePath = tableBasePath;
    cfg.tableType = "MERGE_ON_READ";
    cfg.targetTableName = tableName;
    cfg.sourceClassName = "org.apache.hudi.utilities.sources.JsonDFSSource";
    cfg.schemaProviderClassName = "org.apache.hudi.utilities.schema.FilebasedSchemaProvider";
    cfg.sourceOrderingFields = "timestamp";
    cfg.ignoreCheckpoint = ignoreCheckpoint;
    cfg.operation = WriteOperationType.UPSERT;
    cfg.forceDisableCompaction = disableCompaction;
    return cfg;
  }

  @Test
  public void testNullSchemaProvider() {
    String tableBasePath = basePath + "/test_table";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true,
        false, false, null, null);
    Exception e = assertThrows(HoodieIngestionException.class, () -> {
      syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    }, "Should error out when schema provider is not provided");
    log.debug("Expected error during reading data from source ", e);
    String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
    assertTrue(errorMsg.contains("Schema provider is required for this operation and for the source of interest. "
        + "Please set '--schemaprovider-class' in the top level HoodieStreamer config for the source of interest. "
        + "Based on the schema provider class chosen, additional configs might be required. "
        + "For eg, if you choose 'org.apache.hudi.utilities.schema.SchemaRegistryProvider', "
        + "you may need to set configs like 'hoodie.streamer.schemaprovider.registry.url'."));
  }

  @Test
  public void testPayloadClassUpdate() throws Exception {
    String dataSetBasePath = basePath + "/test_dataset_mor_payload_class_update";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, false,
        true, false, null, "MERGE_ON_READ");
    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(1000, dataSetBasePath, sqlContext);
    HoodieTableMetaClient metaClient = UtilHelpers.createMetaClient(jsc, dataSetBasePath, false);
    assertEquals(metaClient.getTableConfig().getPayloadClass(), DefaultHoodieRecordPayload.class.getName());

    //now create one more deltaStreamer instance and update payload class
    cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, false,
        true, true, DummyAvroPayload.class.getName(), "MERGE_ON_READ");
    new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf());

    // NOTE: Payload class cannot be updated.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(metaClient.getTableConfig().getPayloadClass(), DefaultHoodieRecordPayload.class.getName());
  }

  @Test
  public void testPartialPayloadClass() throws Exception {
    String dataSetBasePath = basePath + "/test_dataset_mor";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, false,
        true, true, PartialUpdateAvroPayload.class.getName(), "MERGE_ON_READ");
    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(1000, dataSetBasePath, sqlContext);

    //now assert that hoodie.properties file now has updated payload class name
    HoodieTableMetaClient metaClient = UtilHelpers.createMetaClient(jsc, dataSetBasePath, false);
    assertEquals(metaClient.getTableConfig().getPayloadClass(), DefaultHoodieRecordPayload.class.getName());
  }

  @Disabled("To be fixed with HUDI-9714")
  @Test
  public void testPayloadClassUpdateWithCOWTable() throws Exception {
    String dataSetBasePath = basePath + "/test_dataset_cow";
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, false,
        true, false, null, null);
    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(1000, dataSetBasePath, sqlContext);

    Properties props = new Properties();
    String metaPath = dataSetBasePath + "/.hoodie/hoodie.properties";
    FileSystem fs = HadoopFSUtils.getFs(cfg.targetBasePath, jsc.hadoopConfiguration());
    try (InputStream inputStream = fs.open(new Path(metaPath))) {
      props.load(inputStream);
    }

    assertFalse(props.containsKey(HoodieTableConfig.PAYLOAD_CLASS_NAME.key()));
    assertTrue(props.containsKey(HoodieTableConfig.RECORD_MERGE_MODE.key()));
    assertTrue(props.containsKey(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key()));

    //now create one more deltaStreamer instance and update payload class
    cfg = TestHelpers.makeConfig(dataSetBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, false,
        true, true, DummyAvroPayload.class.getName(), null);
    new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf());

    props = new Properties();
    fs = HadoopFSUtils.getFs(cfg.targetBasePath, jsc.hadoopConfiguration());
    try (InputStream inputStream = fs.open(new Path(metaPath))) {
      props.load(inputStream);
    }

    //now using payload
    assertEquals(DummyAvroPayload.class.getName(), props.get(HoodieTableConfig.PAYLOAD_CLASS_NAME.key()));
  }

  private static Stream<Arguments> getArgumentsForFilterDupesWithPrecombineTest() {
    return Stream.of(
        Arguments.of(HoodieRecordType.AVRO, "MERGE_ON_READ", EMPTY_STRING),
        Arguments.of(HoodieRecordType.AVRO, "MERGE_ON_READ", "timestamp"),
        Arguments.of(HoodieRecordType.AVRO, "COPY_ON_WRITE", EMPTY_STRING),
        Arguments.of(HoodieRecordType.AVRO, "COPY_ON_WRITE", "timestamp"),
        Arguments.of(HoodieRecordType.SPARK, "MERGE_ON_READ", EMPTY_STRING),
        Arguments.of(HoodieRecordType.SPARK, "MERGE_ON_READ", "timestamp"),
        Arguments.of(HoodieRecordType.SPARK, "COPY_ON_WRITE", EMPTY_STRING),
        Arguments.of(HoodieRecordType.SPARK, "COPY_ON_WRITE", "timestamp"));
  }

  @ParameterizedTest
  @MethodSource("getArgumentsForFilterDupesWithPrecombineTest")
  public void testFilterDupesWithPrecombine(
      HoodieRecordType recordType, String tableType, String sourceOrderingField) throws Exception {
    String tableBasePath = basePath + "/test_dupes_tables_with_precombine";
    HoodieDeltaStreamer.Config cfg =
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    cfg.tableType = tableType;
    cfg.filterDupes = true;
    cfg.sourceOrderingFields = sourceOrderingField;
    addRecordMerger(recordType, cfg.configs);
    syncOnce(cfg);

    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Generate the same 1000 records + 1000 new ones
    // We use TestDataSource to assist w/ generating input data. for every subquent batches, it produces 50% inserts and 50% updates.
    runStreamSync(cfg, true, 2000, WriteOperationType.INSERT);
    assertRecordCount(2000, tableBasePath, sqlContext); // if filter dupes is not enabled, we should be expecting 3000 records here.
    TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);

    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testDeltaStreamerWithMultipleOrderingFields(HoodieTableType tableType) throws Exception {
    String tableBasePath = basePath + "/test_with_multiple_ordering_fields";
    HoodieDeltaStreamer.Config cfg =
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    cfg.tableType = tableType.name();
    cfg.filterDupes = true;
    cfg.sourceOrderingFields = "timestamp,rider";
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;

    TestDataSource.recordInstantTime = Option.of("002");
    syncOnce(cfg);
    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Generate new updates with lower recordInstantTime so that updates are rejected
    TestDataSource.recordInstantTime = Option.of("001");
    runStreamSync(cfg, false, 50, WriteOperationType.UPSERT);
    int numInserts = 25;
    // TestDataSource generates 25 inserts and 25 updates
    assertRecordCount(1025, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    // Filter records with rider-001 value and deduct the number of inserts to get number of updates written
    long numUpdates = sparkSession.read().format("hudi").load(tableBasePath).filter("rider = 'rider-001'").count()
        - numInserts;
    // There should be no updates since ordering value rider-001 is lower than existing record ordering value rider-002
    assertEquals(0, numUpdates);

    // Generate new updates with higher recordInstantTime so that updates are accepted
    TestDataSource.recordInstantTime = Option.of("003");
    runStreamSync(cfg, false, 50, WriteOperationType.UPSERT);
    // TestDataSource generates 25 inserts and 25 updates
    assertRecordCount(1050, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00002", tableBasePath, 3);
    // Filter records with rider-003 value and deduct the number of inserts to get number of updates written
    numUpdates = sparkSession.read().format("hudi").load(tableBasePath).filter("rider = 'rider-003'").count()
        - numInserts;
    // All updates should reflect since the ordering value rider-003 is higher
    assertEquals(25, numUpdates);

    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  public void testDeltaStreamerFailureWithChangingOrderingFields(HoodieTableType tableType) throws Exception {
    String tableBasePath = basePath + "/test_with_changing_ordering_fields";
    HoodieDeltaStreamer.Config cfg =
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    cfg.tableType = tableType.name();
    cfg.filterDupes = true;
    cfg.sourceOrderingFields = "timestamp,rider";
    cfg.recordMergeMode = RecordMergeMode.EVENT_TIME_ORDERING;
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.recordMergeStrategyId = HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID;

    TestDataSource.recordInstantTime = Option.of("001");
    syncOnce(cfg);
    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Change ordering fields in deltastreamer
    Exception e = assertThrows(HoodieException.class, () -> {
      cfg.sourceOrderingFields = "timestamp";
      TestDataSource.recordInstantTime = Option.of("002");
      runStreamSync(cfg, false, 10, WriteOperationType.UPSERT);
    });
    assertTrue(e.getMessage().contains("hoodie.table.ordering.fields") && e.getMessage().contains("timestamp,rider"));
  }

  @Test
  public void testFilterDupes() throws Exception {
    String tableBasePath = basePath + "/test_dupes_table";

    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    syncOnce(cfg);
    assertRecordCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Generate the same 1000 records + 1000 new ones for upsert
    runStreamSync(cfg, true, 2000, WriteOperationType.INSERT);
    assertRecordCount(2000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    // 1000 records for commit 00000 & 1000 for commit 00001
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1000, counts.get(0).getLong(1));
    assertEquals(1000, counts.get(1).getLong(1));

    // Test with empty commits
    HoodieTableMetaClient mClient = createMetaClient(jsc, tableBasePath);
    HoodieInstant lastFinished = mClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    HoodieDeltaStreamer.Config cfg2 = TestHelpers.makeDropAllConfig(tableBasePath, WriteOperationType.UPSERT);
    cfg2.configs.add(String.format("%s=false", HoodieCleanConfig.AUTO_CLEAN.key()));
    addRecordMerger(HoodieRecordType.AVRO, cfg2.configs);
    runStreamSync(cfg2, false, 2000, WriteOperationType.UPSERT);
    mClient = createMetaClient(jsc, tableBasePath);
    HoodieInstant newLastFinished = mClient.getCommitsTimeline().filterCompletedInstants().lastInstant().get();
    assertTrue(InstantComparison.compareTimestamps(newLastFinished.requestedTime(), GREATER_THAN, lastFinished.requestedTime()
    ));

    // Ensure it is empty
    HoodieCommitMetadata commitMetadata =
        mClient.getActiveTimeline().readCommitMetadata(newLastFinished);
    log.info("New Commit Metadata={}", commitMetadata);
    assertTrue(commitMetadata.getPartitionToWriteStats().isEmpty());

    // Try UPSERT with filterDupes true. Expect exception
    cfg2.filterDupes = true;
    cfg2.operation = WriteOperationType.UPSERT;
    try {
      syncOnce(cfg2);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("'--filter-dupes' needs to be disabled when '--op' is 'UPSERT' to ensure updates are not missed."));
    }
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  private void runStreamSync(
      HoodieDeltaStreamer.Config cfg, boolean filterDupes, int numberOfRecords, WriteOperationType operationType) throws Exception {
    cfg.filterDupes = filterDupes;
    cfg.sourceLimit = numberOfRecords;
    cfg.operation = operationType;
    syncOnce(cfg);
  }

  @Test
  public void testEmptyBatchWithNullSchemaValue() throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "0");

    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config config = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
        null, PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, null, "timestamp", null);
    HoodieDeltaStreamer deltaStreamer1 = new HoodieDeltaStreamer(config, jsc);
    deltaStreamer1.sync();
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);
    HoodieInstant firstCommit = metaClient.getActiveTimeline().lastInstant().get();
    deltaStreamer1.shutdownGracefully();

    prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, "2.parquet", false, null, null);
    HoodieDeltaStreamer.Config updatedConfig = config;
    updatedConfig.schemaProviderClassName = NullValueSchemaProvider.class.getName();
    updatedConfig.sourceClassName = TestParquetDFSSourceEmptyBatch.class.getName();
    HoodieDeltaStreamer deltaStreamer2 = new HoodieDeltaStreamer(updatedConfig, jsc);
    deltaStreamer2.sync();
    // since we mimic'ed empty batch, total records should be same as first sync().
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);

    // validate schema is set in commit even if target schema returns null on empty batch
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    HoodieInstant secondCommit = metaClient.reloadActiveTimeline().lastInstant().get();
    HoodieSchema lastCommitSchema = tableSchemaResolver.getTableSchema(secondCommit, true);
    assertNotEquals(firstCommit, secondCommit);
    assertNotEquals(lastCommitSchema, Schema.create(Schema.Type.NULL));
    deltaStreamer2.shutdownGracefully();
  }

  @Test
  public void testEmptyBatchWithNullSchemaFirstBatch() throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "0");

    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config config = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
        Collections.singletonList(TestIdentityTransformer.class.getName()), PROPS_FILENAME_TEST_PARQUET, false,
        false, 100000, false, null, "MERGE_ON_READ", "timestamp", null);

    config.schemaProviderClassName = NullValueSchemaProvider.class.getName();
    config.sourceClassName = TestParquetDFSSourceEmptyBatch.class.getName();
    syncOnce(config);
    assertRecordCount(0, tableBasePath, sqlContext);

    config.schemaProviderClassName = null;
    config.sourceClassName = ParquetDFSSource.class.getName();
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, "2.parquet", false, null, null);
    syncOnce(config);
    //since first batch has empty schema, only records from the second batch should be written
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testDropPartitionColumns(HoodieRecordType recordType) throws Exception {
    String tableBasePath = basePath + "/test_drop_partition_columns" + testNum++;
    // ingest data with dropping partition columns enabled
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT);
    addRecordMerger(recordType, cfg.configs);
    cfg.configs.add(String.format("%s=%s", HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), "true"));
    syncOnce(cfg);
    // assert ingest successful
    TestHelpers.assertAtLeastNCommits(1, tableBasePath);

    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(
        HoodieTestUtils.createMetaClient(storage, tableBasePath));
    // get schema from data file written in the latest commit
    HoodieSchema tableSchema = tableSchemaResolver.getTableSchemaFromDataFile();
    assertNotNull(tableSchema);

    List<String> tableFields = tableSchema.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList());
    // now assert that the partition column is not in the target schema
    assertFalse(tableFields.contains("partition_path"));
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Test
  public void testAutoGenerateRecordKeys() throws Exception {
    boolean useSchemaProvider = false;
    List<String> transformerClassNames = null;
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 100;
    boolean hasTransformer = transformerClassNames != null && !transformerClassNames.isEmpty();
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", true);

    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config config = TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
        transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
        useSchemaProvider, 100000, false, null, null, "timestamp", null);
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(config, jsc);
    deltaStreamer.sync();
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    // validate that auto record keys are enabled.
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tableBasePath).setConf(HoodieTestUtils.getDefaultStorageConf()).build();
    assertFalse(metaClient.getTableConfig().getRecordKeyFields().isPresent());

    prepareParquetDFSFiles(200, PARQUET_SOURCE_ROOT, "2.parquet", false, null, null);
    deltaStreamer.sync();
    assertRecordCount(parquetRecordsCount + 200, tableBasePath, sqlContext);
    testNum++;
    deltaStreamer.shutdownGracefully();
  }
}
