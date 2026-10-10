/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.agent.architect;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.table.HoodieTableFactory;
import org.apache.hudi.util.ChangelogModes;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Factory and planner fixtures for the bounded Hudi Architect Flink SQL paths.
 *
 * <p>The fixture depends on the released Hudi 1.2.0 Flink 1.20 bundle and Flink 1.20.1 rather
 * than the enclosing checkout. It plans the exact SQL golden files checked by the Python
 * contract tests and never starts a Flink job or external service.
 */
class TestFlinkArchitectSqlFixtures {

  private static final String ASF_LICENSE_MARKER =
      "Licensed to the Apache Software Foundation (ASF) under one";
  private static final Pattern SET_STATEMENT = Pattern.compile(
      "SET\\s+'([^']+)'\\s*=\\s*'([^']+)'\\s*;?");

  @Test
  void testStableKeySinkAndInsertPlan() throws Exception {
    TestContext context = newTestContext();
    StreamTableEnvironment tableEnv = context.tableEnv;
    registerStableKeySource(context);

    SqlFixture fixture = SqlFixture.load("stable_key.sql");
    applyRuntimeStatements(tableEnv, fixture.runtimeSql);
    tableEnv.executeSql(fixture.tableDdl);
    String plan = tableEnv.explainSql(
        fixture.insertSql,
        ExplainDetail.CHANGELOG_MODE,
        ExplainDetail.JSON_EXECUTION_PLAN);

    assertPinnedArtifacts();
    assertTrue(plan.contains("orders_hudi"), plan);
    assertTrue(plan.contains("hoodie_append_write: default_database.orders_hudi"), plan);
    assertTrue(plan.contains("changelogMode=[I]"), plan);
  }

  @Test
  void testAutoKeySinkAndInsertPlan() throws Exception {
    TestContext context = newTestContext();
    StreamTableEnvironment tableEnv = context.tableEnv;
    registerAutoKeySource(context);

    SqlFixture fixture = SqlFixture.load("auto_key.sql");
    applyRuntimeStatements(tableEnv, fixture.runtimeSql);
    tableEnv.executeSql(fixture.tableDdl);
    String plan = tableEnv.explainSql(
        fixture.insertSql,
        ExplainDetail.CHANGELOG_MODE,
        ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(plan.contains("events_hudi"), plan);
    assertTrue(plan.contains("hoodie_append_write: default_database.events_hudi"), plan);
    assertTrue(plan.contains("changelogMode=[I]"), plan);
  }

  @Test
  void testMutableCowSinkAndUpsertPlan() throws Exception {
    TestContext context = newTestContext();
    StreamTableEnvironment tableEnv = context.tableEnv;
    registerMutableSource(context);

    SqlFixture fixture = SqlFixture.load("mutable_cow.sql");
    applyRuntimeStatements(tableEnv, fixture.runtimeSql);
    tableEnv.executeSql(fixture.tableDdl);
    String plan = tableEnv.explainSql(
        fixture.insertSql,
        ExplainDetail.CHANGELOG_MODE,
        ExplainDetail.JSON_EXECUTION_PLAN);

    assertPinnedArtifacts();
    assertTrue(plan.contains("orders_mutable_hudi"), plan);
    assertTrue(plan.contains("stream_write: default_database.orders_mutable_hudi"), plan);
    assertTrue(plan.contains("changelogMode=[I,UA,D]"), plan);
    assertTrue(plan.contains("index_bootstrap"), plan);
    assertEquals(
        Set.of(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE),
        ChangelogModes.UPSERT.getContainedKinds());
    assertFalse(ChangelogModes.UPSERT.contains(RowKind.UPDATE_BEFORE));
  }

  @Test
  void testPinnedMutableOrderingConfiguration() {
    Configuration conf = mutableConfiguration();

    assertEquals("hoodie.write.record.merge.mode", FlinkOptions.RECORD_MERGE_MODE.key());
    assertEquals("event_version", conf.get(FlinkOptions.ORDERING_FIELDS));
    assertEquals("EVENT_TIME_ORDERING", conf.get(FlinkOptions.RECORD_MERGE_MODE));
    assertFalse(conf.get(FlinkOptions.CHANGELOG_ENABLED));
  }

  @Test
  void testPinnedMutableIndexConfiguration() {
    Configuration conf = mutableConfiguration();

    assertEquals("FLINK_STATE", conf.get(FlinkOptions.INDEX_TYPE));
    assertTrue(conf.get(FlinkOptions.INDEX_GLOBAL_ENABLED));
    assertEquals(0.0, conf.get(FlinkOptions.INDEX_STATE_TTL));
  }

  @Test
  void testPinnedMutableIndexBootstrapConfiguration() {
    Configuration conf = mutableConfiguration();

    assertTrue(conf.get(FlinkOptions.INDEX_BOOTSTRAP_ENABLED));
  }

  @Test
  void testPinnedFactorySkipsMissingRecordKeyCheckInAppendMode() {
    TestContext context = newTestContext();
    registerAutoKeySource(context);
    context.tableEnv.executeSql(
        "CREATE TABLE `missing_key_sink` (\n"
            + "  `payload` STRING,\n"
            + "  `event_ts` TIMESTAMP(3) NOT NULL\n"
            + ") WITH (\n"
            + "  'connector' = 'hudi',\n"
            + "  'path' = 'file:///tmp/hudi-architect-missing-key',\n"
            + "  'table.type' = 'COPY_ON_WRITE',\n"
            + "  'write.operation' = 'insert',\n"
            + "  'write.insert.cluster' = 'false',\n"
            + "  'hoodie.datasource.write.recordkey.field' = 'missing_id'\n"
            + ")");

    String plan = context.tableEnv.explainSql(
        "INSERT INTO `missing_key_sink` (`payload`, `event_ts`) "
            + "SELECT `payload`, `event_ts` FROM `events_source`",
        ExplainDetail.CHANGELOG_MODE,
        ExplainDetail.JSON_EXECUTION_PLAN);

    // Hudi 1.2.0 deliberately skips checkRecordKey in append mode. The Architect
    // validator must therefore reject this option before the factory is invoked.
    assertTrue(plan.contains("hoodie_append_write: default_database.missing_key_sink"), plan);
  }

  @Test
  void testPinnedAppendModeRequiresInsertClusteringDisabled() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.set(FlinkOptions.OPERATION, "insert");
    conf.set(FlinkOptions.INSERT_CLUSTER, false);
    assertTrue(OptionsResolver.isAppendMode(conf));

    conf.set(FlinkOptions.INSERT_CLUSTER, true);
    assertFalse(OptionsResolver.isAppendMode(conf));
  }

  @Test
  void testPinnedBinaryValuesUseObjectIdentityForRouting() throws Exception {
    assertPinnedArtifacts();
    DataType[] binaryTypes = {
        DataTypes.BYTES(), DataTypes.BINARY(2), DataTypes.VARBINARY(2)
    };
    for (DataType binaryType : binaryTypes) {
      Configuration conf = new Configuration();
      conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
      conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition_bytes");
      RowType rowType = (RowType) DataTypes.ROW(
          DataTypes.FIELD("id", binaryType.notNull()),
          DataTypes.FIELD("partition_bytes", binaryType.notNull())).getLogicalType();
      RowDataKeyGen keyGen = newRowDataKeyGen(conf, rowType);

      byte[] firstKey = {1, 2};
      byte[] secondKey = {1, 2};
      byte[] firstPartition = {3, 4};
      byte[] secondPartition = {3, 4};
      GenericRowData first = GenericRowData.of(firstKey, firstPartition);
      GenericRowData second = GenericRowData.of(secondKey, secondPartition);

      assertEquals(firstKey.toString(), keyGen.getRecordKey(first));
      assertEquals(secondKey.toString(), keyGen.getRecordKey(second));
      assertNotEquals(keyGen.getRecordKey(first), keyGen.getRecordKey(second));
      assertEquals(firstPartition.toString(), keyGen.getPartitionPath(first));
      assertEquals(secondPartition.toString(), keyGen.getPartitionPath(second));
      assertNotEquals(keyGen.getPartitionPath(first), keyGen.getPartitionPath(second));
    }
  }

  @Test
  void testPinnedPlannerAcceptsTemporalPrecisionSix() {
    TestContext context = newTestContext();
    context.tableEnv.executeSql(
        "CREATE TEMPORARY VIEW `temporal_boundary_source` AS SELECT\n"
            + "  CAST(NULL AS TIME(6)) AS `time_value`,\n"
            + "  CAST(NULL AS TIMESTAMP(6)) AS `timestamp_value`,\n"
            + "  CAST(NULL AS TIMESTAMP_LTZ(6)) AS `local_timestamp_value`");
    context.tableEnv.executeSql(
        "CREATE TABLE `temporal_boundary_sink` (\n"
            + "  `time_value` TIME(6),\n"
            + "  `timestamp_value` TIMESTAMP(6),\n"
            + "  `local_timestamp_value` TIMESTAMP_LTZ(6)\n"
            + ") WITH (\n"
            + "  'connector' = 'hudi',\n"
            + "  'path' = 'file:///tmp/hudi-architect-temporal-boundary',\n"
            + "  'table.type' = 'COPY_ON_WRITE',\n"
            + "  'write.operation' = 'insert',\n"
            + "  'write.insert.cluster' = 'false'\n"
            + ")");

    String plan = context.tableEnv.explainSql(
        "INSERT INTO `temporal_boundary_sink` "
            + "SELECT `time_value`, `timestamp_value`, `local_timestamp_value` "
            + "FROM `temporal_boundary_source`",
        ExplainDetail.CHANGELOG_MODE,
        ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(plan.contains("temporal_boundary_sink"), plan);
    assertTrue(plan.contains("changelogMode=[I]"), plan);
  }

  @Test
  void testPinnedPlannerRejectsTemporalPrecisionAboveSix() {
    String[] temporalTypes = {"TIMESTAMP", "TIMESTAMP_LTZ"};
    for (String temporalType : temporalTypes) {
      for (int precision : new int[] {7, 9}) {
        TestContext context = newTestContext();
        String suffix = temporalType.toLowerCase() + "_" + precision;
        context.tableEnv.executeSql(
            "CREATE TEMPORARY VIEW `source_" + suffix + "` AS SELECT "
                + "CAST(NULL AS " + temporalType + "(" + precision + ")) AS `value`");

        RuntimeException failure = assertThrows(
            RuntimeException.class,
            () -> {
              context.tableEnv.executeSql(
                  "CREATE TABLE `sink_" + suffix + "` (\n"
                      + "  `value` " + temporalType + "(" + precision + ")\n"
                      + ") WITH (\n"
                      + "  'connector' = 'hudi',\n"
                      + "  'path' = 'file:///tmp/hudi-architect-" + suffix + "',\n"
                      + "  'table.type' = 'COPY_ON_WRITE',\n"
                      + "  'write.operation' = 'insert',\n"
                      + "  'write.insert.cluster' = 'false'\n"
                      + ")");
              context.tableEnv.explainSql(
                  "INSERT INTO `sink_" + suffix + "` SELECT `value` FROM `source_"
                      + suffix + "`");
            });

        assertTrue(
            failureMessages(failure).contains("only supports precisions <= 6"),
            failureMessages(failure));
      }
    }
  }

  @Test
  void testPinnedSchemaConverterRejectsTimePrecisionAboveSix() {
    HoodieSchemaConverter.convertToSchema(new TimeType(6));
    for (int precision : new int[] {7, 9}) {
      IllegalArgumentException failure = assertThrows(
          IllegalArgumentException.class,
          () -> HoodieSchemaConverter.convertToSchema(new TimeType(precision)));
      assertTrue(
          failure.getMessage().contains("maximum precision is 6"),
          failure.getMessage());
    }
  }

  @Test
  void testPinnedPlannerRejectsNonAvroFieldName() {
    TestContext context = newTestContext();
    context.tableEnv.executeSql(
        "CREATE TEMPORARY VIEW `invalid_name_source` AS SELECT "
            + "CAST('alice' AS STRING) AS `user-id`");

    RuntimeException failure = assertThrows(
        RuntimeException.class,
        () -> {
          context.tableEnv.executeSql(
              "CREATE TABLE `invalid_name_sink` (\n"
                  + "  `user-id` STRING\n"
                  + ") WITH (\n"
                  + "  'connector' = 'hudi',\n"
                  + "  'path' = 'file:///tmp/hudi-architect-invalid-name',\n"
                  + "  'table.type' = 'COPY_ON_WRITE',\n"
                  + "  'write.operation' = 'insert',\n"
                  + "  'write.insert.cluster' = 'false'\n"
                  + ")");
          context.tableEnv.explainSql(
              "INSERT INTO `invalid_name_sink` SELECT `user-id` FROM `invalid_name_source`");
        });

    assertTrue(
        failureMessages(failure).contains("Illegal character in: user-id"),
        failureMessages(failure));
  }

  @Test
  void testPinnedWriterRejectsReservedHudiMetadataField() {
    assertPinnedArtifacts();
    Set<String> reservedNames = Set.of(
        "_hoodie_commit_seqno",
        "_hoodie_commit_time",
        "_hoodie_file_name",
        "_hoodie_operation",
        "_hoodie_partition_path",
        "_hoodie_record_key");
    assertEquals(reservedNames, HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION);

    for (String reservedName : reservedNames) {
      RowType physicalRowType = (RowType) DataTypes.ROW(
          DataTypes.FIELD(reservedName, DataTypes.STRING())).getLogicalType();
      boolean withOperationField =
          HoodieRecord.OPERATION_METADATA_FIELD.equals(reservedName);

      ValidationException failure = assertThrows(
          ValidationException.class,
          () -> DataTypeUtils.addMetadataFields(physicalRowType, withOperationField));

      assertTrue(
          failure.getMessage().contains("Field names must be unique"),
          failure.getMessage());
    }
  }

  @Test
  void testPinnedRuntimeCheckpointIntervalBoundary() {
    assertPinnedArtifacts();
    StreamExecutionEnvironment invalidEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    IllegalArgumentException failure = assertThrows(
        IllegalArgumentException.class,
        () -> invalidEnvironment.enableCheckpointing(9L));
    assertTrue(failure.getMessage().contains("larger than or equal to 10 ms"));

    StreamExecutionEnvironment validEnvironment =
        StreamExecutionEnvironment.getExecutionEnvironment();
    validEnvironment.enableCheckpointing(10L);
    assertEquals(10L, validEnvironment.getCheckpointConfig().getCheckpointInterval());
  }

  private static void assertPinnedArtifacts() {
    assertNotNull(HoodieTableFactory.class.getProtectionDomain().getCodeSource());
    assertEquals("1.2.0", HoodieTableFactory.class.getPackage().getImplementationVersion());
    assertEquals("1.20.1", EnvironmentSettings.class.getPackage().getImplementationVersion());
  }

  private static RowDataKeyGen newRowDataKeyGen(Configuration conf, RowType rowType)
      throws Exception {
    Method factory = RowDataKeyGen.class.getDeclaredMethod(
        "instance", Configuration.class, RowType.class);
    factory.setAccessible(true);
    return (RowDataKeyGen) factory.invoke(null, conf, rowType);
  }

  private static Configuration mutableConfiguration() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.set(FlinkOptions.OPERATION, "upsert");
    conf.set(FlinkOptions.ORDERING_FIELDS, "event_version");
    conf.set(FlinkOptions.RECORD_MERGE_MODE, "EVENT_TIME_ORDERING");
    conf.set(FlinkOptions.INDEX_TYPE, "FLINK_STATE");
    conf.set(FlinkOptions.INDEX_GLOBAL_ENABLED, true);
    conf.set(FlinkOptions.INDEX_STATE_TTL, 0.0);
    conf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);
    conf.set(FlinkOptions.CHANGELOG_ENABLED, false);
    return conf;
  }

  private static TestContext newTestContext() {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.enableCheckpointing(60_000L);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
        environment, EnvironmentSettings.newInstance().inStreamingMode().build());
    return new TestContext(environment, tableEnv);
  }

  private static void registerStableKeySource(TestContext context) {
    DataStream<Row> stream = context.environment.fromCollection(
        Collections.singletonList(Row.of(
            "id-1",
            "Alice",
            Timestamp.valueOf("2026-01-01 00:00:00"),
            Date.valueOf("2026-01-01"))),
        Types.ROW_NAMED(
            new String[] {"id", "name", "event_ts", "partition_date"},
            Types.STRING,
            Types.STRING,
            Types.SQL_TIMESTAMP,
            Types.SQL_DATE));
    Schema schema = Schema.newBuilder()
        .column("id", DataTypes.STRING().notNull())
        .column("name", DataTypes.STRING())
        .column("event_ts", DataTypes.TIMESTAMP(3).notNull())
        .column("partition_date", DataTypes.DATE().notNull())
        .build();
    registerInsertOnlyView(context.tableEnv, "orders_source", stream, schema);
  }

  private static void registerAutoKeySource(TestContext context) {
    DataStream<Row> stream = context.environment.fromCollection(
        Collections.singletonList(Row.of(
            "payload-1", Timestamp.valueOf("2026-01-01 00:00:00"))),
        Types.ROW_NAMED(
            new String[] {"payload", "event_ts"},
            Types.STRING,
            Types.SQL_TIMESTAMP));
    Schema schema = Schema.newBuilder()
        .column("payload", DataTypes.STRING())
        .column("event_ts", DataTypes.TIMESTAMP(3).notNull())
        .build();
    registerInsertOnlyView(context.tableEnv, "events_source", stream, schema);
  }

  private static void registerMutableSource(TestContext context) {
    Row insert = Row.ofKind(
        RowKind.INSERT,
        "id-1",
        "Alice",
        1L,
        Date.valueOf("2026-01-01"));
    Row updateAfter = Row.ofKind(
        RowKind.UPDATE_AFTER,
        "id-1",
        "Alice updated",
        2L,
        Date.valueOf("2026-01-01"));
    Row delete = Row.ofKind(
        RowKind.DELETE,
        "id-1",
        "Alice updated",
        3L,
        Date.valueOf("2026-01-01"));
    DataStream<Row> stream = context.environment.fromCollection(
        List.of(insert, updateAfter, delete),
        Types.ROW_NAMED(
            new String[] {"id", "name", "event_version", "partition_date"},
            Types.STRING,
            Types.STRING,
            Types.LONG,
            Types.SQL_DATE));
    Schema schema = Schema.newBuilder()
        .column("id", DataTypes.STRING().notNull())
        .column("name", DataTypes.STRING())
        .column("event_version", DataTypes.BIGINT().notNull())
        .column("partition_date", DataTypes.DATE().notNull())
        .primaryKey("id")
        .build();
    ChangelogMode upsertMode = ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .addContainedKind(RowKind.DELETE)
        .build();
    Table source = context.tableEnv.fromChangelogStream(stream, schema, upsertMode);
    context.tableEnv.createTemporaryView("orders_mutable_source", source);
  }

  private static void registerInsertOnlyView(
      StreamTableEnvironment tableEnv,
      String viewName,
      DataStream<Row> stream,
      Schema schema) {
    ChangelogMode declaredMode = ChangelogMode.insertOnly();
    assertEquals(ChangelogMode.insertOnly(), declaredMode);
    Table source = tableEnv.fromChangelogStream(stream, schema, declaredMode);
    tableEnv.createTemporaryView(viewName, source);
  }

  private static void applyRuntimeStatements(StreamTableEnvironment tableEnv, String runtimeSql) {
    for (String statement : runtimeSql.split(";\\s*")) {
      if (!statement.isBlank()) {
        Matcher matcher = SET_STATEMENT.matcher(statement.trim());
        assertTrue(matcher.matches(), statement);
        tableEnv.getConfig().getConfiguration().setString(matcher.group(1), matcher.group(2));
      }
    }
    Configuration configuration = tableEnv.getConfig().getConfiguration();
    assertEquals(RuntimeExecutionMode.STREAMING, configuration.get(ExecutionOptions.RUNTIME_MODE));
    assertEquals(
        Duration.ofSeconds(60),
        configuration.get(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL));
  }

  private static String failureMessages(Throwable failure) {
    StringBuilder messages = new StringBuilder();
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (current.getMessage() != null) {
        messages.append(current.getMessage()).append('\n');
      }
    }
    return messages.toString();
  }

  private static final class SqlFixture {
    private final String runtimeSql;
    private final String tableDdl;
    private final String insertSql;

    private SqlFixture(String runtimeSql, String tableDdl, String insertSql) {
      this.runtimeSql = runtimeSql;
      this.tableDdl = tableDdl;
      this.insertSql = insertSql;
    }

    private static SqlFixture load(String resourceName) throws IOException {
      String sql;
      try (InputStream stream = TestFlinkArchitectSqlFixtures.class
          .getClassLoader().getResourceAsStream(resourceName)) {
        assertNotNull(stream, resourceName);
        sql = stripLicenseHeader(
            new String(stream.readAllBytes(), StandardCharsets.UTF_8)).trim();
      }
      String[] sections = sql.split("\\R\\s*\\R", 3);
      assertEquals(3, sections.length, sql);
      return new SqlFixture(
          sections[0].trim(), sections[1].trim(), sections[2].trim());
    }

    private static String stripLicenseHeader(String sql) {
      int licenseEnd = sql.indexOf("*/");
      assertTrue(licenseEnd >= 0, "missing block-comment license header");
      assertTrue(sql.startsWith("/*"), "license header is not first");
      assertTrue(
          sql.substring(0, licenseEnd).contains(ASF_LICENSE_MARKER),
          "missing ASF license header");
      return sql.substring(licenseEnd + 2).stripLeading();
    }
  }

  private static final class TestContext {
    private final StreamExecutionEnvironment environment;
    private final StreamTableEnvironment tableEnv;

    private TestContext(
        StreamExecutionEnvironment environment, StreamTableEnvironment tableEnv) {
      this.environment = environment;
      this.tableEnv = tableEnv;
    }
  }
}
