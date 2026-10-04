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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.table.HoodieTableFactory;

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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Factory and planner fixtures for the bounded Hudi Architect PR2 path.
 *
 * <p>The fixture depends on the released Hudi 1.2.0 Flink 1.20 bundle and Flink 1.20.1 rather
 * than the enclosing checkout. It plans the exact SQL golden files checked by the Python
 * contract tests and never starts a Flink job or external service.
 */
class TestFlinkArchitectSqlFixtures {

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

  private static void assertPinnedArtifacts() {
    assertNotNull(HoodieTableFactory.class.getProtectionDomain().getCodeSource());
    assertEquals("1.2.0", HoodieTableFactory.class.getPackage().getImplementationVersion());
    assertEquals("1.20.1", EnvironmentSettings.class.getPackage().getImplementationVersion());
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
        sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8).trim();
      }
      String[] sections = sql.split("\\R\\s*\\R", 3);
      assertEquals(3, sections.length, sql);
      return new SqlFixture(
          sections[0].trim(), sections[1].trim(), sections[2].trim());
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
