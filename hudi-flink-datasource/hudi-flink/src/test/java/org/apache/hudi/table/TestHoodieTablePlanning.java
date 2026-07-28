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

package org.apache.hudi.table;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Planning tests for the Hudi table source, sink, and factory.
 *
 * <p>The tests translate plans but deliberately do not execute a Flink job.
 */
class TestHoodieTablePlanning {

  @TempDir
  File tempFile;

  @Test
  void testSourcePushDownsForBothSourceImplementations() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    createTable(tableEnv, "source_v2", new File(tempFile, "source_v2"), true);
    createTable(tableEnv, "legacy_source", new File(tempFile, "legacy_source"), false);

    for (String tableName : new String[] {"source_v2", "legacy_source"}) {
      String pushedPlan = tableEnv.explainSql(
          "SELECT name FROM " + tableName + " WHERE `partition` = 'p1'",
          ExplainDetail.CHANGELOG_MODE,
          ExplainDetail.JSON_EXECUTION_PLAN);
      assertTrue(pushedPlan.contains("TableSourceScan"), pushedPlan);
      assertTrue(pushedPlan.contains("filter=[=(partition"), pushedPlan);
      assertTrue(pushedPlan.contains("project=[name, partition]"), pushedPlan);

      String limitedPlan = tableEnv.explainSql(
          "SELECT name FROM " + tableName + " LIMIT 2",
          ExplainDetail.CHANGELOG_MODE,
          ExplainDetail.JSON_EXECUTION_PLAN);
      assertTrue(limitedPlan.contains("limit=[2]"), limitedPlan);
    }
  }

  @Test
  void testSinkPlanning() throws Exception {
    TableEnvironment tableEnv = TestTableEnvs.getBatchTableEnv();
    createTable(tableEnv, "sink_table", new File(tempFile, "sink_table"), true);

    String plan = tableEnv.explainSql(
        "INSERT INTO sink_table VALUES "
            + "('id1', 'Alice', 20, TIMESTAMP '2026-01-01 00:00:00', 'p1')",
        ExplainDetail.CHANGELOG_MODE,
        ExplainDetail.JSON_EXECUTION_PLAN);

    assertTrue(plan.contains("Sink(table=[default_catalog.default_database.sink_table]"), plan);
    assertTrue(plan.contains("stream_write: default_database.sink_table"), plan);
  }

  private static void createTable(
      TableEnvironment tableEnv,
      String tableName,
      File tablePath,
      boolean sourceV2) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tablePath.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);

    tableEnv.executeSql(
        "CREATE TABLE " + tableName + " ("
            + "uuid STRING,"
            + "name STRING,"
            + "age INT,"
            + "ts TIMESTAMP(3),"
            + "`partition` STRING,"
            + "PRIMARY KEY (uuid) NOT ENFORCED"
            + ") PARTITIONED BY (`partition`) WITH ("
            + "'connector' = 'hudi',"
            + "'path' = '" + sqlLiteral(tablePath.getAbsolutePath()) + "',"
            + "'" + FlinkOptions.ORDERING_FIELDS.key() + "' = 'ts',"
            + "'" + FlinkOptions.READ_SOURCE_V2_ENABLED.key() + "' = '" + sourceV2 + "'"
            + ")");
  }

  private static String sqlLiteral(String value) {
    return value.replace("'", "''");
  }
}
