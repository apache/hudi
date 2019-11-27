/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.integ;

import org.apache.hudi.common.util.collection.Pair;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

/**
 * Goes through steps described in https://hudi.incubator.apache.org/docker_demo.html
 *
 * To run this as a standalone test in the IDE or command line. First bring up the demo setup using
 * `docker/setup_demo.sh` and then run the test class as you would do normally.
 */
public class ITTestHoodieDemo extends ITTestBase {

  private static String HDFS_DATA_DIR = "/usr/hive/data/input";
  private static String HDFS_BATCH_PATH1 = HDFS_DATA_DIR + "/" + "batch_1.json";
  private static String HDFS_BATCH_PATH2 = HDFS_DATA_DIR + "/" + "batch_2.json";
  private static String HDFS_PRESTO_INPUT_TABLE_CHECK_PATH = HDFS_DATA_DIR + "/" + "presto-table-check.commands";
  private static String HDFS_PRESTO_INPUT_BATCH1_PATH = HDFS_DATA_DIR + "/" + "presto-batch1.commands";
  private static String HDFS_PRESTO_INPUT_BATCH2_PATH = HDFS_DATA_DIR + "/" + "presto-batch2-after-compaction.commands";

  private static String INPUT_BATCH_PATH1 = HOODIE_WS_ROOT + "/docker/demo/data/batch_1.json";
  private static String PRESTO_INPUT_TABLE_CHECK_RELATIVE_PATH = "/docker/demo/presto-table-check.commands";
  private static String PRESTO_INPUT_BATCH1_RELATIVE_PATH = "/docker/demo/presto-batch1.commands";
  private static String INPUT_BATCH_PATH2 = HOODIE_WS_ROOT + "/docker/demo/data/batch_2.json";
  private static String PRESTO_INPUT_BATCH2_RELATIVE_PATH = "/docker/demo/presto-batch2-after-compaction.commands";

  private static String COW_BASE_PATH = "/user/hive/warehouse/stock_ticks_cow";
  private static String MOR_BASE_PATH = "/user/hive/warehouse/stock_ticks_mor";
  private static String COW_TABLE_NAME = "stock_ticks_cow";
  private static String MOR_TABLE_NAME = "stock_ticks_mor";

  private static String DEMO_CONTAINER_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/setup_demo_container.sh";
  private static String MIN_COMMIT_TIME_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/get_min_commit_time.sh";
  private static String HUDI_CLI_TOOL = HOODIE_WS_ROOT + "/hudi-cli/hudi-cli.sh";
  private static String COMPACTION_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/compaction.commands";
  private static String SPARKSQL_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-batch1.commands";
  private static String SPARKSQL_BATCH2_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-batch2.commands";
  private static String SPARKSQL_INCREMENTAL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-incremental.commands";
  private static String HIVE_TBLCHECK_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-table-check.commands";
  private static String HIVE_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-batch1.commands";
  private static String HIVE_BATCH2_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-batch2-after-compaction.commands";
  private static String HIVE_INCREMENTAL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental.commands";

  private static String HIVE_SYNC_CMD_FMT =
      " --enable-hive-sync " + " --hoodie-conf hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hiveserver:10000 "
          + " --hoodie-conf hoodie.datasource.hive_sync.username=hive "
          + " --hoodie-conf hoodie.datasource.hive_sync.password=hive "
          + " --hoodie-conf hoodie.datasource.hive_sync.partition_fields=%s "
          + " --hoodie-conf hoodie.datasource.hive_sync.database=default "
          + " --hoodie-conf hoodie.datasource.hive_sync.table=%s";

  @Test
  public void testDemo() throws Exception {
    setupDemo();

    // batch 1
    ingestFirstBatchAndHiveSync();
    testHiveAfterFirstBatch();
    testPrestoAfterFirstBatch();
    testSparkSQLAfterFirstBatch();

    // batch 2
    ingestSecondBatchAndHiveSync();
    testHiveAfterSecondBatch();
    testPrestoAfterSecondBatch();
    testSparkSQLAfterSecondBatch();
    testIncrementalHiveQuery();
    testIncrementalSparkSQLQuery();

    // compaction
    scheduleAndRunCompaction();
    testHiveAfterSecondBatchAfterCompaction();
    testPrestoAfterSecondBatchAfterCompaction();
    testIncrementalHiveQueryAfterCompaction();
  }

  private void setupDemo() throws Exception {
    List<String> cmds = new ImmutableList.Builder<String>()
        .add("hdfs dfsadmin -safemode wait") // handle NN going into safe mode at times
        .add("hdfs dfs -mkdir -p " + HDFS_DATA_DIR)
        .add("hdfs dfs -copyFromLocal -f " + INPUT_BATCH_PATH1 + " " + HDFS_BATCH_PATH1)
        .add("/bin/bash " + DEMO_CONTAINER_SCRIPT).build();
    executeCommandStringsInDocker(ADHOC_1_CONTAINER, cmds);

    // create input dir in presto coordinator
    cmds = new ImmutableList.Builder<String>()
        .add("mkdir -p " + HDFS_DATA_DIR).build();
    executeCommandStringsInDocker(PRESTO_COORDINATOR, cmds);

    // copy presto sql files to presto coordinator
    executePrestoCopyCommand( System.getProperty("user.dir") + "/.." + PRESTO_INPUT_TABLE_CHECK_RELATIVE_PATH, HDFS_DATA_DIR);
    executePrestoCopyCommand( System.getProperty("user.dir") + "/.." + PRESTO_INPUT_BATCH1_RELATIVE_PATH, HDFS_DATA_DIR);
    executePrestoCopyCommand( System.getProperty("user.dir") + "/.." + PRESTO_INPUT_BATCH2_RELATIVE_PATH, HDFS_DATA_DIR);
  }

  private void ingestFirstBatchAndHiveSync() throws Exception {
    List<String> cmds = new ImmutableList.Builder<String>()
        .add("spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --storage-type COPY_ON_WRITE "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + COW_BASE_PATH + " --target-table " + COW_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties "
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + String.format(HIVE_SYNC_CMD_FMT, "dt", COW_TABLE_NAME))
        .add("spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --storage-type MERGE_ON_READ "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + MOR_BASE_PATH + " --target-table " + MOR_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties "
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + " --disable-compaction " + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_TABLE_NAME))
        .build();

    executeCommandStringsInDocker(ADHOC_1_CONTAINER, cmds);
  }

  private void testHiveAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeHiveCommandFile(HIVE_TBLCHECK_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| stock_ticks_cow     |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_mor     |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_mor_rt  |");

    assertStdOutContains(stdOutErrPair,
        "|   partition    |\n" + "+----------------+\n" + "| dt=2018-08-31  |\n" + "+----------------+\n", 3);

    stdOutErrPair = executeHiveCommandFile(HIVE_BATCH1_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n" + "+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:29:00  |\n", 3);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |\n",
        3);
  }

  private void testSparkSQLAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeSparkSQLCommand(SPARKSQL_BATCH1_COMMANDS, true);
    assertStdOutContains(stdOutErrPair, "|default |stock_ticks_cow   |false      |\n"
        + "|default |stock_ticks_mor    |false      |\n" + "|default |stock_ticks_mor_rt |false      |");
    assertStdOutContains(stdOutErrPair,
        "+------+-------------------+\n" + "|GOOG  |2018-08-31 10:29:00|\n" + "+------+-------------------+", 3);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |", 3);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|", 3);
  }

  private void ingestSecondBatchAndHiveSync() throws Exception {
    List<String> cmds = new ImmutableList.Builder<String>()
        .add("hdfs dfs -copyFromLocal -f " + INPUT_BATCH_PATH2 + " " + HDFS_BATCH_PATH2)
        .add("spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --storage-type COPY_ON_WRITE "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + COW_BASE_PATH + " --target-table " + COW_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties "
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + String.format(HIVE_SYNC_CMD_FMT, "dt", COW_TABLE_NAME))
        .add("spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --storage-type MERGE_ON_READ "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + MOR_BASE_PATH + " --target-table " + MOR_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties "
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + " --disable-compaction " + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_TABLE_NAME))
        .build();
    executeCommandStringsInDocker(ADHOC_1_CONTAINER, cmds);
  }

  private void testPrestoAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_TABLE_CHECK_PATH);
    assertStdOutContains(stdOutErrPair, "stock_ticks_cow");
    assertStdOutContains(stdOutErrPair, "stock_ticks_mor",2);

    stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_BATCH1_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\"", 4);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"", 2);
  }

  private void testHiveAfterSecondBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeHiveCommandFile(HIVE_BATCH1_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n" + "+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:29:00  |\n");
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n" + "+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:59:00  |\n", 2);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |\n");
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |\n",
        2);
  }

  private void testPrestoAfterSecondBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_BATCH1_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"",2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"");
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testHiveAfterSecondBatchAfterCompaction() throws Exception {
    Pair<String, String> stdOutErrPair = executeHiveCommandFile(HIVE_BATCH2_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n" + "+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:59:00  |", 2);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |",
        2);
  }

  private void testPrestoAfterSecondBatchAfterCompaction() throws Exception {
    Pair<String, String> stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_BATCH2_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"");
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testSparkSQLAfterSecondBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeSparkSQLCommand(SPARKSQL_BATCH2_COMMANDS, true);
    assertStdOutContains(stdOutErrPair,
        "+------+-------------------+\n" + "|GOOG  |2018-08-31 10:59:00|\n" + "+------+-------------------+", 2);

    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |", 3);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|", 2);
    assertStdOutContains(stdOutErrPair,
        "+------+-------------------+\n" + "|GOOG  |2018-08-31 10:29:00|\n" + "+------+-------------------+");
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|");
  }

  private void testIncrementalHiveQuery() throws Exception {
    String minCommitTime =
        executeCommandStringInDocker(ADHOC_2_CONTAINER, MIN_COMMIT_TIME_SCRIPT, true).getStdout().toString();
    Pair<String, String> stdOutErrPair =
        executeHiveCommandFile(HIVE_INCREMENTAL_COMMANDS, "min.commit.time=" + minCommitTime + "`");
    assertStdOutContains(stdOutErrPair, "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |");
  }

  private void testIncrementalHiveQueryAfterCompaction() throws Exception {
    String minCommitTime =
        executeCommandStringInDocker(ADHOC_2_CONTAINER, MIN_COMMIT_TIME_SCRIPT, true).getStdout().toString();
    Pair<String, String> stdOutErrPair =
        executeHiveCommandFile(HIVE_INCREMENTAL_COMMANDS, "min.commit.time=" + minCommitTime + "`");
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |");
  }

  private void testIncrementalSparkSQLQuery() throws Exception {
    Pair<String, String> stdOutErrPair = executeSparkSQLCommand(SPARKSQL_INCREMENTAL_COMMANDS, true);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|");
    assertStdOutContains(stdOutErrPair, "|default |stock_ticks_cow           |false      |\n"
        + "|default |stock_ticks_derived_mor   |false      |\n" + "|default |stock_ticks_derived_mor_rt|false      |\n"
        + "|default |stock_ticks_mor           |false      |\n" + "|default |stock_ticks_mor_rt        |false      |\n"
        + "|        |stock_ticks_cow_incr      |true       |");
    assertStdOutContains(stdOutErrPair, "|count(1)|\n" + "+--------+\n" + "|99     |", 2);
  }

  private void scheduleAndRunCompaction() throws Exception {
    executeCommandStringInDocker(ADHOC_1_CONTAINER, HUDI_CLI_TOOL + " --cmdfile " + COMPACTION_COMMANDS, true);
  }
}
