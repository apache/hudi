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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Goes through steps described in https://hudi.apache.org/docker_demo.html
 * <p>
 * To run this as a standalone test in the IDE or command line. First bring up the demo setup using
 * `docker/setup_demo.sh` and then run the test class as you would do normally.
 */
public class ITTestHoodieDemo extends ITTestBase {

  private static final String TRINO_TABLE_CHECK_FILENAME = "trino-table-check.commands";
  private static final String TRINO_BATCH1_FILENAME = "trino-batch1.commands";
  private static final String TRINO_BATCH2_FILENAME = "trino-batch2-after-compaction.commands";

  private static final String HDFS_DATA_DIR = "/usr/hive/data/input";
  private static final String HDFS_BATCH_PATH1 = HDFS_DATA_DIR + "/batch_1.json";
  private static final String HDFS_BATCH_PATH2 = HDFS_DATA_DIR + "/batch_2.json";
  private static final String HDFS_PRESTO_INPUT_TABLE_CHECK_PATH = HDFS_DATA_DIR + "/presto-table-check.commands";
  private static final String HDFS_PRESTO_INPUT_BATCH1_PATH = HDFS_DATA_DIR + "/presto-batch1.commands";
  private static final String HDFS_PRESTO_INPUT_BATCH2_PATH = HDFS_DATA_DIR + "/presto-batch2-after-compaction.commands";
  private static final String HDFS_TRINO_INPUT_TABLE_CHECK_PATH = HDFS_DATA_DIR + "/" + TRINO_TABLE_CHECK_FILENAME;
  private static final String HDFS_TRINO_INPUT_BATCH1_PATH = HDFS_DATA_DIR + "/" + TRINO_BATCH1_FILENAME;
  private static final String HDFS_TRINO_INPUT_BATCH2_PATH = HDFS_DATA_DIR + "/" + TRINO_BATCH2_FILENAME;

  private static final String INPUT_BATCH_PATH1 = HOODIE_WS_ROOT + "/docker/demo/data/batch_1.json";
  private static final String PRESTO_INPUT_TABLE_CHECK_RELATIVE_PATH = "/docker/demo/presto-table-check.commands";
  private static final String PRESTO_INPUT_BATCH1_RELATIVE_PATH = "/docker/demo/presto-batch1.commands";
  private static final String INPUT_BATCH_PATH2 = HOODIE_WS_ROOT + "/docker/demo/data/batch_2.json";
  private static final String PRESTO_INPUT_BATCH2_RELATIVE_PATH = "/docker/demo/presto-batch2-after-compaction.commands";
  private static final String TRINO_INPUT_TABLE_CHECK_RELATIVE_PATH = "/docker/demo/" + TRINO_TABLE_CHECK_FILENAME;
  private static final String TRINO_INPUT_BATCH1_RELATIVE_PATH = "/docker/demo/" + TRINO_BATCH1_FILENAME;
  private static final String TRINO_INPUT_BATCH2_RELATIVE_PATH = "/docker/demo/" + TRINO_BATCH2_FILENAME;

  private static final String COW_BASE_PATH = "/user/hive/warehouse/stock_ticks_cow";
  private static final String MOR_BASE_PATH = "/user/hive/warehouse/stock_ticks_mor";
  private static final String COW_TABLE_NAME = "stock_ticks_cow";
  private static final String MOR_TABLE_NAME = "stock_ticks_mor";

  private static final String BOOTSTRAPPED_SRC_PATH = "/user/hive/warehouse/stock_ticks_cow_bs_src";
  private static final String COW_BOOTSTRAPPED_BASE_PATH = "/user/hive/warehouse/stock_ticks_cow_bs";
  private static final String MOR_BOOTSTRAPPED_BASE_PATH = "/user/hive/warehouse/stock_ticks_mor_bs";
  private static final String COW_BOOTSTRAPPED_TABLE_NAME = "stock_ticks_cow_bs";
  private static final String MOR_BOOTSTRAPPED_TABLE_NAME = "stock_ticks_mor_bs";

  private static final String DEMO_CONTAINER_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/setup_demo_container.sh";
  private static final String MIN_COMMIT_TIME_COW_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/get_min_commit_time_cow.sh";
  private static final String MIN_COMMIT_TIME_MOR_SCRIPT = HOODIE_WS_ROOT + "/docker/demo/get_min_commit_time_mor.sh";
  private static final String HUDI_CLI_TOOL = HOODIE_WS_ROOT + "/hudi-cli/hudi-cli.sh";
  private static final String COMPACTION_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/compaction.commands";
  private static final String COMPACTION_BOOTSTRAP_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/compaction-bootstrap.commands";
  private static final String SPARKSQL_BS_PREP_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-bootstrap-prep-source.commands";
  private static final String SPARKSQL_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-batch1.commands";
  private static final String SPARKSQL_BATCH2_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-batch2.commands";
  private static final String SPARKSQL_INCREMENTAL_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/sparksql-incremental.commands";
  private static final String HIVE_TBLCHECK_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-table-check.commands";
  private static final String HIVE_BATCH1_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-batch1.commands";
  private static final String HIVE_BATCH2_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-batch2-after-compaction.commands";
  private static final String HIVE_INCREMENTAL_COW_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental-cow.commands";
  private static final String HIVE_INCREMENTAL_MOR_RO_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental-mor-ro.commands";
  private static final String HIVE_INCREMENTAL_MOR_RT_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/hive-incremental-mor-rt.commands";

  private HoodieFileFormat baseFileFormat;

  private static String HIVE_SYNC_CMD_FMT =
      " --enable-hive-sync --hoodie-conf hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hiveserver:10000 "
          + " --hoodie-conf hoodie.datasource.hive_sync.username=hive "
          + " --hoodie-conf hoodie.datasource.hive_sync.password=hive "
          + " --hoodie-conf hoodie.datasource.hive_sync.partition_fields=%s "
          + " --hoodie-conf hoodie.datasource.hive_sync.database=default "
          + " --hoodie-conf hoodie.datasource.hive_sync.table=%s";

  @AfterEach
  public void clean() throws Exception {
    String hdfsCmd = "hdfs dfs -rm -R ";
    List<String> tablePaths = CollectionUtils.createImmutableList(
        COW_BASE_PATH, MOR_BASE_PATH, COW_BOOTSTRAPPED_BASE_PATH, MOR_BOOTSTRAPPED_BASE_PATH);
    for (String tablePath : tablePaths) {
      executeCommandStringInDocker(ADHOC_1_CONTAINER, hdfsCmd + tablePath, true);
    }
  }

  @Test
  public void testParquetDemo() throws Exception {
    baseFileFormat = HoodieFileFormat.PARQUET;

    setupDemo();

    // batch 1
    ingestFirstBatchAndHiveSync();
    testHiveAfterFirstBatch();
    testPrestoAfterFirstBatch();
    testTrinoAfterFirstBatch();
    testSparkSQLAfterFirstBatch();

    // batch 2
    ingestSecondBatchAndHiveSync();
    testHiveAfterSecondBatch();
    testPrestoAfterSecondBatch();
    testTrinoAfterSecondBatch();
    testSparkSQLAfterSecondBatch();
    testIncrementalHiveQueryBeforeCompaction();
    testIncrementalSparkSQLQuery();

    // compaction
    scheduleAndRunCompaction();

    testHiveAfterSecondBatchAfterCompaction();
    testPrestoAfterSecondBatchAfterCompaction();
    testTrinoAfterSecondBatchAfterCompaction();
    testIncrementalHiveQueryAfterCompaction();
  }

  @Test
  @Disabled
  public void testHFileDemo() throws Exception {
    baseFileFormat = HoodieFileFormat.HFILE;

    // TODO: Presto, Trino and SparkSQL support for HFile format

    setupDemo();

    // batch 1
    ingestFirstBatchAndHiveSync();
    testHiveAfterFirstBatch();
    //testPrestoAfterFirstBatch();
    //testTrinoAfterFirstBatch();
    //testSparkSQLAfterFirstBatch();

    // batch 2
    ingestSecondBatchAndHiveSync();
    testHiveAfterSecondBatch();
    //testPrestoAfterSecondBatch();
    //testTrinoAfterSecondBatch();
    //testSparkSQLAfterSecondBatch();
    testIncrementalHiveQueryBeforeCompaction();
    //testIncrementalSparkSQLQuery();

    // compaction
    scheduleAndRunCompaction();
    testHiveAfterSecondBatchAfterCompaction();
    //testPrestoAfterSecondBatchAfterCompaction();
    //testTrinoAfterSecondBatchAfterCompaction();
    //testIncrementalHiveQueryAfterCompaction();
  }

  private void setupDemo() throws Exception {
    List<String> cmds = CollectionUtils.createImmutableList("hdfs dfsadmin -safemode wait",
        "hdfs dfs -mkdir -p " + HDFS_DATA_DIR,
        "hdfs dfs -copyFromLocal -f " + INPUT_BATCH_PATH1 + " " + HDFS_BATCH_PATH1,
        "/bin/bash " + DEMO_CONTAINER_SCRIPT,
        "mkdir -p " + HDFS_DATA_DIR);

    executeCommandStringsInDocker(ADHOC_1_CONTAINER, cmds);

    // create input dir in presto coordinator
    cmds = Collections.singletonList("mkdir -p " + HDFS_DATA_DIR);
    executeCommandStringsInDocker(PRESTO_COORDINATOR, cmds);

    // copy presto sql files to presto coordinator
    executePrestoCopyCommand(System.getProperty("user.dir") + "/.." + PRESTO_INPUT_TABLE_CHECK_RELATIVE_PATH, HDFS_DATA_DIR);
    executePrestoCopyCommand(System.getProperty("user.dir") + "/.." + PRESTO_INPUT_BATCH1_RELATIVE_PATH, HDFS_DATA_DIR);
    executePrestoCopyCommand(System.getProperty("user.dir") + "/.." + PRESTO_INPUT_BATCH2_RELATIVE_PATH, HDFS_DATA_DIR);

    executeTrinoCopyCommand(System.getProperty("user.dir") + "/.." + TRINO_INPUT_TABLE_CHECK_RELATIVE_PATH, HDFS_DATA_DIR);
    executeTrinoCopyCommand(System.getProperty("user.dir") + "/.." + TRINO_INPUT_BATCH1_RELATIVE_PATH, HDFS_DATA_DIR);
    executeTrinoCopyCommand(System.getProperty("user.dir") + "/.." + TRINO_INPUT_BATCH2_RELATIVE_PATH, HDFS_DATA_DIR);
  }

  private void ingestFirstBatchAndHiveSync() throws Exception {
    List<String> cmds = CollectionUtils.createImmutableList(
        "spark-submit"
            + " --conf \'spark.executor.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\'"
            + " --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --table-type COPY_ON_WRITE "
            + " --base-file-format " + baseFileFormat.toString()
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + COW_BASE_PATH + " --target-table " + COW_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties"
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider ",
        "spark-submit --class org.apache.hudi.hive.HiveSyncTool " + HUDI_HIVE_SYNC_BUNDLE
            + " --database default"
            + " --table " + COW_TABLE_NAME
            + " --base-path " + COW_BASE_PATH
            + " --base-file-format " + baseFileFormat.toString()
            + " --user hive"
            + " --pass hive"
            + " --jdbc-url jdbc:hive2://hiveserver:10000"
            + " --partitioned-by dt",
        ("spark-submit"
            + " --conf \'spark.executor.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\'"
            + " --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --table-type MERGE_ON_READ "
            + " --base-file-format " + baseFileFormat.toString()
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + MOR_BASE_PATH + " --target-table " + MOR_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties"
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + " --disable-compaction " + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_TABLE_NAME)));

    executeCommandStringsInDocker(ADHOC_1_CONTAINER, cmds);
    executeSparkSQLCommand(SPARKSQL_BS_PREP_COMMANDS, true);
    List<String> bootstrapCmds = CollectionUtils.createImmutableList(
        "spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
        + " --table-type COPY_ON_WRITE "
        + " --run-bootstrap "
        + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
        + " --target-base-path " + COW_BOOTSTRAPPED_BASE_PATH + " --target-table " + COW_BOOTSTRAPPED_TABLE_NAME
        + " --props /var/demo/config/dfs-source.properties"
        + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
        + " --initial-checkpoint-provider"
        + " org.apache.hudi.utilities.checkpointing.InitialCheckpointFromAnotherHoodieTimelineProvider"
        + " --hoodie-conf hoodie.bootstrap.base.path=" + BOOTSTRAPPED_SRC_PATH
        + " --hoodie-conf hoodie.deltastreamer.checkpoint.provider.path=" + COW_BASE_PATH
        + " --hoodie-conf hoodie.bootstrap.parallelism=2 "
        + " --hoodie-conf hoodie.bootstrap.keygen.class=" + SimpleKeyGenerator.class.getName()
        + String.format(HIVE_SYNC_CMD_FMT, "dt", COW_BOOTSTRAPPED_TABLE_NAME),
        "spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
        + " --table-type MERGE_ON_READ "
        + " --run-bootstrap "
        + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
        + " --target-base-path " + MOR_BOOTSTRAPPED_BASE_PATH + " --target-table " + MOR_BOOTSTRAPPED_TABLE_NAME
        + " --props /var/demo/config/dfs-source.properties"
        + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
        + " --initial-checkpoint-provider"
        + " org.apache.hudi.utilities.checkpointing.InitialCheckpointFromAnotherHoodieTimelineProvider"
        + " --hoodie-conf hoodie.bootstrap.base.path=" + BOOTSTRAPPED_SRC_PATH
        + " --hoodie-conf hoodie.deltastreamer.checkpoint.provider.path=" + COW_BASE_PATH
        + " --hoodie-conf hoodie.bootstrap.parallelism=2 "
        + " --hoodie-conf hoodie.bootstrap.keygen.class=" + SimpleKeyGenerator.class.getName()
        + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_BOOTSTRAPPED_TABLE_NAME));
    executeCommandStringsInDocker(ADHOC_1_CONTAINER, bootstrapCmds);
  }

  private void testHiveAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeHiveCommandFile(HIVE_TBLCHECK_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| stock_ticks_cow     |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_cow_bs  |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_mor_ro  |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_mor_rt  |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_mor_bs_ro  |");
    assertStdOutContains(stdOutErrPair, "| stock_ticks_mor_bs_rt  |");
    assertStdOutContains(stdOutErrPair,
        "|   partition    |\n+----------------+\n| dt=2018-08-31  |\n+----------------+\n", 3);

    // There should have 5 data source tables except stock_ticks_mor_bs_rt.
    // After [HUDI-2071] has solved, we can inc the number 5 to 6.
    assertStdOutContains(stdOutErrPair, "'spark.sql.sources.provider'='hudi'", 5);

    stdOutErrPair = executeHiveCommandFile(HIVE_BATCH1_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:29:00  |\n", 6);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |\n",
        6);
  }

  private void testSparkSQLAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeSparkSQLCommand(SPARKSQL_BATCH1_COMMANDS, true);
    assertStdOutContains(stdOutErrPair, "|default |stock_ticks_cow   |false      |\n"
                                                    + "|default |stock_ticks_cow_bs   |false      |\n"
                                                    + "|default |stock_ticks_mor_bs_ro |false      |\n"
                                                    +  "|default |stock_ticks_mor_bs_rt |false      |"
                                                    + "|default |stock_ticks_mor_ro |false      |\n"
                                                    +  "|default |stock_ticks_mor_rt |false      |");
    assertStdOutContains(stdOutErrPair,
        "+------+-------------------+\n|GOOG  |2018-08-31 10:29:00|\n+------+-------------------+", 6);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |", 6);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|", 6);
  }

  private void ingestSecondBatchAndHiveSync() throws Exception {
    // Note : Unlike normal tables, bootstrapped tables do not have checkpoint. So, they
    // begin with null checkpoint and read all states.
    List<String> cmds = CollectionUtils.createImmutableList(
            ("hdfs dfs -copyFromLocal -f " + INPUT_BATCH_PATH2 + " " + HDFS_BATCH_PATH2),
            ("spark-submit"
            + " --conf \'spark.executor.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\'"
            + " --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --table-type COPY_ON_WRITE "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + COW_BASE_PATH + " --target-table " + COW_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties"
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + String.format(HIVE_SYNC_CMD_FMT, "dt", COW_TABLE_NAME)),
            ("spark-submit"
            + " --conf \'spark.executor.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console\'"
            + " --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --table-type MERGE_ON_READ "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + MOR_BASE_PATH + " --target-table " + MOR_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties"
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + " --disable-compaction " + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_TABLE_NAME)),
            ("spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --table-type COPY_ON_WRITE "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + COW_BOOTSTRAPPED_BASE_PATH + " --target-table " + COW_BOOTSTRAPPED_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties"
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + String.format(HIVE_SYNC_CMD_FMT, "dt", COW_BOOTSTRAPPED_TABLE_NAME)),
            ("spark-submit --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer " + HUDI_UTILITIES_BUNDLE
            + " --table-type MERGE_ON_READ "
            + " --source-class org.apache.hudi.utilities.sources.JsonDFSSource --source-ordering-field ts "
            + " --target-base-path " + MOR_BOOTSTRAPPED_BASE_PATH + " --target-table " + MOR_BOOTSTRAPPED_TABLE_NAME
            + " --props /var/demo/config/dfs-source.properties"
            + " --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider "
            + " --disable-compaction " + String.format(HIVE_SYNC_CMD_FMT, "dt", MOR_BOOTSTRAPPED_TABLE_NAME)));
    executeCommandStringsInDocker(ADHOC_1_CONTAINER, cmds);
  }

  private void testPrestoAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_TABLE_CHECK_PATH);
    assertStdOutContains(stdOutErrPair, "stock_ticks_cow", 2);
    assertStdOutContains(stdOutErrPair, "stock_ticks_mor",4);

    stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_BATCH1_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\"", 4);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"", 2);
  }

  private void testTrinoAfterFirstBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeTrinoCommandFile(HDFS_TRINO_INPUT_TABLE_CHECK_PATH);
    assertStdOutContains(stdOutErrPair, "stock_ticks_cow", 2);
    assertStdOutContains(stdOutErrPair, "stock_ticks_mor", 4);

    stdOutErrPair = executeTrinoCommandFile(HDFS_TRINO_INPUT_BATCH1_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\"", 4);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"", 2);
  }

  private void testHiveAfterSecondBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeHiveCommandFile(HIVE_BATCH1_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:29:00  |\n", 2);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:59:00  |\n", 4);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:29:00  | 3391    | 1230.1899  | 1230.085  |\n", 2);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |\n",
        4);
  }

  private void testPrestoAfterSecondBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executePrestoCommandFile(HDFS_PRESTO_INPUT_BATCH1_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"");
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testTrinoAfterSecondBatch() throws Exception {
    Pair<String, String> stdOutErrPair = executeTrinoCommandFile(HDFS_TRINO_INPUT_BATCH1_PATH);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 09:59:00\",\"6330\",\"1230.5\",\"1230.02\"", 2);
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:29:00\",\"3391\",\"1230.1899\",\"1230.085\"");
    assertStdOutContains(stdOutErrPair,
        "\"GOOG\",\"2018-08-31 10:59:00\",\"9021\",\"1227.1993\",\"1227.215\"");
  }

  private void testHiveAfterSecondBatchAfterCompaction() throws Exception {
    Pair<String, String> stdOutErrPair = executeHiveCommandFile(HIVE_BATCH2_COMMANDS);
    assertStdOutContains(stdOutErrPair, "| symbol  |         _c1          |\n+---------+----------------------+\n"
        + "| GOOG    | 2018-08-31 10:59:00  |", 4);
    assertStdOutContains(stdOutErrPair,
        "| symbol  |          ts          | volume  |    open    |   close   |\n"
            + "+---------+----------------------+---------+------------+-----------+\n"
            + "| GOOG    | 2018-08-31 09:59:00  | 6330    | 1230.5     | 1230.02   |\n"
            + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |",
        4);
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

  private void testTrinoAfterSecondBatchAfterCompaction() throws Exception {
    Pair<String, String> stdOutErrPair = executeTrinoCommandFile(HDFS_TRINO_INPUT_BATCH2_PATH);
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
        "+------+-------------------+\n|GOOG  |2018-08-31 10:59:00|\n+------+-------------------+", 4);

    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 09:59:00|6330  |1230.5   |1230.02 |", 6);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|", 4);
    assertStdOutContains(stdOutErrPair,
        "+------+-------------------+\n|GOOG  |2018-08-31 10:29:00|\n+------+-------------------+", 2);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:29:00|3391  |1230.1899|1230.085|", 2);
  }

  private void testIncrementalHiveQuery(String minCommitTimeScript, String incrementalCommandsFile,
                                        String expectedOutput, int expectedTimes) throws Exception {
    String minCommitTime =
        executeCommandStringInDocker(ADHOC_2_CONTAINER, minCommitTimeScript, true).getStdout().toString();
    Pair<String, String> stdOutErrPair =
        executeHiveCommandFile(incrementalCommandsFile, "min.commit.time=" + minCommitTime + "`");
    assertStdOutContains(stdOutErrPair, expectedOutput, expectedTimes);
  }

  private void testIncrementalHiveQueryBeforeCompaction() throws Exception {
    String expectedOutput = "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |";

    // verify that 10:59 is present in COW table because there is no compaction process for COW
    testIncrementalHiveQuery(MIN_COMMIT_TIME_COW_SCRIPT, HIVE_INCREMENTAL_COW_COMMANDS, expectedOutput, 2);

    // verify that 10:59 is NOT present in RO table because of pending compaction
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RO_COMMANDS, expectedOutput, 0);

    // verify that 10:59 is present in RT table even with pending compaction
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RT_COMMANDS, expectedOutput, 2);
  }

  private void testIncrementalHiveQueryAfterCompaction() throws Exception {
    String expectedOutput = "| symbol  |          ts          | volume  |    open    |   close   |\n"
        + "+---------+----------------------+---------+------------+-----------+\n"
        + "| GOOG    | 2018-08-31 10:59:00  | 9021    | 1227.1993  | 1227.215  |";

    // verify that 10:59 is present for all views because compaction is complete
    testIncrementalHiveQuery(MIN_COMMIT_TIME_COW_SCRIPT, HIVE_INCREMENTAL_COW_COMMANDS, expectedOutput, 2);
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RO_COMMANDS, expectedOutput, 2);
    testIncrementalHiveQuery(MIN_COMMIT_TIME_MOR_SCRIPT, HIVE_INCREMENTAL_MOR_RT_COMMANDS, expectedOutput, 2);
  }

  private void testIncrementalSparkSQLQuery() throws Exception {
    Pair<String, String> stdOutErrPair = executeSparkSQLCommand(SPARKSQL_INCREMENTAL_COMMANDS, true);
    assertStdOutContains(stdOutErrPair, "|GOOG  |2018-08-31 10:59:00|9021  |1227.1993|1227.215|", 2);
    assertStdOutContains(stdOutErrPair, "|default |stock_ticks_cow              |false      |\n"
        + "|default |stock_ticks_cow_bs           |false      |\n"
        + "|default |stock_ticks_derived_mor_bs_ro|false      |\n"
        + "|default |stock_ticks_derived_mor_bs_rt|false      |\n"
        + "|default |stock_ticks_derived_mor_ro   |false      |\n"
        + "|default |stock_ticks_derived_mor_rt   |false      |\n"
        + "|default |stock_ticks_mor_bs_ro        |false      |\n"
        + "|default |stock_ticks_mor_bs_rt        |false      |"
        + "|default |stock_ticks_mor_ro           |false      |\n"
        + "|default |stock_ticks_mor_rt           |false      |");
    assertStdOutContains(stdOutErrPair, "|count(1)|\n+--------+\n|99     |", 4);
  }

  private void scheduleAndRunCompaction() throws Exception {
    executeCommandStringInDocker(ADHOC_1_CONTAINER, HUDI_CLI_TOOL + " --cmdfile " + COMPACTION_COMMANDS, true);
    executeCommandStringInDocker(ADHOC_1_CONTAINER, HUDI_CLI_TOOL + " --cmdfile " + COMPACTION_BOOTSTRAP_COMMANDS, true);
  }
}
