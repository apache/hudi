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

import com.google.common.collect.ImmutableList;
import org.apache.hudi.common.util.collection.Pair;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Goes through steps described in https://hudi.incubator.apache.org/docker_demo.html
 * Tests only the hive query side
 */
public class ITTestHoodieDemo extends ITTestBase {

  private static String HDFS_DATA_DIR = "/usr/hive/data/input";
  private static String HDFS_BATCH_PATH1 = HDFS_DATA_DIR + "/" + "batch_1.json";
  private static String HDFS_BATCH_PATH2 = HDFS_DATA_DIR + "/" + "batch_2.json";

  private static String INPUT_BATCH_PATH1 = HOODIE_WS_ROOT +
      "/docker/demo/data/batch_1.json";
  private static String INPUT_BATCH_PATH2 = HOODIE_WS_ROOT +
      "/docker/demo/data/batch_2.json";

  private static String COW_BASE_PATH = "/user/hive/warehouse/stock_ticks_cow";
  private static String MOR_BASE_PATH = "/user/hive/warehouse/stock_ticks_mor";
  private static String COW_TABLE_NAME = "stock_ticks_cow";
  private static String MOR_TABLE_NAME = "stock_ticks_mor";

  private static String DEMO_CONTAINER_SCRIPT = "/var/hoodie/ws/docker/demo/setup_demo_container.sh";
  private static String HIVE_SYNC_TOOL = HOODIE_WS_ROOT + "/hudi-hive/run_sync_tool.sh";
  private static String HUDI_CLI_TOOL = HOODIE_WS_ROOT + "/hudi-cli/hudi-cli.sh";
  private static String COMPACTION_COMMANDS = HOODIE_WS_ROOT + "/docker/demo/compaction_commands";

  @Test
  public void testDemo() throws Exception {
    setupDemo();
    ingestFirstBatchAndHiveSync();
    testHiveAfterFirstBatch();
    ingestSecondBatch();
    testHiveAfterSecondBatch(false, true);
    testIncrementalHiveQuery();
    scheduleAndRunCompaction();
    testHiveAfterSecondBatch(true, false);
    testIncrementalHiveQuery();
  }

  private void setupDemo() throws Exception {
    List<String[]> cmds = new ImmutableList.Builder<String[]>()
        .add(new String[]{"hadoop", "fs", "-mkdir", "-p", HDFS_DATA_DIR})
        .add(new String[]{"hadoop", "fs", "-copyFromLocal", "-f", INPUT_BATCH_PATH1, HDFS_BATCH_PATH1})
        .add(new String[]{"/bin/bash", DEMO_CONTAINER_SCRIPT})
        .build();
    executeCommandsInDocker(ADHOC_1_CONTAINER, cmds);
  }

  private void ingestFirstBatchAndHiveSync() throws Exception {
    List<String[]> cmds = new ImmutableList.Builder<String[]>()
        .add(new String[]{"spark-submit", "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
            HUDI_UTILITIES_BUNDLE, "--storage-type", "COPY_ON_WRITE", "--source-class",
            "org.apache.hudi.utilities.sources.JsonDFSSource", "--source-ordering-field", "ts", "--target-base-path",
            COW_BASE_PATH, "--target-table", COW_TABLE_NAME, "--props", "/var/demo/config/dfs-source.properties",
            "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider"})
        .add(new String[]{"spark-submit", "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
            HUDI_UTILITIES_BUNDLE, "--storage-type", "MERGE_ON_READ", "--source-class",
            "org.apache.hudi.utilities.sources.JsonDFSSource", "--source-ordering-field", "ts", "--target-base-path",
            MOR_BASE_PATH, "--target-table", MOR_TABLE_NAME, "--props", "/var/demo/config/dfs-source.properties",
            "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
            "--disable-compaction"})
        .add(new String[]{HIVE_SYNC_TOOL, "--jdbc-url", "jdbc:hive2://hiveserver:10000", "--user", "hive",
            "--pass", "hive", "--partitioned-by", "dt", "--base-path", COW_BASE_PATH, "--database", "default",
            "--table", COW_TABLE_NAME})
        .add(new String[]{HIVE_SYNC_TOOL, "--jdbc-url", "jdbc:hive2://hiveserver:10000", "--user", "hive",
            "--pass", "hive", "--partitioned-by", "dt", "--base-path", MOR_BASE_PATH, "--database", "default",
            "--table", MOR_TABLE_NAME})
        .build();

    executeCommandsInDocker(ADHOC_1_CONTAINER, cmds);
  }

  private void testHiveAfterFirstBatch() throws Exception {
    Pair<String, String> res = executeHiveCommand("show tables", true);
    Assert.assertTrue(res.getLeft().contains("stock_ticks_cow"));
    Assert.assertTrue(res.getLeft().contains("stock_ticks_mor"));
    Assert.assertTrue(res.getLeft().contains("stock_ticks_mor_rt"));
    executeHiveCommandWithExpectedOutput("show partitions stock_ticks_cow", "dt=2018-08-31");
    executeHiveCommandWithExpectedOutput("show partitions stock_ticks_mor", "dt=2018-08-31");
    executeHiveCommandWithExpectedOutput("show partitions stock_ticks_mor_rt", "dt=2018-08-31");
    executeHiveCommandWithExpectedOutput(
        "select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'",
        "GOOG\t2018-08-31 10:29:00");
    executeHiveCommandWithExpectedOutput(
        "select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG'",
        "GOOG\t2018-08-31 10:29:00");
    executeHiveCommandWithExpectedOutput(
        "select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'",
        "GOOG\t2018-08-31 10:29:00");
    executeHiveCommandWithExpectedOutput(
        "select symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'",
        "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
            + "GOOG\t2018-08-31 10:29:00\t3391\t1230.1899\t1230.085");
    executeHiveCommandWithExpectedOutput(
        "select symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG'",
        "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
            + "GOOG\t2018-08-31 10:29:00\t3391\t1230.1899\t1230.085");
    executeHiveCommandWithExpectedOutput(
        "select symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'",
        "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
            + "GOOG\t2018-08-31 10:29:00\t3391\t1230.1899\t1230.085");
  }

  private void ingestSecondBatch() throws Exception {
    List<String[]> cmds = new ImmutableList.Builder<String[]>()
        .add(new String[]{"hadoop", "fs", "-copyFromLocal", "-f", INPUT_BATCH_PATH2, HDFS_BATCH_PATH2})
        .add(new String[]{"spark-submit", "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
            HUDI_UTILITIES_BUNDLE, "--storage-type", "COPY_ON_WRITE", "--source-class",
            "org.apache.hudi.utilities.sources.JsonDFSSource", "--source-ordering-field", "ts", "--target-base-path",
            COW_BASE_PATH, "--target-table", COW_TABLE_NAME, "--props", "/var/demo/config/dfs-source.properties",
            "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider"})
        .add(new String[]{"spark-submit", "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
            HUDI_UTILITIES_BUNDLE, "--storage-type", "MERGE_ON_READ", "--source-class",
            "org.apache.hudi.utilities.sources.JsonDFSSource", "--source-ordering-field", "ts", "--target-base-path",
            MOR_BASE_PATH, "--target-table", MOR_TABLE_NAME, "--props", "/var/demo/config/dfs-source.properties",
            "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
            "--disable-compaction"})
        .build();
    executeCommandsInDocker(ADHOC_1_CONTAINER, cmds);
  }

  private void testHiveAfterSecondBatch(boolean hasCompactionRan, boolean testCOWTable) throws Exception {

    if (testCOWTable) {
      executeHiveCommandWithExpectedOutput(
          "select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG'",
          "GOOG\t2018-08-31 10:59:00");
    }

    if (hasCompactionRan) {
      executeHiveCommandWithExpectedOutput(
          "select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG'",
          "GOOG\t2018-08-31 10:59:00");
    } else {
      executeHiveCommandWithExpectedOutput(
          "select symbol, max(ts) from stock_ticks_mor group by symbol HAVING symbol = 'GOOG'",
          "GOOG\t2018-08-31 10:29:00");
    }

    executeHiveCommandWithExpectedOutput(
        "select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG'",
        "GOOG\t2018-08-31 10:59:00");

    if (testCOWTable) {
      executeHiveCommandWithExpectedOutput(
          "select symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG'",
          "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
              + "GOOG\t2018-08-31 10:59:00\t9021\t1227.1993\t1227.215");
    }

    if (hasCompactionRan) {
      executeHiveCommandWithExpectedOutput(
          "select symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG'",
          "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
              + "GOOG\t2018-08-31 10:59:00\t9021\t1227.1993\t1227.215");
    } else {
      executeHiveCommandWithExpectedOutput(
          "select symbol, ts, volume, open, close  from stock_ticks_mor where  symbol = 'GOOG'",
          "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
              + "GOOG\t2018-08-31 10:29:00\t3391\t1230.1899\t1230.085");
    }
    executeHiveCommandWithExpectedOutput(
        "select symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG'",
        "GOOG\t2018-08-31 09:59:00\t6330\t1230.5\t1230.02\n"
            + "GOOG\t2018-08-31 10:59:00\t9021\t1227.1993\t1227.215");
  }

  private void testIncrementalHiveQuery() throws Exception {
    Pair<String, String> stdoutErr = executeHiveCommand("select min(`_hoodie_commit_time`) from stock_ticks_cow", true);
    String minInstantTimeForCOW = stdoutErr.getLeft();
    executeHiveCommandWithExpectedOutput("set hoodie.stock_ticks_cow.consume.mode=INCREMENTAL; "
            + "set hoodie.stock_ticks_cow.consume.max.commits=3; set hoodie.stock_ticks_cow.consume.start.timestamp="
            + minInstantTimeForCOW
            + "; select symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG' "
            + "and `_hoodie_commit_time` > '" + minInstantTimeForCOW + "';",
        "GOOG\t2018-08-31 10:59:00\t9021\t1227.1993\t1227.215");
  }

  private void scheduleAndRunCompaction() throws Exception {
    List<String[]> cmds = new ImmutableList.Builder<String[]>()
        .add(new String[]{HUDI_CLI_TOOL, "--cmdfile", COMPACTION_COMMANDS})
        .build();
    executeCommandsInDocker(ADHOC_1_CONTAINER, cmds);
  }
}