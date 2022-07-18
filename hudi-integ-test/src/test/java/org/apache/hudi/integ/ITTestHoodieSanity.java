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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests to run as part of verification.
 */

@Disabled
public class ITTestHoodieSanity extends ITTestBase {

  private static final String HDFS_BASE_URL =  "hdfs://namenode";
  private static final String HDFS_STREAMING_SOURCE =  HDFS_BASE_URL + "/streaming/source/";
  private static final String HDFS_STREAMING_CKPT =  HDFS_BASE_URL + "/streaming/ckpt/";

  enum PartitionType {
    SINGLE_KEY_PARTITIONED, MULTI_KEYS_PARTITIONED, NON_PARTITIONED,
  }

  @Disabled
  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie with single partition key data-set
   * and performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnSinglePartitionKeyCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_single_partition_key_cow_test_" + HoodieActiveTimeline.createNewInstantTime();
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.COPY_ON_WRITE.name(),
        PartitionType.SINGLE_KEY_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
  }

  @Disabled
  @ParameterizedTest
  @ValueSource(strings = { HOODIE_JAVA_APP, HOODIE_JAVA_STREAMING_APP })
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie with multiple partition-keys
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count query
   * in hive console.
   */
  public void testRunHoodieJavaAppOnMultiPartitionKeysCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_multi_partition_key_cow_test_" + HoodieActiveTimeline.createNewInstantTime();
    testRunHoodieJavaApp(HOODIE_JAVA_APP, hiveTableName, HoodieTableType.COPY_ON_WRITE.name(),
        PartitionType.MULTI_KEYS_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample non-partitioned COW Hoodie data-set and
   * performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnNonPartitionedCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_non_partition_key_cow_test_" + HoodieActiveTimeline.createNewInstantTime();
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.COPY_ON_WRITE.name(), PartitionType.NON_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
  }

  @Disabled
  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample MOR Hoodie with single partition key data-set
   * and performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnSinglePartitionKeyMORTable() throws Exception {
    String hiveTableName = "docker_hoodie_single_partition_key_mor_test_" + HoodieActiveTimeline.createNewInstantTime();
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.MERGE_ON_READ.name(),
        PartitionType.SINGLE_KEY_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.MERGE_ON_READ.name());
  }

  @Disabled
  @ParameterizedTest
  @ValueSource(strings = { HOODIE_JAVA_APP, HOODIE_JAVA_STREAMING_APP })
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample MOR Hoodie with multiple partition-keys
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count query
   * in hive console.
   */
  public void testRunHoodieJavaAppOnMultiPartitionKeysMORTable(String command) throws Exception {
    String hiveTableName = "docker_hoodie_multi_partition_key_mor_test_" + HoodieActiveTimeline.createNewInstantTime();
    testRunHoodieJavaApp(command, hiveTableName, HoodieTableType.MERGE_ON_READ.name(),
        PartitionType.MULTI_KEYS_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.MERGE_ON_READ.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample non-partitioned MOR Hoodie data-set and
   * performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnNonPartitionedMORTable() throws Exception {
    String hiveTableName = "docker_hoodie_non_partition_key_mor_test_" + HoodieActiveTimeline.createNewInstantTime();
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.MERGE_ON_READ.name(), PartitionType.NON_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.MERGE_ON_READ.name());
  }

  /**
   * A basic integration test that runs HoodieJavaApp to create a sample Hoodie data-set and performs upserts on it.
   * Hive integration and upsert functionality is checked by running a count query in hive console. TODO: Add
   * spark-shell test-case
   */
  public void testRunHoodieJavaApp(String command, String hiveTableName, String tableType, PartitionType partitionType)
      throws Exception {

    String hdfsPath = "/" + hiveTableName;
    String hdfsUrl = HDFS_BASE_URL + hdfsPath;

    // Delete hdfs path if it exists
    try {
      executeCommandStringInDocker(ADHOC_1_CONTAINER, "hdfs dfs -rm -r " + hdfsUrl, true);
    } catch (AssertionError ex) {
      // Path not exists, pass
    }

    // Drop Table if it exists
    try {
      dropHiveTables(hiveTableName, tableType);
    } catch (AssertionError ex) {
      // In travis, sometimes, the hivemetastore is not ready even though we wait for the port to be up
      // Workaround to sleep for 5 secs and retry
      Thread.sleep(5000);
      dropHiveTables(hiveTableName, tableType);
    }

    // Ensure table does not exist
    Pair<String, String> stdOutErr = executeHiveCommand("show tables like '" + hiveTableName + "'");
    assertTrue(stdOutErr.getLeft().isEmpty(), "Dropped table " + hiveTableName + " exists!");

    // Run Hoodie Java App
    String cmd;
    if (partitionType == PartitionType.SINGLE_KEY_PARTITIONED) {
      cmd = command + " --hive-sync --table-path " + hdfsUrl + " --hive-url " + HIVE_SERVER_JDBC_URL
          + " --table-type " + tableType + " --hive-table " + hiveTableName;
    } else if (partitionType == PartitionType.MULTI_KEYS_PARTITIONED) {
      cmd = command + " --hive-sync --table-path " + hdfsUrl + " --hive-url " + HIVE_SERVER_JDBC_URL
          + " --table-type " + tableType + " --hive-table " + hiveTableName + " --use-multi-partition-keys";
    } else {
      cmd = command + " --hive-sync --table-path " + hdfsUrl + " --hive-url " + HIVE_SERVER_JDBC_URL
          + " --table-type " + tableType + " --hive-table " + hiveTableName + " --non-partitioned";
    }

    if (command.equals(HOODIE_JAVA_STREAMING_APP)) {
      String streamingSourcePath = HDFS_STREAMING_SOURCE + hiveTableName;
      String streamingCkptPath = HDFS_STREAMING_CKPT + hiveTableName;
      cmd = cmd + " --streaming-source-path " +  streamingSourcePath
          + " --streaming-checkpointing-path " + streamingCkptPath;
    }
    executeCommandStringInDocker(ADHOC_1_CONTAINER, cmd, true);

    String snapshotTableName = tableType.equals(HoodieTableType.MERGE_ON_READ.name())
        ? hiveTableName + "_rt" : hiveTableName;
    Option<String> roTableName = tableType.equals(HoodieTableType.MERGE_ON_READ.name())
        ? Option.of(hiveTableName + "_ro") : Option.empty();

    // Ensure table does exist
    stdOutErr = executeHiveCommand("show tables like '" + snapshotTableName + "'");
    assertEquals(snapshotTableName, stdOutErr.getLeft(), "Table exists");

    // Ensure row count is 80 (without duplicates) (100 - 20 deleted)
    stdOutErr = executeHiveCommand("select count(1) from " + snapshotTableName);
    assertEquals(80, Integer.parseInt(stdOutErr.getLeft().trim()),
        "Expecting 80 rows to be present in the snapshot table");

    if (roTableName.isPresent()) {
      stdOutErr = executeHiveCommand("select count(1) from " + roTableName.get());
      assertEquals(80, Integer.parseInt(stdOutErr.getLeft().trim()),
          "Expecting 80 rows to be present in the snapshot table");
    }

    // Make the HDFS dataset non-hoodie and run the same query; Checks for interoperability with non-hoodie tables
    // Delete Hoodie directory to make it non-hoodie dataset
    executeCommandStringInDocker(ADHOC_1_CONTAINER, "hdfs dfs -rm -r " + hdfsPath + "/.hoodie", true);

    // Run the count query again. Without Hoodie, all versions are included. So we get a wrong count
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      stdOutErr = executeHiveCommand("select count(1) from " + roTableName.get());
    } else {
      stdOutErr = executeHiveCommand("select count(1) from " + snapshotTableName);
    }
    assertEquals(280, Integer.parseInt(stdOutErr.getLeft().trim()),
        "Expecting 280 rows to be present in the new table");
  }

  public void testRunHoodieJavaApp(String hiveTableName, String tableType, PartitionType partitionType)
      throws Exception {
    testRunHoodieJavaApp(HOODIE_JAVA_APP, hiveTableName, tableType, partitionType);
  }

  private void dropHiveTables(String hiveTableName, String tableType) throws Exception {
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      executeHiveCommand("drop table if exists " + hiveTableName + "_rt");
      executeHiveCommand("drop table if exists " + hiveTableName + "_ro");
    } else {
      executeHiveCommand("drop table if exists " + hiveTableName);
    }
  }
}
