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
import org.apache.hudi.common.model.HoodieTableType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Smoke tests to run as part of verification.
 */
public class ITTestHoodieSanity extends ITTestBase {

  enum PartitionType {
    SINGLE_KEY_PARTITIONED, MULTI_KEYS_PARTITIONED, NON_PARTITIONED,
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie with single partition key data-set
   * and performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnSinglePartitionKeyCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_single_partition_key_cow_test";
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.COPY_ON_WRITE.name(), PartitionType.SINGLE_KEY_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie with multiple partition-keys
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count query
   * in hive console.
   */
  public void testRunHoodieJavaAppOnMultiPartitionKeysCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_multi_partition_key_cow_test";
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.COPY_ON_WRITE.name(), PartitionType.MULTI_KEYS_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample non-partitioned COW Hoodie data-set and
   * performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnNonPartitionedCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_non_partition_key_cow_test";
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.COPY_ON_WRITE.name(), PartitionType.NON_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.COPY_ON_WRITE.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample MOR Hoodie with single partition key data-set
   * and performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnSinglePartitionKeyMORTable() throws Exception {
    String hiveTableName = "docker_hoodie_single_partition_key_mor_test";
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.MERGE_ON_READ.name(), PartitionType.SINGLE_KEY_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.MERGE_ON_READ.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample MOR Hoodie with multiple partition-keys
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count query
   * in hive console.
   */
  public void testRunHoodieJavaAppOnMultiPartitionKeysMORTable() throws Exception {
    String hiveTableName = "docker_hoodie_multi_partition_key_mor_test";
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.MERGE_ON_READ.name(), PartitionType.MULTI_KEYS_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.MERGE_ON_READ.name());
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample non-partitioned MOR Hoodie data-set and
   * performs upserts on it. Hive integration and upsert functionality is checked by running a count query in hive
   * console.
   */
  public void testRunHoodieJavaAppOnNonPartitionedMORTable() throws Exception {
    String hiveTableName = "docker_hoodie_non_partition_key_mor_test";
    testRunHoodieJavaApp(hiveTableName, HoodieTableType.MERGE_ON_READ.name(), PartitionType.NON_PARTITIONED);
    dropHiveTables(hiveTableName, HoodieTableType.MERGE_ON_READ.name());
  }

  /**
   * A basic integration test that runs HoodieJavaApp to create a sample Hoodie data-set and performs upserts on it.
   * Hive integration and upsert functionality is checked by running a count query in hive console. TODO: Add
   * spark-shell test-case
   */
  public void testRunHoodieJavaApp(String hiveTableName, String tableType, PartitionType partitionType)
      throws Exception {

    String hdfsPath = "/" + hiveTableName;
    String hdfsUrl = "hdfs://namenode" + hdfsPath;

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
    Assert.assertTrue("Dropped table " + hiveTableName + " exists!", stdOutErr.getLeft().isEmpty());

    // Run Hoodie Java App
    String cmd;
    if (partitionType == PartitionType.SINGLE_KEY_PARTITIONED) {
      cmd = HOODIE_JAVA_APP + " --hive-sync --table-path " + hdfsUrl + " --hive-url " + HIVE_SERVER_JDBC_URL
          + " --table-type " + tableType + " --hive-table " + hiveTableName;
    } else if (partitionType == PartitionType.MULTI_KEYS_PARTITIONED) {
      cmd = HOODIE_JAVA_APP + " --hive-sync --table-path " + hdfsUrl + " --hive-url " + HIVE_SERVER_JDBC_URL
          + " --table-type " + tableType + " --hive-table " + hiveTableName + " --use-multi-partition-keys";
    } else {
      cmd = HOODIE_JAVA_APP + " --hive-sync --table-path " + hdfsUrl + " --hive-url " + HIVE_SERVER_JDBC_URL
          + " --table-type " + tableType + " --hive-table " + hiveTableName + " --non-partitioned";
    }
    executeCommandStringInDocker(ADHOC_1_CONTAINER, cmd, true);

    // Ensure table does exist
    stdOutErr = executeHiveCommand("show tables like '" + hiveTableName + "'");
    Assert.assertEquals("Table exists", hiveTableName, stdOutErr.getLeft());

    // Ensure row count is 80 (without duplicates) (100 - 20 deleted)
    stdOutErr = executeHiveCommand("select count(1) from " + hiveTableName);
    Assert.assertEquals("Expecting 100 rows to be present in the new table", 80,
        Integer.parseInt(stdOutErr.getLeft().trim()));

    // If is MOR table, ensure realtime table row count is 100 - 20 = 80 (without duplicates)
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      stdOutErr = executeHiveCommand("select count(1) from " + hiveTableName + "_rt");
      Assert.assertEquals("Expecting 100 rows to be present in the realtime table,", 80,
          Integer.parseInt(stdOutErr.getLeft().trim()));
    }

    // Make the HDFS dataset non-hoodie and run the same query
    // Checks for interoperability with non-hoodie tables

    // Delete Hoodie directory to make it non-hoodie dataset
    executeCommandStringInDocker(ADHOC_1_CONTAINER, "hdfs dfs -rm -r " + hdfsPath + "/.hoodie", true);

    // Run the count query again. Without Hoodie, all versions are included. So we get a wrong count
    stdOutErr = executeHiveCommand("select count(1) from " + hiveTableName);
    Assert.assertEquals("Expecting 280 rows to be present in the new table", 280,
        Integer.parseInt(stdOutErr.getLeft().trim()));
  }

  private void dropHiveTables(String hiveTableName, String tableType) throws Exception {
    executeHiveCommand("drop table if exists " + hiveTableName);
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      executeHiveCommand("drop table if exists " + hiveTableName + "_rt");
    }
  }
}
