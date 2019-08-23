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
import org.junit.Assert;
import org.junit.Test;

/**
 * Smoke tests to run as part of verification.
 */
public class ITTestHoodieSanity extends ITTestBase {

  enum PartitionType {
    SINGLE_KEY_PARTITIONED,
    MULTI_KEYS_PARTITIONED,
    NON_PARTITIONED,
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie with single partition key
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count
   * query in hive console.
   */
  public void testRunHoodieJavaAppOnSinglePartitionKeyCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_single_partition_key_cow_test";
    testRunHoodieJavaAppOnCOWTable(hiveTableName, PartitionType.SINGLE_KEY_PARTITIONED);
    executeHiveCommand("drop table if exists " + hiveTableName);
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie with multiple partition-keys
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count
   * query in hive console.
   */
  public void testRunHoodieJavaAppOnMultiPartitionKeysCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_multi_partition_key_cow_test";
    testRunHoodieJavaAppOnCOWTable(hiveTableName, PartitionType.MULTI_KEYS_PARTITIONED);
    executeHiveCommand("drop table if exists " + hiveTableName);
  }

  @Test
  /**
   * A basic integration test that runs HoodieJavaApp to create a sample non-partitioned COW Hoodie
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count
   * query in hive console.
   */
  public void testRunHoodieJavaAppOnNonPartitionedCOWTable() throws Exception {
    String hiveTableName = "docker_hoodie_non_partition_key_cow_test";
    testRunHoodieJavaAppOnCOWTable(hiveTableName, PartitionType.NON_PARTITIONED);
    executeHiveCommand("drop table if exists " + hiveTableName);
  }

  /**
   * A basic integration test that runs HoodieJavaApp to create a sample COW Hoodie
   * data-set and performs upserts on it. Hive integration and upsert functionality is checked by running a count
   * query in hive console.
   * TODO: Add spark-shell test-case
   */
  public void testRunHoodieJavaAppOnCOWTable(String hiveTableName, PartitionType partitionType) throws Exception {

    String hdfsPath = "/" + hiveTableName;
    String hdfsUrl = "hdfs://namenode" + hdfsPath;

    // Drop Table if it exists
    String hiveDropCmd = "drop table if exists " + hiveTableName;
    try {
      executeHiveCommand(hiveDropCmd);
    } catch (AssertionError ex) {
      // In travis, sometimes, the hivemetastore is not ready even though we wait for the port to be up
      // Workaround to sleep for 5 secs and retry
      Thread.sleep(5000);
      executeHiveCommand(hiveDropCmd);
    }

    // Ensure table does not exist
    Pair<String, String> stdOutErr = executeHiveCommand("show tables like '" + hiveTableName + "'");
    Assert.assertTrue("Dropped table " + hiveTableName + " exists!", stdOutErr.getLeft().isEmpty());

    // Run Hoodie Java App
    String cmd;
    if (partitionType == PartitionType.SINGLE_KEY_PARTITIONED) {
      cmd = HOODIE_JAVA_APP + " --hive-sync --table-path " + hdfsUrl
          + " --hive-url " + HIVE_SERVER_JDBC_URL + " --hive-table " + hiveTableName;
    } else if (partitionType == PartitionType.MULTI_KEYS_PARTITIONED) {
      cmd = HOODIE_JAVA_APP + " --hive-sync --table-path " + hdfsUrl
          + " --hive-url " + HIVE_SERVER_JDBC_URL + " --hive-table " + hiveTableName
          + " --use-multi-partition-keys";
    } else {
      cmd = HOODIE_JAVA_APP + " --hive-sync --table-path " + hdfsUrl
          + " --hive-url " + HIVE_SERVER_JDBC_URL + " --hive-table " + hiveTableName
          + " --non-partitioned";
    }
    executeCommandStringInDocker(ADHOC_1_CONTAINER, cmd, true);

    // Ensure table does exist
    stdOutErr = executeHiveCommand("show tables like '" + hiveTableName + "'");
    Assert.assertEquals("Table exists", hiveTableName, stdOutErr.getLeft());

    // Ensure row count is 100 (without duplicates)
    stdOutErr = executeHiveCommand("select count(1) from " + hiveTableName);
    Assert.assertEquals("Expecting 100 rows to be present in the new table", 100,
        Integer.parseInt(stdOutErr.getLeft().trim()));

    // Make the HDFS dataset non-hoodie and run the same query
    // Checks for interoperability with non-hoodie tables

    // Delete Hoodie directory to make it non-hoodie dataset
    executeCommandStringInDocker(ADHOC_1_CONTAINER, "hdfs dfs -rm -r " + hdfsPath + "/.hoodie", true);

    // Run the count query again. Without Hoodie, all versions are included. So we get a wrong count
    stdOutErr = executeHiveCommand("select count(1) from " + hiveTableName);
    Assert.assertEquals("Expecting 200 rows to be present in the new table", 200,
        Integer.parseInt(stdOutErr.getLeft().trim()));
  }
}
