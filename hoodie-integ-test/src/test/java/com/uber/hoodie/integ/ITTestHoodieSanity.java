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

package com.uber.hoodie.integ;

import java.util.Arrays;
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
  public void testRunEcho() throws Exception {
    String[] cmd = new String[]{"echo", "Happy Testing"};
    TestExecStartResultCallback callback = executeCommandInDocker(ADHOC_1_CONTAINER,
        cmd, true);
    String stdout = callback.getStdout().toString();
    String stderr = callback.getStderr().toString();
    LOG.info("Got output for (" + Arrays.toString(cmd) + ") :" + stdout);
    LOG.info("Got error output for (" + Arrays.toString(cmd) + ") :" + stderr);
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
    {
      String[] hiveDropCmd = getHiveConsoleCommand("drop table if exists " + hiveTableName);
      executeCommandInDocker(HIVESERVER, hiveDropCmd, true);
    }

    // Ensure table does not exist
    {
      String[] hiveTableCheck = getHiveConsoleCommand("show tables like '" + hiveTableName + "'");
      TestExecStartResultCallback callback =
          executeCommandInDocker(HIVESERVER, hiveTableCheck, true);
      String stderr = callback.getStderr().toString();
      String stdout = callback.getStdout().toString();
      LOG.info("Got output for (" + Arrays.toString(hiveTableCheck) + ") :" + stdout);
      LOG.info("Got error output for (" + Arrays.toString(hiveTableCheck) + ") :" + stderr);
      Assert.assertTrue("Result :" + callback.getStdout().toString(), stdout.trim().isEmpty());
    }

    // Run Hoodie Java App
    {
      String[] cmd = null;
      if (partitionType == PartitionType.SINGLE_KEY_PARTITIONED) {
        cmd = new String[]{
            HOODIE_JAVA_APP,
            "--hive-sync",
            "--table-path", hdfsUrl,
            "--hive-url", HIVE_SERVER_JDBC_URL,
            "--hive-table", hiveTableName
        };
      } else if (partitionType == PartitionType.MULTI_KEYS_PARTITIONED) {
        cmd = new String[]{
            HOODIE_JAVA_APP,
            "--hive-sync",
            "--table-path", hdfsUrl,
            "--hive-url", HIVE_SERVER_JDBC_URL,
            "--use-multi-partition-keys",
            "--hive-table", hiveTableName
        };
      } else {
        cmd = new String[]{
            HOODIE_JAVA_APP,
            "--hive-sync",
            "--table-path", hdfsUrl,
            "--hive-url", HIVE_SERVER_JDBC_URL,
            "--non-partitioned",
            "--hive-table", hiveTableName
        };
      }
      TestExecStartResultCallback callback = executeCommandInDocker(ADHOC_1_CONTAINER,
          cmd, true);
      String stdout = callback.getStdout().toString().trim();
      String stderr = callback.getStderr().toString().trim();
      LOG.info("Got output for (" + Arrays.toString(cmd) + ") :" + stdout);
      LOG.info("Got error output for (" + Arrays.toString(cmd) + ") :" + stderr);
    }

    // Ensure table does exist
    {
      String[] hiveTableCheck = getHiveConsoleCommand("show tables like '" + hiveTableName + "'");
      TestExecStartResultCallback callback =
          executeCommandInDocker(HIVESERVER, hiveTableCheck, true);
      String stderr = callback.getStderr().toString().trim();
      String stdout = callback.getStdout().toString().trim();
      LOG.info("Got output for (" + Arrays.toString(hiveTableCheck) + ") : (" + stdout + ")");
      LOG.info("Got error output for (" + Arrays.toString(hiveTableCheck) + ") : (" + stderr + ")");
      Assert.assertEquals("Table exists", hiveTableName, stdout);
    }

    // Ensure row count is 100 (without duplicates)
    {
      String[] hiveTableCheck = getHiveConsoleCommand("select count(1) from " + hiveTableName);
      TestExecStartResultCallback callback =
          executeCommandInDocker(ADHOC_1_CONTAINER, hiveTableCheck, true);
      String stderr = callback.getStderr().toString().trim();
      String stdout = callback.getStdout().toString().trim();
      LOG.info("Got output for (" + Arrays.toString(hiveTableCheck) + ") : (" + stdout + ")");
      LOG.info("Got error output for (" + Arrays.toString(hiveTableCheck) + ") : (" + stderr + ")");
      Assert.assertEquals("Expecting 100 rows to be present in the new table", 100,
          Integer.parseInt(stdout.trim()));
    }

    // Make the HDFS dataset non-hoodie and run the same query
    // Checks for interoperability with non-hoodie tables
    {
      // Delete Hoodie directory to make it non-hoodie dataset
      String[] cmd = new String[]{
          "hadoop", "fs", "-rm", "-r", hdfsPath + "/.hoodie"
      };
      TestExecStartResultCallback callback =
          executeCommandInDocker(ADHOC_1_CONTAINER, cmd, true);
      String stderr = callback.getStderr().toString().trim();
      String stdout = callback.getStdout().toString().trim();
      LOG.info("Got output for (" + Arrays.toString(cmd) + ") : (" + stdout + ")");
      LOG.info("Got error output for (" + Arrays.toString(cmd) + ") : (" + stderr + ")");

      // Run the count query again. Without Hoodie, all versions are included. So we get a wrong count
      String[] hiveTableCheck = getHiveConsoleCommand("select count(1) from " + hiveTableName);
      callback = executeCommandInDocker(ADHOC_1_CONTAINER, hiveTableCheck, true);
      stderr = callback.getStderr().toString().trim();
      stdout = callback.getStdout().toString().trim();
      LOG.info("Got output for (" + Arrays.toString(hiveTableCheck) + ") : (" + stdout + ")");
      LOG.info("Got error output for (" + Arrays.toString(hiveTableCheck) + ") : (" + stderr + ")");
      Assert.assertEquals("Expecting 200 rows to be present in the new table", 200,
          Integer.parseInt(stdout.trim()));
    }
  }
}
