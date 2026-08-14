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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.collection.Pair;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class to run cmd and generate data in hive.
 */
@Slf4j
public class HoodieTestHiveBase extends ITTestBase {

  protected enum PartitionType {
    SINGLE_KEY_PARTITIONED, MULTI_KEYS_PARTITIONED, NON_PARTITIONED,
  }

  private static final int DEFAULT_TIME_WAIT = 5000;
  private static final String OVERWRITE_COMMIT_TYPE = "overwrite";

  /**
   * A basic integration test that runs HoodieJavaApp to create a sample Hoodie data-set and performs upserts on it.
   * Hive integration and upsert functionality is checked by running a count query in hive console. TODO: Add
   * spark-shell test-case
   */
  public void generateDataByHoodieJavaApp(String hiveTableName, String tableType, PartitionType partitionType,
      String commitType, String hoodieTableName) throws Exception {

    String hdfsPath = getHDFSPath(hiveTableName);
    String hdfsUrl = "hdfs://namenode" + hdfsPath;

    Pair<String, String> stdOutErr;
    if (OVERWRITE_COMMIT_TYPE.equals(commitType)) {
      // Drop Table if it exists
      try {
        dropHiveTables(hiveTableName, tableType);
      } catch (AssertionError ex) {
        // In travis, sometimes, the hivemetastore is not ready even though we wait for the port to be up
        // Workaround to sleep for 5 secs and retry
        // Set sleep time by hoodie.hiveserver.time.wait
        Thread.sleep(getTimeWait());
        dropHiveTables(hiveTableName, tableType);
      }

      // Ensure table does not exist
      stdOutErr = executeHiveCommand("show tables like '" + hiveTableName + "'");
      if (!stdOutErr.getLeft().isEmpty()) {
        throw new IllegalStateException("Dropped table " + hiveTableName + " exists!");
      }
    }

    // Run Hoodie Java App
    String cmd = String.format("%s --hive-sync --table-path %s  --hive-url %s  --table-type %s  --hive-table %s"
             + " --commit-type %s  --table-name %s", HOODIE_GENERATE_APP, hdfsUrl, HIVE_SERVER_JDBC_URL,
        tableType, hiveTableName, commitType, hoodieTableName);
    if (partitionType == PartitionType.MULTI_KEYS_PARTITIONED) {
      cmd = cmd + " --use-multi-partition-keys";
    } else if (partitionType == PartitionType.NON_PARTITIONED) {
      cmd = cmd + " --non-partitioned";
    }
    executeCommandStringInDocker(ADHOC_1_CONTAINER, cmd, true);

    String snapshotTableName = getSnapshotTableName(tableType, hiveTableName);

    // Ensure table does exist
    stdOutErr = executeHiveCommand("show tables like '" + snapshotTableName + "'");
    assertEquals(snapshotTableName, stdOutErr.getLeft(), "Table exists");
  }

  protected void dropHiveTables(String hiveTableName, String tableType) throws Exception {
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      executeHiveCommand("drop table if exists " + hiveTableName + "_rt");
      executeHiveCommand("drop table if exists " + hiveTableName + "_ro");
    } else {
      executeHiveCommand("drop table if exists " + hiveTableName);
    }
  }

  protected String getHDFSPath(String hiveTableName) {
    return "/" + hiveTableName;
  }

  protected String getSnapshotTableName(String tableType, String hiveTableName) {
    return tableType.equals(HoodieTableType.MERGE_ON_READ.name())
        ? hiveTableName + "_rt" : hiveTableName;
  }

  private int getTimeWait() {
    try (InputStream stream = HoodieTestHiveBase.class.getClassLoader().getResourceAsStream("hoodie-docker.properties")) {
      TypedProperties properties = new TypedProperties();
      properties.load(stream);
      return properties.getInteger("hoodie.hiveserver.time.wait", DEFAULT_TIME_WAIT);
    } catch (IOException e) {
      log.warn("Can not load property file, use default time wait for hiveserver.");
      return DEFAULT_TIME_WAIT;
    }
  }
}
