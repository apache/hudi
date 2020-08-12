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

package org.apache.hudi.hive.client;

import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HoodieHiveDriverClient extends HoodieHiveClient {

  // Make sure we have the hive JDBC driver in classpath
  private static String driverName = HiveDriver.class.getName();

  static {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + driverName + " in classpath. ", e);
    }
  }

  private static final Logger LOG = LogManager.getLogger(HoodieHiveDriverClient.class);

  public HoodieHiveDriverClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg, configuration, fs);
    LOG.info("Creating hive connection with Hive Driver");
  }

  /**
   * Add the (NEW) partitions to the table.
   */
  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    String sql = HiveSchemaUtil.generateAddPartitionsDDL(tableName, partitionsToAdd, syncConfig, partitionValueExtractor);
    runHiveSQLUsingHiveDriver(sql);
  }

  /**
   * Partition path has changed - update the path for te following partitions.
   */
  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    List<String> sqls = HiveSchemaUtil.generateChangePartitionsDDLs(tableName, changedPartitions, syncConfig, fs, partitionValueExtractor);
    for (String sql : sqls) {
      runHiveSQLUsingHiveDriver(sql);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      String sql = HiveSchemaUtil.generateUpdateTableDefinitionDDL(tableName, newSchema, syncConfig);
      runHiveSQLUsingHiveDriver(sql);
    } catch (IOException e) {
      throw new HoodieHiveSyncException(String.format("Failed to update table for %s with Hive Driver", tableName), e);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass) {
    try {
      String createSQLQuery =
          HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, syncConfig, inputFormatClass,
              outputFormatClass, serdeClass);
      LOG.info("Creating table with " + createSQLQuery);
      runHiveSQLUsingHiveDriver(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException(String.format("Failed to create table %s with Hive Driver", tableName), e);
    }
  }

  /**
   * Execute a update in hive using Hive Driver.
   *
   * @param sql SQL statement to execute
   */
  private CommandProcessorResponse runHiveSQLUsingHiveDriver(String sql) {
    List<CommandProcessorResponse> responses = updateHiveSQLs(Collections.singletonList(sql));
    return responses.get(responses.size() - 1);
  }

  private List<CommandProcessorResponse> updateHiveSQLs(List<String> sqls) {
    SessionState ss = null;
    org.apache.hadoop.hive.ql.Driver hiveDriver = null;
    List<CommandProcessorResponse> responses = new ArrayList<>();
    try {
      final long startTime = System.currentTimeMillis();
      ss = SessionState.start(configuration);
      ss.setCurrentDatabase(syncConfig.databaseName);
      hiveDriver = new org.apache.hadoop.hive.ql.Driver(configuration);
      final long endTime = System.currentTimeMillis();
      LOG.info(String.format("Time taken to start SessionState and create Driver: %s ms", (endTime - startTime)));
      for (String sql : sqls) {
        final long start = System.currentTimeMillis();
        responses.add(hiveDriver.run(sql));
        final long end = System.currentTimeMillis();
        LOG.info(String.format("Time taken to execute [%s]: %s ms", sql, (end - start)));
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL", e);
    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException ie) {
          LOG.error("Error while closing SessionState", ie);
        }
      }
      if (hiveDriver != null) {
        try {
          hiveDriver.close();
        } catch (Exception e) {
          LOG.error("Error while closing hiveDriver", e);
        }
      }
    }
    return responses;
  }
}