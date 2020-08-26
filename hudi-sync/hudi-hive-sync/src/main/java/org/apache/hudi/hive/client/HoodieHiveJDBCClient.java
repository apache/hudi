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
import org.apache.hive.jdbc.HiveDriver;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HoodieHiveJDBCClient extends HoodieHiveClient {

  // Make sure we have the hive JDBC driver in classpath

  private static final Logger LOG = LogManager.getLogger(HoodieHiveJDBCClient.class);
  // private Connection connection;

  public HoodieHiveJDBCClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg, configuration, fs);

    // Support both JDBC and metastore based implementations for backwards compatiblity. Future users should
    // disable jdbc and depend on metastore client for all hive registrations
    LOG.info("Creating hive connection " + cfg.jdbcUrl);
    // createHiveConnection();
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
    updateHiveSQLUsingJDBC(sql);
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
      updateHiveSQLUsingJDBC(sql);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      String sql = HiveSchemaUtil.generateUpdateTableDefinitionDDL(tableName, newSchema, syncConfig);
      updateHiveSQLUsingJDBC(sql);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass) {
    try {
      String createSQLQuery =
          HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, syncConfig, inputFormatClass,
              outputFormatClass, serdeClass);
      LOG.info("Creating table with " + createSQLQuery);
      updateHiveSQLUsingJDBC(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  /**
   * Get the table schema.
   */
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    if (!doesTableExist(tableName)) {
      throw new IllegalArgumentException(
          "Failed to get schema for table " + tableName + " does not exist");
    }
    Map<String, String> schema = new HashMap<>();
    ResultSet result = null;
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      result = databaseMetaData.getColumns(null, syncConfig.databaseName, tableName, null);
      while (result.next()) {
        String columnName = result.getString(4);
        String columnType = result.getString(6);
        if ("DECIMAL".equals(columnType)) {
          int columnSize = result.getInt("COLUMN_SIZE");
          int decimalDigits = result.getInt("DECIMAL_DIGITS");
          columnType += String.format("(%s,%s)", columnSize, decimalDigits);
        }
        schema.put(columnName, columnType);
      }
      return schema;
    } catch (SQLException e) {
      throw new HoodieHiveSyncException("Failed to get table schema for " + tableName, e);
    } finally {
      closeQuietly(result, null);
    }
  }

  /**
   * Execute a update in hive metastore with this SQL.
   *
   * @param s SQL to execute
   */
  public void updateHiveSQLUsingJDBC(String s) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      LOG.info("Executing SQL " + s);
      stmt.execute(s);
    } catch (SQLException e) {
      throw new HoodieHiveSyncException("Failed in executing SQL " + s, e);
    } finally {
      closeQuietly(null, stmt);
    }
  }

  private void createHiveConnection() {
    if (connection == null) {
      try {
        Class.forName(HiveDriver.class.getCanonicalName());
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load Hive driver class", e);
        return;
      }

      try {
        this.connection = DriverManager.getConnection(syncConfig.jdbcUrl, syncConfig.hiveUser, syncConfig.hivePass);
        LOG.info("Successfully established Hive connection to  " + syncConfig.jdbcUrl);
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Cannot create hive connection " + getHiveJdbcUrlWithDefaultDBName(), e);
      }
    }
  }

  private String getHiveJdbcUrlWithDefaultDBName() {
    String hiveJdbcUrl = syncConfig.jdbcUrl;
    String urlAppend = null;
    // If the hive url contains addition properties like ;transportMode=http;httpPath=hs2
    if (hiveJdbcUrl.contains(";")) {
      urlAppend = hiveJdbcUrl.substring(hiveJdbcUrl.indexOf(";"));
      hiveJdbcUrl = hiveJdbcUrl.substring(0, hiveJdbcUrl.indexOf(";"));
    }
    if (!hiveJdbcUrl.endsWith("/")) {
      hiveJdbcUrl = hiveJdbcUrl + "/";
    }
    return hiveJdbcUrl + (urlAppend == null ? "" : urlAppend);
  }

  @Override
  public void createHiveDatabase(String dbName) {
    LOG.info("Creating database " + dbName);
    updateHiveSQLUsingJDBC("CREATE DATABASE IF NOT EXISTS " + dbName);
  }

  @Override
  public void dropHiveDatabase(String dbName) {
    LOG.info("Dropping database " + dbName);
    updateHiveSQLUsingJDBC("DROP DATABASE IF EXISTS " + dbName);
  }

  @Override
  public void dropHiveTable(String dbName, String tableName) {
    LOG.info("Dropping table " + tableName);
    updateHiveSQLUsingJDBC("DROP TABLE IF EXISTS " + dbName + "." + tableName);
  }
}