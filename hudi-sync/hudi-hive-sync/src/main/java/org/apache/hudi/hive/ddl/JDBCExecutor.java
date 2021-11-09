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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class offers DDL executor backed by the jdbc This class preserves the old useJDBC = true way of doing things.
 */
public class JDBCExecutor extends QueryBasedDDLExecutor {
  private static final Logger LOG = LogManager.getLogger(QueryBasedDDLExecutor.class);
  private final HiveSyncConfig config;
  private Connection connection;

  public JDBCExecutor(HiveSyncConfig config, FileSystem fs) {
    super(config, fs);
    Objects.requireNonNull(config.jdbcUrl, "--jdbc-url option is required for jdbc sync mode");
    Objects.requireNonNull(config.hiveUser, "--user option is required for jdbc sync mode");
    Objects.requireNonNull(config.hivePass, "--pass option is required for jdbc sync mode");
    this.config = config;
    createHiveConnection(config.jdbcUrl, config.hiveUser, config.hivePass);
  }

  @Override
  public void runSQL(String s) {
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

  private void closeQuietly(ResultSet resultSet, Statement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      LOG.warn("Could not close the statement opened ", e);
    }

    try {
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (SQLException e) {
      LOG.warn("Could not close the resultset opened ", e);
    }
  }

  private void createHiveConnection(String jdbcUrl, String hiveUser, String hivePass) {
    if (connection == null) {
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load Hive driver class", e);
        return;
      }

      try {
        this.connection = DriverManager.getConnection(jdbcUrl, hiveUser, hivePass);
        LOG.info("Successfully established Hive connection to  " + jdbcUrl);
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Cannot create hive connection " + getHiveJdbcUrlWithDefaultDBName(jdbcUrl), e);
      }
    }
  }

  private String getHiveJdbcUrlWithDefaultDBName(String jdbcUrl) {
    String hiveJdbcUrl = jdbcUrl;
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
  public Map<String, String> getTableSchema(String tableName) {
    Map<String, String> schema = new HashMap<>();
    ResultSet result = null;
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      result = databaseMetaData.getColumns(null, config.databaseName, tableName, null);
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

  @Override
  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close connection ", e);
    }
  }
}
