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

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.hive.util.HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;

/**
 * This class offers DDL executor backed by the jdbc This class preserves the old useJDBC = true way of doing things.
 */
@Slf4j
public class JDBCExecutor extends QueryBasedDDLExecutor {

  private Connection connection;

  public JDBCExecutor(HiveSyncConfig config) {
    super(config);
    Objects.requireNonNull(config.getStringOrDefault(HIVE_URL), "--jdbc-url option is required for jdbc sync mode");
    Objects.requireNonNull(config.getStringOrDefault(HIVE_USER), "--user option is required for jdbc sync mode");
    Objects.requireNonNull(config.getStringOrDefault(HIVE_PASS), "--pass option is required for jdbc sync mode");
    createHiveConnection(config.getStringOrDefault(HIVE_URL), config.getStringOrDefault(HIVE_USER), config.getStringOrDefault(HIVE_PASS));
  }

  @Override
  public void runSQL(String s) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      log.info("Executing SQL {}", s);
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
      log.info("Could not close the statement opened ", e);
    }

    try {
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (SQLException e) {
      log.info("Could not close the resultset opened ", e);
    }
  }

  private void createHiveConnection(String jdbcUrl, String hiveUser, String hivePass) {
    if (connection == null) {
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
      } catch (ClassNotFoundException e) {
        log.error("Unable to load Hive driver class", e);
        return;
      }

      try {
        this.connection = DriverManager.getConnection(jdbcUrl, hiveUser, hivePass);
        log.info("Successfully established Hive connection to {}", jdbcUrl);
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
      String catalog = connection.getCatalog();
      result = databaseMetaData.getColumns(catalog, databaseName, tableName, "%");
      while (result.next()) {
        String columnName = result.getString("COLUMN_NAME");
        String columnType = result.getString("TYPE_NAME");
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
  public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
    if (partitionsToDrop.isEmpty()) {
      log.info("No partitions to add for {}", tableName);
      return;
    }
    log.info("Dropping partitions {} from table {}", partitionsToDrop.size(), tableName);
    List<String> sqls = constructDropPartitions(tableName, partitionsToDrop);
    sqls.stream().forEach(sql -> runSQL(sql));
  }

  private List<String> constructDropPartitions(String tableName, List<String> partitions) {
    List<String> result = new ArrayList<>();
    int batchSyncPartitionNum = config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM);
    StringBuilder alterSQL = getAlterTableDropPrefix(tableName);

    for (int i = 0; i < partitions.size(); i++) {
      String partitionClause = getPartitionClause(partitions.get(i));
      if (i == 0) {
        alterSQL.append(" PARTITION (").append(partitionClause).append(")");
      } else {
        alterSQL.append(", PARTITION (").append(partitionClause).append(")");
      }

      if ((i + 1) % batchSyncPartitionNum == 0) {
        result.add(alterSQL.toString());
        alterSQL = getAlterTableDropPrefix(tableName);
      }
    }
    // add left partitions to result
    if (partitions.size() % batchSyncPartitionNum != 0) {
      result.add(alterSQL.toString());
    }
    return result;
  }

  public StringBuilder getAlterTableDropPrefix(String tableName) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(HIVE_ESCAPE_CHARACTER).append(databaseName)
        .append(HIVE_ESCAPE_CHARACTER).append(".").append(HIVE_ESCAPE_CHARACTER)
        .append(tableName).append(HIVE_ESCAPE_CHARACTER).append(" DROP IF EXISTS ");
    return alterSQL;
  }

  @Override
  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      log.error("Could not close connection ", e);
    }
  }
}
