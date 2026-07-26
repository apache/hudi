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
import org.apache.hudi.hive.util.JDBCConnectionPool;

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
import java.util.concurrent.Future;

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
  // Optional. When non-null, partition-phase SQL lists fan out across this pool;
  // table-level SQL (createTable, schema evolution, single-statement runSQL callers)
  // always uses the session `connection` above. The connection passed to
  // JDBCBasedMetadataOperator (via getConnection()) is also the session connection.
  // See JDBCConnectionPool javadoc.
  private final JDBCConnectionPool connectionPool;

  /**
   * Returns the underlying JDBC connection for use by
   * {@link JDBCBasedMetadataOperator} when the Thrift
   * IMetaStoreClient is unavailable (e.g., HMS 4.x).
   */
  public Connection getConnection() {
    return connection;
  }

  public JDBCExecutor(HiveSyncConfig config) {
    this(config, null);
  }

  public JDBCExecutor(HiveSyncConfig config, JDBCConnectionPool connectionPool) {
    super(config);
    Objects.requireNonNull(config.getStringOrDefault(HIVE_URL), "--jdbc-url option is required for jdbc sync mode");
    Objects.requireNonNull(config.getStringOrDefault(HIVE_USER), "--user option is required for jdbc sync mode");
    Objects.requireNonNull(config.getStringOrDefault(HIVE_PASS), "--pass option is required for jdbc sync mode");
    createHiveConnection(config.getStringOrDefault(HIVE_URL), config.getStringOrDefault(HIVE_USER), config.getStringOrDefault(HIVE_PASS));
    this.connectionPool = connectionPool;
  }

  @Override
  public void runSQL(String s) {
    runOnConnection(connection, s);
  }

  private void runOnConnection(Connection conn, String s) {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      log.info("Executing SQL {}", s);
      stmt.execute(s);
    } catch (SQLException e) {
      throw new HoodieHiveSyncException("Failed in executing SQL " + s, e);
    } finally {
      closeQuietly(null, stmt);
    }
  }

  /**
   * Partition-phase SQL fan-out. When the connection pool is present, any leading
   * {@code USE database} statements are broadcast to every pooled connection (Hive
   * 2.x's ALTER PARTITION SET LOCATION ignores db.table qualifiers and uses the
   * connection's current database, so each pooled connection needs to USE the
   * right db before any partition ALTER). The remaining statements are then
   * dispatched round-robin across the pool. Falls through to the sequential path
   * on the session connection when no pool is configured.
   */
  @Override
  protected void runSQLs(List<String> sqls) {
    if (connectionPool == null || sqls.isEmpty()) {
      super.runSQLs(sqls);
      return;
    }
    int firstNonUse = 0;
    while (firstNonUse < sqls.size() && isUseStatement(sqls.get(firstNonUse))) {
      firstNonUse++;
    }
    if (firstNonUse > 0) {
      List<String> setupStatements = sqls.subList(0, firstNonUse);
      try {
        connectionPool.runOnEachConnection(setupStatements);
      } catch (Exception e) {
        throw new HoodieHiveSyncException("Failed running per-connection setup SQL", e);
      }
    }
    List<String> partitionStatements = sqls.subList(firstNonUse, sqls.size());
    if (partitionStatements.isEmpty()) {
      return;
    }
    List<Future<?>> futures = new ArrayList<>(partitionStatements.size());
    for (String sql : partitionStatements) {
      futures.add(connectionPool.executor().submit(() ->
          connectionPool.run(conn -> {
            runOnConnection(conn, sql);
            return null;
          })
      ));
    }
    HoodieHiveSyncException firstError = null;
    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        if (firstError == null) {
          firstError = new HoodieHiveSyncException("Interrupted while running partition SQL", ie);
        }
      } catch (java.util.concurrent.ExecutionException ee) {
        Throwable cause = ee.getCause();
        if (firstError == null) {
          firstError = (cause instanceof HoodieHiveSyncException)
              ? (HoodieHiveSyncException) cause
              : new HoodieHiveSyncException("Failed in executing SQL", cause);
        } else {
          log.warn("Additional JDBC partition SQL failed (suppressed in favor of first error)", cause);
        }
      }
    }
    if (firstError != null) {
      throw firstError;
    }
  }

  private static boolean isUseStatement(String sql) {
    return sql != null && sql.regionMatches(true, 0, "USE ", 0, 4);
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
    // Close the pool first so worker threads stop dispatching against their
    // connections before we tear down anything else.
    if (connectionPool != null) {
      try {
        connectionPool.close();
      } catch (Exception e) {
        log.warn("Error closing JDBCConnectionPool", e);
      }
    }
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      log.error("Could not close connection ", e);
    }
  }
}
