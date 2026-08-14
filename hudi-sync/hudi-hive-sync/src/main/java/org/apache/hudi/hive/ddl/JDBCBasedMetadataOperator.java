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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provides metadata query operations (tableExists, databaseExists,
 * getTableSchema, etc.) backed by a JDBC connection to HiveServer2.
 *
 * <p>This is used as a fallback when the Thrift-based
 * {@link org.apache.hadoop.hive.metastore.IMetaStoreClient} is
 * incompatible with the target HMS version (e.g., HMS 4.x changed the
 * Thrift API from {@code get_table} to {@code get_table_req}).
 *
 * <p>All SQL queries use standard HiveQL that is stable across Hive
 * versions. The operator does not manage the lifecycle of the JDBC
 * connection — the caller ({@link JDBCExecutor}) owns it.
 *
 * <p>This JDBC-based approach serves as an interim fallback. A longer-term
 * solution could introduce an HMS adapter pattern (similar to SparkAdapter)
 * to compile against HMS 4.x Thrift APIs directly.
 */
@Slf4j
public class JDBCBasedMetadataOperator {

  @FunctionalInterface
  private interface ResultSetExtractor<T> {
    T extract(ResultSet rs) throws SQLException;
  }

  private final Connection connection;
  private final String databaseName;

  public JDBCBasedMetadataOperator(Connection connection, String databaseName) {
    this.connection = connection;
    this.databaseName = databaseName;
  }

  /**
   * Checks if a table exists via {@code SHOW TABLES ... LIKE}.
   */
  public boolean tableExists(String tableName) {
    return executeQuery(
        "SHOW TABLES IN `" + databaseName + "` LIKE '" + tableName + "'",
        ResultSet::next,
        "Failed to check if table exists via JDBC: " + tableName);
  }

  /**
   * Checks if a database exists via {@code SHOW DATABASES LIKE}.
   */
  public boolean databaseExists(String dbName) {
    return executeQuery(
        "SHOW DATABASES LIKE '" + dbName + "'",
        ResultSet::next,
        "Failed to check if database exists via JDBC: " + dbName);
  }

  /**
   * Retrieves field schemas via {@code DESCRIBE}.
   */
  public List<FieldSchema> getFieldSchemas(String tableName) {
    String fqTable = fqTableName(tableName);
    return executeQuery("DESCRIBE " + fqTable, rs -> {
      List<FieldSchema> fields = new ArrayList<>();
      while (rs.next()) {
        String colName = rs.getString(1);
        String colType = rs.getString(2);
        String comment = rs.getString(3);
        if (colName != null && !colName.trim().isEmpty()
            && !colName.startsWith("#")) {
          fields.add(new FieldSchema(
              colName.trim(),
              colType != null ? colType.trim() : "",
              comment));
        }
      }
      return fields;
    }, "Failed to get field schemas via JDBC for: " + tableName);
  }

  /**
   * Retrieves a single table property via {@code SHOW TBLPROPERTIES}.
   *
   * <p>When the requested property does not exist, Hive returns a single-column
   * result with an error message. A valid property returns two columns (key, value).
   * We use the column count to distinguish these cases rather than checking
   * the result text for specific error strings.
   */
  public Option<String> getTableProperty(String tableName, String key) {
    String fqTable = fqTableName(tableName);
    return executeQuery(
        "SHOW TBLPROPERTIES " + fqTable + " ('" + key + "')", rs -> {
          if (rs.next()) {
            int columnCount = rs.getMetaData().getColumnCount();
            if (columnCount >= 2) {
              String value = rs.getString(2);
              if (value != null) {
                return Option.of(value);
              }
            }
          }
          return Option.empty();
        }, "Failed to get table property via JDBC: " + key);
  }

  /**
   * Sets table properties via {@code ALTER TABLE ... SET TBLPROPERTIES}.
   */
  public void setTableProperties(String tableName, Map<String, String> properties) {
    String fqTable = fqTableName(tableName);
    StringBuilder sb = new StringBuilder("ALTER TABLE ")
        .append(fqTable).append(" SET TBLPROPERTIES (");
    appendKeyValuePairs(sb, properties);
    sb.append(")");
    executeSQL(sb.toString());
  }

  /**
   * Removes a table property via {@code ALTER TABLE ... UNSET TBLPROPERTIES IF EXISTS}.
   */
  public void unsetTableProperty(String tableName, String key) {
    String fqTable = fqTableName(tableName);
    executeSQL("ALTER TABLE " + fqTable
        + " UNSET TBLPROPERTIES IF EXISTS ('" + escapeSingleQuotes(key) + "')");
  }

  /**
   * Retrieves the table location from {@code DESCRIBE FORMATTED}.
   */
  public String getTableLocation(String tableName) {
    String fqTable = fqTableName(tableName);
    return executeQuery("DESCRIBE FORMATTED " + fqTable, rs -> {
      while (rs.next()) {
        String col = rs.getString(1);
        if (col != null && col.trim().equals("Location:")) {
          return rs.getString(2).trim();
        }
      }
      throw new HoodieHiveSyncException(
          "Location not found in DESCRIBE FORMATTED for: " + tableName);
    }, "Failed to get table location via JDBC for: " + tableName);
  }

  /**
   * Lists all partitions via {@code SHOW PARTITIONS}.
   *
   * <p>Note: partition locations are not available from
   * {@code SHOW PARTITIONS}. The returned {@link Partition} objects
   * have locations constructed from the table base path.
   *
   * <p>Example: {@code SHOW PARTITIONS} returns rows like
   * {@code datestamp=2025-01-15/region=us}, which is parsed into a
   * {@link Partition} with values {@code ["2025-01-15", "us"]} and
   * location {@code basePath/datestamp=2025-01-15/region=us}.
   */
  public List<Partition> getAllPartitions(String tableName, String basePath) {
    String fqTable = fqTableName(tableName);
    return executeQuery("SHOW PARTITIONS " + fqTable, rs -> {
      List<Partition> partitions = new ArrayList<>();
      while (rs.next()) {
        String partSpec = rs.getString(1);
        List<String> values = new ArrayList<>();
        for (String kv : partSpec.split("/")) {
          int idx = kv.indexOf("=");
          values.add(idx >= 0 ? kv.substring(idx + 1) : kv);
        }
        String location = basePath + "/" + partSpec;
        partitions.add(new Partition(values, location));
      }
      return partitions;
    }, "Failed to get partitions via JDBC for: " + tableName);
  }

  /**
   * Drops a table via {@code DROP TABLE IF EXISTS}.
   */
  public void dropTable(String tableName) {
    String fqTable = fqTableName(tableName);
    executeSQL("DROP TABLE IF EXISTS " + fqTable);
    log.info("Dropped table via JDBC: {}.{}", databaseName, tableName);
  }

  /**
   * Renames a table via {@code ALTER TABLE ... RENAME TO}.
   */
  public void renameTable(String oldName, String newName) {
    String fqOld = fqTableName(oldName);
    String fqNew = fqTableName(newName);
    executeSQL("ALTER TABLE " + fqOld + " RENAME TO " + fqNew);
  }

  /**
   * Sets the table location and optionally the serde path via ALTER TABLE.
   *
   * @param serdePathKey if present, also sets the serde property with the given key to the location
   */
  public void setTableLocation(String tableName, String location, Option<String> serdePathKey) {
    String fqTable = fqTableName(tableName);
    executeSQL("ALTER TABLE " + fqTable + " SET LOCATION '" + location + "'");
    if (serdePathKey.isPresent()) {
      executeSQL("ALTER TABLE " + fqTable
          + " SET SERDEPROPERTIES ('" + serdePathKey.get() + "'='" + location + "')");
    }
  }

  /**
   * Sets the storage format and serde properties via ALTER TABLE.
   *
   * @param serdeProperties must not be null; pass an empty map if no properties are needed
   */
  public void setStorageFormat(String tableName, String inputFormat,
                               String outputFormat, String serdeClass,
                               Map<String, String> serdeProperties) {
    Objects.requireNonNull(serdeProperties, "serdeProperties must not be null");
    String fqTable = fqTableName(tableName);
    executeSQL("ALTER TABLE " + fqTable + " SET FILEFORMAT INPUTFORMAT '"
        + inputFormat + "' OUTPUTFORMAT '" + outputFormat
        + "' SERDE '" + serdeClass + "'");
    if (!serdeProperties.isEmpty()) {
      StringBuilder sb = new StringBuilder("ALTER TABLE ")
          .append(fqTable).append(" SET SERDEPROPERTIES (");
      appendKeyValuePairs(sb, serdeProperties);
      sb.append(")");
      executeSQL(sb.toString());
    }
  }

  private String fqTableName(String tableName) {
    return "`" + databaseName + "`.`" + tableName + "`";
  }

  private <T> T executeQuery(String sql, ResultSetExtractor<T> extractor, String errorMsg) {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      return extractor.extract(rs);
    } catch (HoodieHiveSyncException e) {
      throw e;
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(errorMsg, e);
    }
  }

  private void executeSQL(String sql) {
    try (Statement stmt = connection.createStatement()) {
      log.info("Executing JDBC metadata SQL: {}", sql);
      stmt.execute(sql);
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to execute JDBC metadata SQL: " + sql, e);
    }
  }

  private static String escapeSingleQuotes(String value) {
    return value.replace("'", "\\'");
  }

  private static void appendKeyValuePairs(StringBuilder sb, Map<String, String> pairs) {
    boolean first = true;
    for (Map.Entry<String, String> entry : pairs.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("'").append(escapeSingleQuotes(entry.getKey())).append("'='")
          .append(escapeSingleQuotes(entry.getValue())).append("'");
      first = false;
    }
  }
}
