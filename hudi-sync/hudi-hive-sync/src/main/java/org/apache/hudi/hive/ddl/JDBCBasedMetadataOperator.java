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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
 */
public class JDBCBasedMetadataOperator {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCBasedMetadataOperator.class);

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
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SHOW TABLES IN `" + databaseName + "` LIKE '" + tableName + "'")) {
      return rs.next();
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to check if table exists via JDBC: " + tableName, e);
    }
  }

  /**
   * Checks if a database exists via {@code SHOW DATABASES LIKE}.
   */
  public boolean databaseExists(String dbName) {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SHOW DATABASES LIKE '" + dbName + "'")) {
      return rs.next();
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to check if database exists via JDBC: " + dbName, e);
    }
  }

  /**
   * Retrieves field schemas via {@code DESCRIBE}.
   */
  public List<FieldSchema> getFieldSchemas(String tableName) {
    List<FieldSchema> fields = new ArrayList<>();
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("DESCRIBE " + fqTable)) {
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
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to get field schemas via JDBC for: " + tableName, e);
    }
    return fields;
  }

  /**
   * Retrieves a single table property via {@code SHOW TBLPROPERTIES}.
   */
  public Option<String> getTableProperty(String tableName, String key) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SHOW TBLPROPERTIES " + fqTable + " ('" + key + "')")) {
      if (rs.next()) {
        String value = rs.getString(2);
        if (value != null && !value.contains("does not exist")) {
          return Option.of(value);
        }
      }
      return Option.empty();
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to get table property via JDBC: " + key, e);
    }
  }

  /**
   * Sets table properties via {@code ALTER TABLE ... SET TBLPROPERTIES}.
   */
  public void setTableProperties(String tableName, Map<String, String> properties) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    StringBuilder sb = new StringBuilder("ALTER TABLE ")
        .append(fqTable).append(" SET TBLPROPERTIES (");
    boolean first = true;
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("'").append(entry.getKey()).append("'='")
          .append(entry.getValue()).append("'");
      first = false;
    }
    sb.append(")");
    executeSQL(sb.toString());
  }

  /**
   * Retrieves the table location from {@code DESCRIBE FORMATTED}.
   */
  public String getTableLocation(String tableName) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("DESCRIBE FORMATTED " + fqTable)) {
      while (rs.next()) {
        String col = rs.getString(1);
        if (col != null && col.trim().equals("Location:")) {
          return rs.getString(2).trim();
        }
      }
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to get table location via JDBC for: " + tableName, e);
    }
    throw new HoodieHiveSyncException(
        "Location not found in DESCRIBE FORMATTED for: " + tableName);
  }

  /**
   * Lists all partitions via {@code SHOW PARTITIONS}.
   *
   * <p>Note: partition locations are not available from
   * {@code SHOW PARTITIONS}. The returned {@link Partition} objects
   * have locations constructed from the table base path.
   */
  public List<Partition> getAllPartitions(String tableName, String basePath) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    List<Partition> partitions = new ArrayList<>();
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SHOW PARTITIONS " + fqTable)) {
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
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to get partitions via JDBC for: " + tableName, e);
    }
    return partitions;
  }

  /**
   * Drops a table via {@code DROP TABLE IF EXISTS}.
   */
  public void dropTable(String tableName) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    executeSQL("DROP TABLE IF EXISTS " + fqTable);
    LOG.info("Dropped table via JDBC: {}.{}", databaseName, tableName);
  }

  /**
   * Renames a table via {@code ALTER TABLE ... RENAME TO}.
   */
  public void renameTable(String oldName, String newName) {
    String fqOld = "`" + databaseName + "`.`" + oldName + "`";
    String fqNew = "`" + databaseName + "`.`" + newName + "`";
    executeSQL("ALTER TABLE " + fqOld + " RENAME TO " + fqNew);
  }

  /**
   * Sets the table location and serde path via ALTER TABLE.
   */
  public void setTableLocation(String tableName, String location, String serdePathKey) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    executeSQL("ALTER TABLE " + fqTable + " SET LOCATION '" + location + "'");
    if (serdePathKey != null) {
      executeSQL("ALTER TABLE " + fqTable
          + " SET SERDEPROPERTIES ('" + serdePathKey + "'='" + location + "')");
    }
  }

  /**
   * Sets the storage format and serde properties via ALTER TABLE.
   */
  public void setStorageFormat(String tableName, String inputFormat,
                               String outputFormat, String serdeClass,
                               Map<String, String> serdeProperties) {
    String fqTable = "`" + databaseName + "`.`" + tableName + "`";
    executeSQL("ALTER TABLE " + fqTable + " SET FILEFORMAT INPUTFORMAT '"
        + inputFormat + "' OUTPUTFORMAT '" + outputFormat
        + "' SERDE '" + serdeClass + "'");
    if (serdeProperties != null && !serdeProperties.isEmpty()) {
      StringBuilder sb = new StringBuilder("ALTER TABLE ")
          .append(fqTable).append(" SET SERDEPROPERTIES (");
      boolean first = true;
      for (Map.Entry<String, String> entry : serdeProperties.entrySet()) {
        if (!first) {
          sb.append(", ");
        }
        sb.append("'").append(entry.getKey()).append("'='")
            .append(entry.getValue()).append("'");
        first = false;
      }
      sb.append(")");
      executeSQL(sb.toString());
    }
  }

  private void executeSQL(String sql) {
    try (Statement stmt = connection.createStatement()) {
      LOG.info("Executing JDBC metadata SQL: {}", sql);
      stmt.execute(sql);
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to execute JDBC metadata SQL: " + sql, e);
    }
  }
}
