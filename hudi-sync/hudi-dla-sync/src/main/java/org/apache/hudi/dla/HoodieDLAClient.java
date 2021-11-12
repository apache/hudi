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

package org.apache.hudi.dla;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieDLAClient extends AbstractSyncHoodieClient {
  private static final Logger LOG = LogManager.getLogger(HoodieDLAClient.class);
  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "hoodie_last_sync";
  // Make sure we have the dla JDBC driver in classpath
  private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
  private static final String DLA_ESCAPE_CHARACTER = "";
  private static final String TBL_PROPERTIES_STR = "TBLPROPERTIES";

  static {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + DRIVER_NAME + " in classpath. ", e);
    }
  }

  private Connection connection;
  private DLASyncConfig dlaConfig;
  private PartitionValueExtractor partitionValueExtractor;

  public HoodieDLAClient(DLASyncConfig syncConfig, FileSystem fs) {
    super(syncConfig.basePath, syncConfig.assumeDatePartitioning, syncConfig.useFileListingFromMetadata,
        false, fs);
    this.dlaConfig = syncConfig;
    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(dlaConfig.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieException(
          "Failed to initialize PartitionValueExtractor class " + dlaConfig.partitionValueExtractorClass, e);
    }
    createDLAConnection();
  }

  private void createDLAConnection() {
    if (connection == null) {
      try {
        Class.forName(DRIVER_NAME);
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load DLA driver class", e);
        return;
      }
      try {
        this.connection = DriverManager.getConnection(dlaConfig.jdbcUrl, dlaConfig.dlaUser, dlaConfig.dlaPass);
        LOG.info("Successfully established DLA connection to  " + dlaConfig.jdbcUrl);
      } catch (SQLException e) {
        throw new HoodieException("Cannot create dla connection ", e);
      }
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties) {
    try {
      String createSQLQuery = HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, toHiveSyncConfig(),
          inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
      LOG.info("Creating table with " + createSQLQuery);
      updateDLASQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieException("Failed to create table " + tableName, e);
    }
  }

  public Map<String, String> getTableSchema(String tableName) {
    if (!doesTableExist(tableName)) {
      throw new IllegalArgumentException(
          "Failed to get schema for table " + tableName + " does not exist");
    }
    Map<String, String> schema = new HashMap<>();
    ResultSet result = null;
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      result = databaseMetaData.getColumns(dlaConfig.databaseName, dlaConfig.databaseName, tableName, null);
      while (result.next()) {
        TYPE_CONVERTOR.doConvert(result, schema);
      }
      return schema;
    } catch (SQLException e) {
      throw new HoodieException("Failed to get table schema for " + tableName, e);
    } finally {
      closeQuietly(result, null);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    String sql = constructAddPartitions(tableName, partitionsToAdd);
    updateDLASQL(sql);
  }

  public String constructAddPartitions(String tableName, List<String> partitions) {
    return constructDLAAddPartitions(tableName, partitions);
  }

  String generateAbsolutePathStr(Path path) {
    String absolutePathStr = path.toString();
    if (path.toUri().getScheme() == null) {
      absolutePathStr = getDefaultFs() + absolutePathStr;
    }
    return absolutePathStr.endsWith("/") ? absolutePathStr : absolutePathStr + "/";
  }

  public List<String> constructChangePartitions(String tableName, List<String> partitions) {
    List<String> changePartitions = new ArrayList<>();
    String useDatabase = "USE " + DLA_ESCAPE_CHARACTER + dlaConfig.databaseName + DLA_ESCAPE_CHARACTER;
    changePartitions.add(useDatabase);
    String alterTable = "ALTER TABLE " + DLA_ESCAPE_CHARACTER + tableName + DLA_ESCAPE_CHARACTER;
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(dlaConfig.basePath, partition);
      String fullPartitionPathStr = generateAbsolutePathStr(partitionPath);
      String changePartition =
          alterTable + " ADD IF NOT EXISTS PARTITION (" + partitionClause + ") LOCATION '" + fullPartitionPathStr + "'";
      changePartitions.add(changePartition);
    }
    return changePartitions;
  }

  /**
   * Generate Hive Partition from partition values.
   *
   * @param partition Partition path
   * @return
   */
  public String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(dlaConfig.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + dlaConfig.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < dlaConfig.partitionFields.size(); i++) {
      partBuilder.add(dlaConfig.partitionFields.get(i) + "='" + partitionValues.get(i) + "'");
    }
    return partBuilder.stream().collect(Collectors.joining(","));
  }

  private String constructDLAAddPartitions(String tableName, List<String> partitions) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(DLA_ESCAPE_CHARACTER).append(dlaConfig.databaseName)
        .append(DLA_ESCAPE_CHARACTER).append(".").append(DLA_ESCAPE_CHARACTER)
        .append(tableName).append(DLA_ESCAPE_CHARACTER).append(" ADD IF NOT EXISTS ");
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(dlaConfig.basePath, partition);
      String fullPartitionPathStr = generateAbsolutePathStr(partitionPath);
      alterSQL.append("  PARTITION (").append(partitionClause).append(") LOCATION '").append(fullPartitionPathStr)
          .append("' ");
    }
    return alterSQL.toString();
  }

  private void updateDLASQL(String sql) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      LOG.info("Executing SQL " + sql);
      stmt.execute(sql);
    } catch (SQLException e) {
      throw new HoodieException("Failed in executing SQL " + sql, e);
    } finally {
      closeQuietly(null, stmt);
    }
  }

  @Override
  public boolean doesTableExist(String tableName) {
    String sql = consutructShowCreateTableSQL(tableName);
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      return false;
    } finally {
      closeQuietly(rs, stmt);
    }
    return true;
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    String sql = consutructShowCreateTableSQL(tableName);
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(sql);
      if (rs.next()) {
        String table = rs.getString(2);
        Map<String, String> attr = new HashMap<>();
        int index = table.indexOf(TBL_PROPERTIES_STR);
        if (index != -1) {
          String sub = table.substring(index + TBL_PROPERTIES_STR.length());
          sub = sub.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("'", "");
          String[] str = sub.split(",");

          for (int i = 0; i < str.length; i++) {
            String key = str[i].split("=")[0].trim();
            String value = str[i].split("=")[1].trim();
            attr.put(key, value);
          }
        }
        return Option.ofNullable(attr.getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
      }
      return Option.empty();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the table", e);
    } finally {
      closeQuietly(rs, stmt);
    }
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // TODO : dla do not support update tblproperties, so do nothing.
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    List<String> sqls = constructChangePartitions(tableName, changedPartitions);
    for (String sql : sqls) {
      updateDLASQL(sql);
    }
  }

  public Map<List<String>, String> scanTablePartitions(String tableName) {
    String sql = constructShowPartitionSQL(tableName);
    Statement stmt = null;
    ResultSet rs = null;
    Map<List<String>, String> partitions = new HashMap<>();
    try {
      stmt = connection.createStatement();
      LOG.info("Executing SQL " + sql);
      rs = stmt.executeQuery(sql);
      while (rs.next()) {
        if (rs.getMetaData().getColumnCount() > 0) {
          String str = rs.getString(1);
          if (!StringUtils.isNullOrEmpty(str)) {
            List<String> values = partitionValueExtractor.extractPartitionValuesInPath(str);
            Path storagePartitionPath = FSUtils.getPartitionPath(dlaConfig.basePath, String.join("/", values));
            String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
            partitions.put(values, fullStoragePartitionPath);
          }
        }
      }
      return partitions;
    } catch (SQLException e) {
      throw new HoodieException("Failed in executing SQL " + sql, e);
    } finally {
      closeQuietly(rs, stmt);
    }
  }

  public List<PartitionEvent> getPartitionEvents(Map<List<String>, String> tablePartitions, List<String> partitionStoragePartitions) {
    Map<String, String> paths = new HashMap<>();

    for (Map.Entry<List<String>, String> entry : tablePartitions.entrySet()) {
      List<String> partitionValues = entry.getKey();
      Collections.sort(partitionValues);
      String fullTablePartitionPath = entry.getValue();
      paths.put(String.join(", ", partitionValues), fullTablePartitionPath);
    }
    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(dlaConfig.basePath, storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);
      if (dlaConfig.useDLASyncHiveStylePartitioning) {
        String partition = String.join("/", storagePartitionValues);
        storagePartitionPath = FSUtils.getPartitionPath(dlaConfig.basePath, partition);
        fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      }
      Collections.sort(storagePartitionValues);
      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }
    return events;
  }

  public void updateTableDefinition(String tableName, SchemaDifference schemaDiff) {
    ValidationUtils.checkArgument(schemaDiff.getDeleteColumns().size() == 0, "not support delete columns");
    ValidationUtils.checkArgument(schemaDiff.getUpdateColumnTypes().size() == 0, "not support alter column type");
    Map<String, String> columns = schemaDiff.getAddColumnTypes();
    for (Map.Entry<String, String> entry : columns.entrySet()) {
      String columnName = entry.getKey();
      String columnType = entry.getValue();
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append(DLA_ESCAPE_CHARACTER)
          .append(dlaConfig.databaseName).append(DLA_ESCAPE_CHARACTER).append(".")
          .append(DLA_ESCAPE_CHARACTER).append(tableName)
          .append(DLA_ESCAPE_CHARACTER).append(" ADD COLUMNS(")
          .append(columnName).append(" ").append(columnType).append(" )");
      LOG.info("Updating table definition with " + sqlBuilder);
      updateDLASQL(sqlBuilder.toString());
    }
  }

  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close connection ", e);
    }
  }

  private String constructShowPartitionSQL(String tableName) {
    String sql = "show partitions " + dlaConfig.databaseName + "." + tableName;
    return sql;
  }

  private String consutructShowCreateTableSQL(String tableName) {
    String sql = "show create table " + dlaConfig.databaseName + "." + tableName;
    return sql;
  }

  private String getDefaultFs() {
    return fs.getConf().get("fs.defaultFS");
  }

  private HiveSyncConfig toHiveSyncConfig() {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.partitionFields = dlaConfig.partitionFields;
    hiveSyncConfig.databaseName = dlaConfig.databaseName;
    Path basePath = new Path(dlaConfig.basePath);
    hiveSyncConfig.basePath = generateAbsolutePathStr(basePath);
    return hiveSyncConfig;
  }
}
