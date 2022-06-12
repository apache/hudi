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

package org.apache.hudi.sync.adb;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.function.Function;

public class HoodieAdbJdbcClient extends AbstractAdbSyncHoodieClient {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieAdbJdbcClient.class);

  public static final String HOODIE_LAST_COMMIT_TIME_SYNC = "hoodie_last_sync";
  // Make sure we have the jdbc driver in classpath
  private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
  public static final String ADB_ESCAPE_CHARACTER = "";
  private static final String TBL_PROPERTIES_STR = "TBLPROPERTIES";

  static {
    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + DRIVER_NAME + " in classpath. ", e);
    }
  }

  private Connection connection;

  public HoodieAdbJdbcClient(AdbSyncConfig syncConfig, FileSystem fs) {
    super(syncConfig, fs);
    createAdbConnection();
    LOG.info("Init adb jdbc client success, jdbcUrl:{}", syncConfig.adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl);
  }

  private void createAdbConnection() {
    if (connection == null) {
      try {
        Class.forName(DRIVER_NAME);
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load jdbc driver class", e);
        return;
      }
      try {
        this.connection = DriverManager.getConnection(
                adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl,
            adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.hiveUser, adbSyncConfig.adbSyncConfigParams.hiveSyncConfigParams.hivePass);
      } catch (SQLException e) {
        throw new HoodieException("Cannot create adb connection ", e);
      }
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties) {
    try {
      LOG.info("Creating table:{}", tableName);
      String createSQLQuery = HiveSchemaUtil.generateCreateDDL(tableName, storageSchema,
          getHiveSyncConfig(), inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
      executeAdbSql(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieException("Fail to create table:" + tableName, e);
    }
  }

  @Override
  public void dropTable(String tableName) {
    LOG.info("Dropping table:{}", tableName);
    String dropTable = "drop table if exists `" + adbSyncConfig.hoodieSyncConfigParams.databaseName + "`.`" + tableName + "`";
    executeAdbSql(dropTable);
  }

  public Map<String, String> getTableSchema(String tableName) {
    Map<String, String> schema = new HashMap<>();
    ResultSet result = null;
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      result = databaseMetaData.getColumns(adbSyncConfig.hoodieSyncConfigParams.databaseName,
              adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName, null);
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
      throw new HoodieException("Fail to get table schema:" + tableName, e);
    } finally {
      closeQuietly(result, null);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for table:{}", tableName);
      return;
    }

    LOG.info("Adding partitions to table:{}, partitionNum:{}", tableName, partitionsToAdd.size());
    String sql = constructAddPartitionsSql(tableName, partitionsToAdd);
    executeAdbSql(sql);
  }

  private void executeAdbSql(String sql) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      LOG.info("Executing sql:{}", sql);
      stmt.execute(sql);
    } catch (SQLException e) {
      throw new HoodieException("Fail to execute sql:" + sql, e);
    } finally {
      closeQuietly(null, stmt);
    }
  }

  private <T> T executeQuerySQL(String sql, Function<ResultSet, T> function) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      LOG.info("Executing sql:{}", sql);
      return function.apply(stmt.executeQuery(sql));
    } catch (SQLException e) {
      throw new HoodieException("Fail to execute sql:" + sql, e);
    } finally {
      closeQuietly(null, stmt);
    }
  }

  public void createDatabase(String databaseName) {
    String rootPath = getDatabasePath();
    LOG.info("Creating database:{}, databaseLocation:{}", databaseName, rootPath);
    String sql = constructCreateDatabaseSql(rootPath);
    executeAdbSql(sql);
  }

  public boolean databaseExists(String databaseName) {
    String sql = constructShowCreateDatabaseSql(databaseName);
    Function<ResultSet, Boolean> transform = resultSet -> {
      try {
        return resultSet.next();
      } catch (Exception e) {
        if (e.getMessage().contains("Unknown database `" + databaseName + "`")) {
          return false;
        } else {
          throw new HoodieException("Fail to execute sql:" + sql, e);
        }
      }
    };
    return executeQuerySQL(sql, transform);
  }

  @Override
  public boolean doesTableExist(String tableName) {
    String sql = constructShowLikeTableSql(tableName);
    Function<ResultSet, Boolean> transform = resultSet -> {
      try {
        return resultSet.next();
      } catch (Exception e) {
        throw new HoodieException("Fail to execute sql:" + sql, e);
      }
    };
    return executeQuerySQL(sql, transform);
  }

  @Override
  public boolean tableExists(String tableName) {
    return doesTableExist(tableName);
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    String sql = constructShowCreateTableSql(tableName);

    Function<ResultSet, Option<String>> transform = resultSet -> {
      try {
        if (resultSet.next()) {
          String table = resultSet.getString(2);
          Map<String, String> attr = new HashMap<>();
          int index = table.indexOf(TBL_PROPERTIES_STR);
          if (index != -1) {
            String sub = table.substring(index + TBL_PROPERTIES_STR.length());
            sub = sub
                .replaceAll("\\(", "")
                .replaceAll("\\)", "")
                .replaceAll("'", "");
            String[] str = sub.split(",");

            for (String s : str) {
              String key = s.split("=")[0].trim();
              String value = s.split("=")[1].trim();
              attr.put(key, value);
            }
          }
          return Option.ofNullable(attr.getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
        }
        return Option.empty();
      } catch (Exception e) {
        throw new HoodieException("Fail to execute sql:" + sql, e);
      }
    };
    return executeQuerySQL(sql, transform);
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the TBLProperties
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      String sql = constructUpdateTblPropertiesSql(tableName, lastCommitSynced);
      executeAdbSql(sql);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Fail to get update last commit time synced:" + lastCommitSynced, e);
    }
  }

  @Override
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    throw new UnsupportedOperationException("Not support updateTableProperties yet");
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for table:{}", tableName);
      return;
    }

    LOG.info("Changing partitions on table:{}, changedPartitionNum:{}", tableName, changedPartitions.size());
    List<String> sqlList = constructChangePartitionsSql(tableName, changedPartitions);
    for (String sql : sqlList) {
      executeAdbSql(sql);
    }
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    throw new UnsupportedOperationException("Not support dropPartitions yet.");
  }

  public Map<List<String>, String> scanTablePartitions(String tableName) {
    String sql = constructShowPartitionSql(tableName);
    Function<ResultSet, Map<List<String>, String>> transform = resultSet -> {
      Map<List<String>, String> partitions = new HashMap<>();
      try {
        while (resultSet.next()) {
          if (resultSet.getMetaData().getColumnCount() > 0) {
            String str = resultSet.getString(1);
            if (!StringUtils.isNullOrEmpty(str)) {
              List<String> values = partitionValueExtractor.extractPartitionValuesInPath(str);
              Path storagePartitionPath = FSUtils.getPartitionPath(adbSyncConfig.hoodieSyncConfigParams.basePath, String.join("/", values));
              String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
              partitions.put(values, fullStoragePartitionPath);
            }
          }
        }
      } catch (Exception e) {
        throw new HoodieException("Fail to execute sql:" + sql, e);
      }
      return partitions;
    };
    return executeQuerySQL(sql, transform);
  }

  public void updateTableDefinition(String tableName, SchemaDifference schemaDiff) {
    LOG.info("Adding columns for table:{}", tableName);
    schemaDiff.getAddColumnTypes().forEach((columnName, columnType) ->
        executeAdbSql(constructAddColumnSql(tableName, columnName, columnType))
    );

    LOG.info("Updating columns' definition for table:{}", tableName);
    schemaDiff.getUpdateColumnTypes().forEach((columnName, columnType) ->
        executeAdbSql(constructChangeColumnSql(tableName, columnName, columnType))
    );
  }

  private String constructAddPartitionsSql(String tableName, List<String> partitions) {
    StringBuilder sqlBuilder = new StringBuilder("alter table `");
    sqlBuilder.append(adbSyncConfig.hoodieSyncConfigParams.databaseName).append("`").append(".`")
        .append(tableName).append("`").append(" add if not exists ");
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(adbSyncConfig.hoodieSyncConfigParams.basePath, partition);
      String fullPartitionPathStr = generateAbsolutePathStr(partitionPath);
      sqlBuilder.append("  partition (").append(partitionClause).append(") location '")
          .append(fullPartitionPathStr).append("' ");
    }

    return sqlBuilder.toString();
  }

  private List<String> constructChangePartitionsSql(String tableName, List<String> partitions) {
    List<String> changePartitions = new ArrayList<>();
    String useDatabase = "use `" + adbSyncConfig.hoodieSyncConfigParams.databaseName + "`";
    changePartitions.add(useDatabase);

    String alterTable = "alter table `" + tableName + "`";
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(adbSyncConfig.hoodieSyncConfigParams.basePath, partition);
      String fullPartitionPathStr = generateAbsolutePathStr(partitionPath);
      String changePartition = alterTable + " add if not exists partition (" + partitionClause
          + ") location '" + fullPartitionPathStr + "'";
      changePartitions.add(changePartition);
    }

    return changePartitions;
  }

  /**
   * Generate Hive Partition from partition values.
   *
   * @param partition Partition path
   * @return partition clause
   */
  private String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(adbSyncConfig.hoodieSyncConfigParams.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + adbSyncConfig.hoodieSyncConfigParams.partitionFields
            + " does not match with partition values " + partitionValues + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < adbSyncConfig.hoodieSyncConfigParams.partitionFields.size(); i++) {
      partBuilder.add(adbSyncConfig.hoodieSyncConfigParams.partitionFields.get(i) + "='" + partitionValues.get(i) + "'");
    }

    return String.join(",", partBuilder);
  }

  private String constructShowPartitionSql(String tableName) {
    return String.format("show partitions `%s`.`%s`", adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName);
  }

  private String constructShowCreateTableSql(String tableName) {
    return String.format("show create table `%s`.`%s`", adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName);
  }

  private String constructShowLikeTableSql(String tableName) {
    return String.format("show tables from `%s` like '%s'", adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName);
  }

  private String constructCreateDatabaseSql(String rootPath) {
    return String.format("create database if not exists `%s` with dbproperties(catalog = 'oss', location = '%s')",
            adbSyncConfig.hoodieSyncConfigParams.databaseName, rootPath);
  }

  private String constructShowCreateDatabaseSql(String databaseName) {
    return String.format("show create database `%s`", databaseName);
  }

  private String constructUpdateTblPropertiesSql(String tableName, String lastCommitSynced) {
    return String.format("alter table `%s`.`%s` set tblproperties('%s' = '%s')",
            adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName, HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced);
  }

  private String constructAddColumnSql(String tableName, String columnName, String columnType) {
    return String.format("alter table `%s`.`%s` add columns(`%s` %s)",
            adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName, columnName, columnType);
  }

  private String constructChangeColumnSql(String tableName, String columnName, String columnType) {
    return String.format("alter table `%s`.`%s` change `%s` `%s` %s",
            adbSyncConfig.hoodieSyncConfigParams.databaseName, tableName, columnName, columnName, columnType);
  }

  private HiveSyncConfig getHiveSyncConfig() {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.hoodieSyncConfigParams.partitionFields = adbSyncConfig.hoodieSyncConfigParams.partitionFields;
    hiveSyncConfig.hoodieSyncConfigParams.databaseName = adbSyncConfig.hoodieSyncConfigParams.databaseName;
    Path basePath = new Path(adbSyncConfig.hoodieSyncConfigParams.basePath);
    hiveSyncConfig.hoodieSyncConfigParams.basePath = generateAbsolutePathStr(basePath);
    return hiveSyncConfig;
  }

  @Override
  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error("Fail to close connection", e);
    }
  }
}
