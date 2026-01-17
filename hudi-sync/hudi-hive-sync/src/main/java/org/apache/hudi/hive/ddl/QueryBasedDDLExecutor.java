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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.util.HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DECODE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;

/**
 * This class adds functionality for all query based DDLExecutors. The classes extending it only have to provide runSQL(sql) functions.
 */
@Slf4j
public abstract class QueryBasedDDLExecutor implements DDLExecutor {

  protected final HiveSyncConfig config;
  protected final String databaseName;
  protected final PartitionValueExtractor partitionValueExtractor;

  public QueryBasedDDLExecutor(HiveSyncConfig config) {
    this.config = config;
    this.databaseName = config.getStringOrDefault(META_SYNC_DATABASE_NAME);
    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(config.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS)).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + config.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS), e);
    }
  }

  /**
   * All implementations of QueryBasedDDLExecutor must supply the runSQL function.
   * @param sql is the sql query which needs to be run
   */
  public abstract void runSQL(String sql);

  @Override
  public void createDatabase(String databaseName) {
    runSQL("create database if not exists " + databaseName);
  }

  @Override
  public void createTable(String tableName, HoodieSchema storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass, Map<String, String> serdeProperties,
                          Map<String, String> tableProperties) {
    try {
      String createSQLQuery =
          HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, config, inputFormatClass,
              outputFormatClass, serdeClass, serdeProperties, tableProperties);
      log.info("Creating table with {}", createSQLQuery);
      runSQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, HoodieSchema newSchema) {
    try {
      String newSchemaStr = HiveSchemaUtil.generateSchemaString(newSchema, config.getSplitStrings(META_SYNC_PARTITION_FIELDS), config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE));
      // Cascade clause should not be present for non-partitioned tables
      String cascadeClause = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size() > 0 ? " cascade" : "";
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append(HIVE_ESCAPE_CHARACTER)
          .append(databaseName).append(HIVE_ESCAPE_CHARACTER).append(".")
          .append(HIVE_ESCAPE_CHARACTER).append(tableName)
          .append(HIVE_ESCAPE_CHARACTER).append(" REPLACE COLUMNS(")
          .append(newSchemaStr).append(" )").append(cascadeClause);
      log.info("Updating table definition with {}", sqlBuilder);
      runSQL(sqlBuilder.toString());
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      log.info("No partitions to add for {}", tableName);
      return;
    }
    log.info("Adding partitions {} to table {}", partitionsToAdd.size(), tableName);
    List<String> sqls = constructAddPartitions(tableName, partitionsToAdd);
    sqls.stream().forEach(sql -> runSQL(sql));
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      log.info("No partitions to change for {}", tableName);
      return;
    }
    log.info("Changing partitions {} on {}", changedPartitions.size(), tableName);
    List<String> sqls = constructChangePartitions(tableName, changedPartitions);
    for (String sql : sqls) {
      runSQL(sql);
    }
  }

  @Override
  public void updateTableComments(String tableName, Map<String, Pair<String, String>> newSchema) {
    for (Map.Entry<String, Pair<String,String>> field : newSchema.entrySet()) {
      String name = field.getKey();
      StringBuilder sql = new StringBuilder();
      String type = field.getValue().getLeft();
      String comment = field.getValue().getRight();
      comment = comment.replace("'","");
      sql.append("ALTER TABLE ").append(HIVE_ESCAPE_CHARACTER)
              .append(databaseName).append(HIVE_ESCAPE_CHARACTER).append(".")
              .append(HIVE_ESCAPE_CHARACTER).append(tableName)
              .append(HIVE_ESCAPE_CHARACTER)
              .append(" CHANGE COLUMN `").append(name).append("` `").append(name)
              .append("` ").append(type).append(" comment '").append(comment).append("' ");
      runSQL(sql.toString());
    }
  }

  private List<String> constructAddPartitions(String tableName, List<String> partitions) {
    List<String> result = new ArrayList<>();
    int batchSyncPartitionNum = config.getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM);
    StringBuilder alterSQL = getAlterTablePrefix(tableName);
    for (int i = 0; i < partitions.size(); i++) {
      String partitionClause = getPartitionClause(partitions.get(i));
      String fullPartitionPath =
          FSUtils.constructAbsolutePath(config.getString(META_SYNC_BASE_PATH), partitions.get(i)).toString();
      alterSQL.append("  PARTITION (").append(partitionClause).append(") LOCATION '").append(fullPartitionPath)
          .append("' ");
      if ((i + 1) % batchSyncPartitionNum == 0) {
        result.add(alterSQL.toString());
        alterSQL = getAlterTablePrefix(tableName);
      }
    }
    // add left partitions to result
    if (partitions.size() % batchSyncPartitionNum != 0) {
      result.add(alterSQL.toString());
    }
    return result;
  }

  private StringBuilder getAlterTablePrefix(String tableName) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(HIVE_ESCAPE_CHARACTER).append(databaseName)
        .append(HIVE_ESCAPE_CHARACTER).append(".").append(HIVE_ESCAPE_CHARACTER)
        .append(tableName).append(HIVE_ESCAPE_CHARACTER).append(" ADD IF NOT EXISTS ");
    return alterSQL;
  }

  public String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size() == partitionValues.size(),
        "Partition key parts " + config.getSplitStrings(META_SYNC_PARTITION_FIELDS) + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size(); i++) {
      String partitionValue = partitionValues.get(i);
      // decode the partition before sync to hive to prevent multiple escapes of HIVE
      if (config.getBoolean(META_SYNC_DECODE_PARTITION)) {
        // This is a decode operator for encode in KeyGenUtils#getRecordPartitionPath
        partitionValue = PartitionPathEncodeUtils.unescapePathName(partitionValue);
      }
      partBuilder.add("`" + config.getSplitStrings(META_SYNC_PARTITION_FIELDS).get(i) + "`='" + partitionValue + "'");
    }
    return String.join(",", partBuilder);
  }

  private List<String> constructChangePartitions(String tableName, List<String> partitions) {
    List<String> changePartitions = new ArrayList<>();
    // Hive 2.x doesn't like db.table name for operations, hence we need to change to using the database first
    String useDatabase = "USE " + HIVE_ESCAPE_CHARACTER + databaseName + HIVE_ESCAPE_CHARACTER;
    changePartitions.add(useDatabase);
    String alterTable = "ALTER TABLE " + HIVE_ESCAPE_CHARACTER + tableName + HIVE_ESCAPE_CHARACTER;
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = HadoopFSUtils.constructAbsolutePathInHadoopPath(config.getString(META_SYNC_BASE_PATH), partition);
      String partitionScheme = partitionPath.toUri().getScheme();
      String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
          ? HadoopFSUtils.getDFSFullPartitionPath(config.getHadoopFileSystem(), partitionPath) : partitionPath.toString();
      String changePartition =
          alterTable + " PARTITION (" + partitionClause + ") SET LOCATION '" + fullPartitionPath + "'";
      changePartitions.add(changePartition);
    }
    return changePartitions;
  }
}

