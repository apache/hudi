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
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hive.util.HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;

public abstract class QueryBasedDDLExecutor implements DDLExecutor {
  private static final Logger LOG = LogManager.getLogger(QueryBasedDDLExecutor.class);
  private final HiveSyncConfig config;
  private final PartitionValueExtractor partitionValueExtractor;
  private final FileSystem fs;

  public QueryBasedDDLExecutor(HiveSyncConfig config, FileSystem fs) {
    this.fs = fs;
    this.config = config;
    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(config.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + config.partitionValueExtractorClass, e);
    }
  }

  public abstract void runSQL(String s);

  @Override
  public void createDatabase(String databaseName) {
    runSQL("create database if not exists " + databaseName);
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass, Map<String, String> serdeProperties,
                          Map<String, String> tableProperties) {
    try {
      String createSQLQuery =
          HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, config, inputFormatClass,
              outputFormatClass, serdeClass, serdeProperties, tableProperties);
      LOG.info("Creating table with " + createSQLQuery);
      runSQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      String newSchemaStr = HiveSchemaUtil.generateSchemaString(newSchema, config.partitionFields, config.supportTimestamp);
      // Cascade clause should not be present for non-partitioned tables
      String cascadeClause = config.partitionFields.size() > 0 ? " cascade" : "";
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append(HIVE_ESCAPE_CHARACTER)
          .append(config.databaseName).append(HIVE_ESCAPE_CHARACTER).append(".")
          .append(HIVE_ESCAPE_CHARACTER).append(tableName)
          .append(HIVE_ESCAPE_CHARACTER).append(" REPLACE COLUMNS(")
          .append(newSchemaStr).append(" )").append(cascadeClause);
      LOG.info("Updating table definition with " + sqlBuilder);
      runSQL(sqlBuilder.toString());
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
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
    runSQL(sql);
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
      runSQL(sql);
    }
  }

  private String constructAddPartitions(String tableName, List<String> partitions) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(HIVE_ESCAPE_CHARACTER).append(config.databaseName)
        .append(HIVE_ESCAPE_CHARACTER).append(".").append(HIVE_ESCAPE_CHARACTER)
        .append(tableName).append(HIVE_ESCAPE_CHARACTER).append(" ADD IF NOT EXISTS ");
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      String fullPartitionPath = FSUtils.getPartitionPath(config.basePath, partition).toString();
      alterSQL.append("  PARTITION (").append(partitionClause).append(") LOCATION '").append(fullPartitionPath)
          .append("' ");
    }
    return alterSQL.toString();
  }

  private String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(config.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + config.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < config.partitionFields.size(); i++) {
      String partitionValue = partitionValues.get(i);
      // decode the partition before sync to hive to prevent multiple escapes of HIVE
      if (config.decodePartition) {
        try {
          // This is a decode operator for encode in KeyGenUtils#getRecordPartitionPath
          partitionValue = URLDecoder.decode(partitionValue, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
          throw new HoodieHiveSyncException("error in decode partition: " + partitionValue, e);
        }
      }
      partBuilder.add("`" + config.partitionFields.get(i) + "`='" + partitionValue + "'");
    }
    return String.join(",", partBuilder);
  }

  private List<String> constructChangePartitions(String tableName, List<String> partitions) {
    List<String> changePartitions = new ArrayList<>();
    // Hive 2.x doesn't like db.table name for operations, hence we need to change to using the database first
    String useDatabase = "USE " + HIVE_ESCAPE_CHARACTER + config.databaseName + HIVE_ESCAPE_CHARACTER;
    changePartitions.add(useDatabase);
    String alterTable = "ALTER TABLE " + HIVE_ESCAPE_CHARACTER + tableName + HIVE_ESCAPE_CHARACTER;
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(config.basePath, partition);
      String partitionScheme = partitionPath.toUri().getScheme();
      String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
          ? FSUtils.getDFSFullPartitionPath(fs, partitionPath) : partitionPath.toString();
      String changePartition =
          alterTable + " PARTITION (" + partitionClause + ") SET LOCATION '" + fullPartitionPath + "'";
      changePartitions.add(changePartition);
    }
    return changePartitions;
  }
}

