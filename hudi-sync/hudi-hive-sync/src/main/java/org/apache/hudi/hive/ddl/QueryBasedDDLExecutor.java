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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.HoodieSyncException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.hive.util.HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;

/**
 * This class adds functionality for all query based DDLExecutors. The classes extending it only have to provide runSQL(sql) functions.
 */
public abstract class QueryBasedDDLExecutor implements DDLExecutor {
  private static final Logger LOG = LogManager.getLogger(QueryBasedDDLExecutor.class);
  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  private final HiveSyncConfig config;
  protected final PartitionValueExtractor partitionValueExtractor;
  protected final FileSystem fs;
  protected final IMetaStoreClient client;

  public QueryBasedDDLExecutor(HiveSyncConfig config, FileSystem fs, IMetaStoreClient client) {
    this.fs = fs;
    this.config = config;
    this.client = client;
    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(config.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + config.partitionValueExtractorClass, e);
    }
  }

  public QueryBasedDDLExecutor(HiveSyncConfig config, FileSystem fs) {
    this(config, fs, null);
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
    List<String> sqls = constructAddPartitions(tableName, partitionsToAdd);
    sqls.stream().forEach(sql -> runSQL(sql));
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

  @Override
  public void updateTableComments(String tableName, Map<String, ImmutablePair<String,String>>  newSchema) {
    for (Map.Entry<String, ImmutablePair<String,String>> field : newSchema.entrySet()) {
      String name = field.getKey();
      StringBuilder sql = new StringBuilder();
      String type = field.getValue().getLeft();
      String comment = field.getValue().getRight();
      comment = comment.replace("'","");
      sql.append("ALTER TABLE ").append(HIVE_ESCAPE_CHARACTER)
              .append(config.databaseName).append(HIVE_ESCAPE_CHARACTER).append(".")
              .append(HIVE_ESCAPE_CHARACTER).append(tableName)
              .append(HIVE_ESCAPE_CHARACTER)
              .append(" CHANGE COLUMN `").append(name).append("` `").append(name)
              .append("` ").append(type).append(" comment '").append(comment).append("' ");
      runSQL(sql.toString());
    }
  }

  private List<String> constructAddPartitions(String tableName, List<String> partitions) {
    if (config.batchSyncNum <= 0) {
      throw new HoodieHiveSyncException("batch-sync-num for sync hive table must be greater than 0, pls check your parameter");
    }
    List<String> result = new ArrayList<>();
    int batchSyncPartitionNum = config.batchSyncNum;
    StringBuilder alterSQL = getAlterTablePrefix(tableName);
    for (int i = 0; i < partitions.size(); i++) {
      String partitionClause = getPartitionClause(partitions.get(i));
      String fullPartitionPath = FSUtils.getPartitionPath(config.basePath, partitions.get(i)).toString();
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

  @Override
  public void close() {
    if (client != null) {
      Hive.closeCurrent();
    }
  }

  private StringBuilder getAlterTablePrefix(String tableName) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(HIVE_ESCAPE_CHARACTER).append(config.databaseName)
        .append(HIVE_ESCAPE_CHARACTER).append(".").append(HIVE_ESCAPE_CHARACTER)
        .append(tableName).append(HIVE_ESCAPE_CHARACTER).append(" ADD IF NOT EXISTS ");
    return alterSQL;
  }

  public String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(config.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + config.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < config.partitionFields.size(); i++) {
      String partitionValue = partitionValues.get(i);
      // decode the partition before sync to hive to prevent multiple escapes of HIVE
      if (config.decodePartition) {
        // This is a decode operator for encode in KeyGenUtils#getRecordPartitionPath
        partitionValue = PartitionPathEncodeUtils.unescapePathName(partitionValue);
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

  @Override
  public List<String> extractPartitionValuesInPath(String storagePartition) {
    return this.partitionValueExtractor.extractPartitionValuesInPath(storagePartition);
  }

  /**
   * Scan table partitions.
   */
  @Override
  public List<Partition> scanTablePartitions(String databaseName, String tableName) throws TException {
    return client.listPartitions(databaseName, tableName, (short) -1);
  }

  @Override
  public boolean doesTableExist(String databaseName, String tableName) {
    try {
      return client.tableExists(databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String databaseName, String tableName) {
    // Get the last commit time from the TBLproperties
    try {
      Table table = client.getTable(databaseName, tableName);
      return Option.ofNullable(table.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the table " + tableName, e);
    }
  }

  @Override
  public Option<String> getLastReplicatedTime(String databaseName, String tableName) {
    // Get the last replicated time from the TBLproperties
    try {
      Table table = client.getTable(databaseName, tableName);
      return Option.ofNullable(table.getParameters().getOrDefault(GLOBALLY_CONSISTENT_READ_TIMESTAMP, null));
    } catch (NoSuchObjectException e) {
      LOG.warn("the said table not found in hms " + databaseName + "." + tableName);
      return Option.empty();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last replicated time from the table " + tableName, e);
    }
  }

  @Override
  public void updateLastReplicatedTimeStamp(HoodieTimeline activeTimeline, String databaseName, String tableName, String timeStamp) {
    if (!activeTimeline.filterCompletedInstants().getInstants()
            .anyMatch(i -> i.getTimestamp().equals(timeStamp))) {
      throw new HoodieHiveSyncException(
              "Not a valid completed timestamp " + timeStamp + " for table " + tableName);
    }
    try {
      Table table = client.getTable(databaseName, tableName);
      table.putToParameters(GLOBALLY_CONSISTENT_READ_TIMESTAMP, timeStamp);
      client.alter_table(databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
              "Failed to update last replicated time to " + timeStamp + " for " + tableName, e);
    }
  }

  @Override
  public void deleteLastReplicatedTimeStamp(String databaseName, String tableName) {
    try {
      Table table = client.getTable(databaseName, tableName);
      String timestamp = table.getParameters().remove(GLOBALLY_CONSISTENT_READ_TIMESTAMP);
      client.alter_table(databaseName, tableName, table);
      if (timestamp != null) {
        LOG.info("deleted last replicated timestamp " + timestamp + " for table " + tableName);
      }
    } catch (NoSuchObjectException e) {
      // this is ok the table doesn't even exist.
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
              "Failed to delete last replicated timestamp for " + tableName, e);
    }
  }

  @Override
  public void updateLastCommitTimeSynced(HoodieTimeline activeTimeline, String databaseName, String tableName) {
    // Set the last commit time from the TBLproperties
    Option<String> lastCommitSynced = activeTimeline.lastInstant().map(HoodieInstant::getTimestamp);
    if (lastCommitSynced.isPresent()) {
      try {
        Table table = client.getTable(databaseName, tableName);
        table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
        client.alter_table(databaseName, tableName, table);
      } catch (Exception e) {
        throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
      }
    }
  }

  @Override
  public Schema getAvroSchemaWithoutMetadataFields(HoodieTableMetaClient metaClient) {
    try {
      return new TableSchemaResolver(metaClient).getTableAvroSchemaWithoutMetadataFields();
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read avro schema", e);
    }
  }

  @Override
  public List<FieldSchema> getTableCommentUsingMetastoreClient(String databaseName, String tableName) {
    try {
      return client.getSchema(databaseName, tableName);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table comments for : " + tableName, e);
    }
  }

  /**
   * @param databaseName
   * @return true if the configured database exists
   */
  @Override
  public boolean doesDataBaseExist(String databaseName) {
    try {
      client.getDatabase(databaseName);
      return true;
    } catch (NoSuchObjectException noSuchObjectException) {
      // NoSuchObjectException is thrown when there is no existing database of the name.
      return false;
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if database exists " + databaseName, e);
    }
  }

  @Override
  public void updateTableProperties(String databaseName, String tableName, Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return;
    }
    try {
      Table table = client.getTable(databaseName, tableName);
      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        table.putToParameters(entry.getKey(), entry.getValue());
      }
      client.alter_table(databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table properties for table: "
              + tableName, e);
    }
  }

  public HiveSyncConfig getConfig() {
    return config;
  }

  public PartitionValueExtractor getPartitionValueExtractor() {
    return partitionValueExtractor;
  }

  public FileSystem getFs() {
    return fs;
  }

  public IMetaStoreClient getClient() {
    return client;
  }
}

