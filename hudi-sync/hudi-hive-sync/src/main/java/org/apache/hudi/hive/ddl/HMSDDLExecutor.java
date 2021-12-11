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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DDLExecutor impl based on HMS which use HMS apis directly for all DDL tasks.
 */
public class HMSDDLExecutor implements DDLExecutor {
  private static final Logger LOG = LogManager.getLogger(HMSDDLExecutor.class);
  private final HiveSyncConfig syncConfig;
  private final PartitionValueExtractor partitionValueExtractor;
  private final FileSystem fs;
  private final IMetaStoreClient client;

  public HMSDDLExecutor(HiveConf conf, HiveSyncConfig syncConfig, FileSystem fs) throws HiveException, MetaException {
    this.client = Hive.get(conf).getMSC();
    this.syncConfig = syncConfig;
    this.fs = fs;
    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(syncConfig.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + syncConfig.partitionValueExtractorClass, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      Database database = new Database(databaseName, "automatically created by hoodie", null, null);
      client.createDatabase(database);
    } catch (Exception e) {
      LOG.error("Failed to create database " + databaseName, e);
      throw new HoodieHiveSyncException("Failed to create database " + databaseName, e);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass, Map<String, String> serdeProperties,
                          Map<String, String> tableProperties) {
    try {
      LinkedHashMap<String, String> mapSchema = HiveSchemaUtil.parquetSchemaToMapSchema(storageSchema, syncConfig.supportTimestamp, false);

      List<FieldSchema> fieldSchema = HiveSchemaUtil.convertMapSchemaToHiveFieldSchema(mapSchema, syncConfig);

      List<FieldSchema> partitionSchema = syncConfig.partitionFields.stream().map(partitionKey -> {
        String partitionKeyType = HiveSchemaUtil.getPartitionKeyType(mapSchema, partitionKey);
        return new FieldSchema(partitionKey, partitionKeyType.toLowerCase(), "");
      }).collect(Collectors.toList());
      Table newTb = new Table();
      newTb.setDbName(syncConfig.databaseName);
      newTb.setTableName(tableName);
      newTb.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      newTb.setCreateTime((int) System.currentTimeMillis());
      StorageDescriptor storageDescriptor = new StorageDescriptor();
      storageDescriptor.setCols(fieldSchema);
      storageDescriptor.setInputFormat(inputFormatClass);
      storageDescriptor.setOutputFormat(outputFormatClass);
      storageDescriptor.setLocation(syncConfig.basePath);
      serdeProperties.put("serialization.format", "1");
      storageDescriptor.setSerdeInfo(new SerDeInfo(null, serdeClass, serdeProperties));
      newTb.setSd(storageDescriptor);
      newTb.setPartitionKeys(partitionSchema);

      if (!syncConfig.createManagedTable) {
        newTb.putToParameters("EXTERNAL", "TRUE");
      }

      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        newTb.putToParameters(entry.getKey(), entry.getValue());
      }
      newTb.setTableType(TableType.EXTERNAL_TABLE.toString());
      client.createTable(newTb);
    } catch (Exception e) {
      LOG.error("failed to create table " + tableName, e);
      throw new HoodieHiveSyncException("failed to create table " + tableName, e);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      boolean cascade = syncConfig.partitionFields.size() > 0;
      List<FieldSchema> fieldSchema = HiveSchemaUtil.convertParquetSchemaToHiveFieldSchema(newSchema, syncConfig);
      Table table = client.getTable(syncConfig.databaseName, tableName);
      StorageDescriptor sd = table.getSd();
      sd.setCols(fieldSchema);
      table.setSd(sd);
      EnvironmentContext environmentContext = new EnvironmentContext();
      if (cascade) {
        LOG.info("partition table,need cascade");
        environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
      }
      client.alter_table_with_environmentContext(syncConfig.databaseName, tableName, table, environmentContext);
    } catch (Exception e) {
      LOG.error("Failed to update table for " + tableName, e);
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public Map<String, String> getTableSchema(String tableName) {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = this.client.getTable(syncConfig.databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      LOG.info(String.format("Time taken to getTableSchema: %s ms", (end - start)));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    try {
      StorageDescriptor sd = client.getTable(syncConfig.databaseName, tableName).getSd();
      List<Partition> partitionList = partitionsToAdd.stream().map(partition -> {
        StorageDescriptor partitionSd = new StorageDescriptor();
        partitionSd.setCols(sd.getCols());
        partitionSd.setInputFormat(sd.getInputFormat());
        partitionSd.setOutputFormat(sd.getOutputFormat());
        partitionSd.setSerdeInfo(sd.getSerdeInfo());
        String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        partitionSd.setLocation(fullPartitionPath);
        return new Partition(partitionValues, syncConfig.databaseName, tableName, 0, 0, partitionSd, null);
      }).collect(Collectors.toList());
      client.add_partitions(partitionList, true, false);
    } catch (TException e) {
      LOG.error(syncConfig.databaseName + "." + tableName + " add partition failed", e);
      throw new HoodieHiveSyncException(syncConfig.databaseName + "." + tableName + " add partition failed", e);
    }
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    try {
      StorageDescriptor sd = client.getTable(syncConfig.databaseName, tableName).getSd();
      List<Partition> partitionList = changedPartitions.stream().map(partition -> {
        Path partitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition);
        String partitionScheme = partitionPath.toUri().getScheme();
        String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
            ? FSUtils.getDFSFullPartitionPath(fs, partitionPath) : partitionPath.toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        sd.setLocation(fullPartitionPath);
        return new Partition(partitionValues, syncConfig.databaseName, tableName, 0, 0, sd, null);
      }).collect(Collectors.toList());
      client.alter_partitions(syncConfig.databaseName, tableName, partitionList, null);
    } catch (TException e) {
      LOG.error(syncConfig.databaseName + "." + tableName + " update partition failed", e);
      throw new HoodieHiveSyncException(syncConfig.databaseName + "." + tableName + " update partition failed", e);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      Hive.closeCurrent();
    }
  }
}
