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

package org.apache.hudi.hive;

import com.beust.jcommander.JCommander;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.InvalidDatasetException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hive.HoodieHiveClient.PartitionEvent;
import org.apache.hudi.hive.HoodieHiveClient.PartitionEvent.PartitionEventType;
import org.apache.hudi.hive.util.SchemaUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;


/**
 * Tool to sync a hoodie HDFS dataset with a hive metastore table. Either use it as a api
 * HiveSyncTool.syncHoodieTable(HiveSyncConfig) or as a command line java -cp hoodie-hive.jar HiveSyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync hive table schema Also this will sync the
 * partitions incrementally (all the partitions modified since the last commit)
 */
@SuppressWarnings("WeakerAccess")
public class HiveSyncTool {

  private static Logger LOG = LogManager.getLogger(HiveSyncTool.class);
  private final HoodieHiveClient hoodieHiveClient;
  public static final String SUFFIX_REALTIME_TABLE = "_rt";
  private final HiveSyncConfig cfg;

  public HiveSyncTool(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    this.hoodieHiveClient = new HoodieHiveClient(cfg, configuration, fs);
    this.cfg = cfg;
  }

  public void syncHoodieTable() throws ClassNotFoundException {
    try {
      switch (hoodieHiveClient.getTableType()) {
        case COPY_ON_WRITE:
          syncHoodieTable(false);
          break;
        case MERGE_ON_READ:
          // sync a RO table for MOR
          syncHoodieTable(false);
          String originalTableName = cfg.tableName;
          // TODO : Make realtime table registration optional using a config param
          cfg.tableName = cfg.tableName + SUFFIX_REALTIME_TABLE;
          // sync a RT table for MOR
          syncHoodieTable(true);
          cfg.tableName = originalTableName;
          break;
        default:
          LOG.error("Unknown table type " + hoodieHiveClient.getTableType());
          throw new InvalidDatasetException(hoodieHiveClient.getBasePath());
      }
    } catch (RuntimeException re) {
      LOG.error("Got runtime exception when hive syncing", re);
    } finally {
      hoodieHiveClient.close();
    }
  }

  private void syncHoodieTable(boolean isRealTime) throws ClassNotFoundException {
    LOG.info("Trying to sync hoodie table " + cfg.tableName + " with base path " + hoodieHiveClient.getBasePath()
        + " of type " + hoodieHiveClient.getTableType());

    // Check if the necessary table exists
    boolean tableExists = hoodieHiveClient.doesTableExist();
    // check if the database exists else create it
    hoodieHiveClient.updateHiveSQL("create database if not exists " + cfg.databaseName);
    // Get the parquet schema for this dataset looking at the latest commit
    MessageType schema = hoodieHiveClient.getDataSchema();
    // Sync schema if needed
    syncSchema(tableExists, isRealTime, schema);

    LOG.info("Schema sync complete. Syncing partitions for " + cfg.tableName);
    // Get the last time we successfully synced partitions
    Option<String> lastCommitTimeSynced = Option.empty();
    if (tableExists) {
      lastCommitTimeSynced = hoodieHiveClient.getLastCommitTimeSynced();
    }
    LOG.info("Last commit time synced was found to be " + lastCommitTimeSynced.orElse("null"));
    List<String> writtenPartitionsSince = hoodieHiveClient.getPartitionsWrittenToSince(lastCommitTimeSynced);
    LOG.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());
    // Sync the partitions if needed
    syncPartitions(writtenPartitionsSince);

    hoodieHiveClient.updateLastCommitTimeSynced();
    LOG.info("Sync complete for " + cfg.tableName);
  }

  /**
   * Get the latest schema from the last commit and check if its in sync with the hive table schema. If not, evolves the
   * table schema.
   *
   * @param tableExists - does table exist
   * @param schema - extracted schema
   */
  private void syncSchema(boolean tableExists, boolean isRealTime, MessageType schema) throws ClassNotFoundException {
    // Check and sync schema
    if (!tableExists) {
      LOG.info("Table " + cfg.tableName + " is not found. Creating it");
      if (!isRealTime) {
        // TODO - RO Table for MOR only after major compaction (UnboundedCompaction is default
        // for now)
        String inputFormatClassName =
            cfg.usePreApacheInputFormat ? com.uber.hoodie.hadoop.HoodieInputFormat.class.getName()
                : HoodieParquetInputFormat.class.getName();
        hoodieHiveClient.createTable(schema, inputFormatClassName, MapredParquetOutputFormat.class.getName(),
            ParquetHiveSerDe.class.getName());
      } else {
        // Custom serde will not work with ALTER TABLE REPLACE COLUMNS
        // https://github.com/apache/hive/blob/release-1.1.0/ql/src/java/org/apache/hadoop/hive
        // /ql/exec/DDLTask.java#L3488
        String inputFormatClassName =
            cfg.usePreApacheInputFormat ? com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat.class.getName()
                : HoodieParquetRealtimeInputFormat.class.getName();
        hoodieHiveClient.createTable(schema, inputFormatClassName, MapredParquetOutputFormat.class.getName(),
            ParquetHiveSerDe.class.getName());
      }
    } else {
      // Check if the dataset schema has evolved
      Map<String, String> tableSchema = hoodieHiveClient.getTableSchema();
      SchemaDifference schemaDiff = SchemaUtil.getSchemaDifference(schema, tableSchema, cfg.partitionFields);
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for " + cfg.tableName);
        hoodieHiveClient.updateTableDefinition(schema);
      } else {
        LOG.info("No Schema difference for " + cfg.tableName);
      }
    }
  }

  /**
   * Syncs the list of storage parititions passed in (checks if the partition is in hive, if not adds it or if the
   * partition path does not match, it updates the partition path)
   */
  private void syncPartitions(List<String> writtenPartitionsSince) {
    try {
      List<Partition> hivePartitions = hoodieHiveClient.scanTablePartitions();
      List<PartitionEvent> partitionEvents =
          hoodieHiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
      List<String> newPartitions = filterPartitions(partitionEvents, PartitionEventType.ADD);
      LOG.info("New Partitions " + newPartitions);
      hoodieHiveClient.addPartitionsToTable(newPartitions);
      List<String> updatePartitions = filterPartitions(partitionEvents, PartitionEventType.UPDATE);
      LOG.info("Changed Partitions " + updatePartitions);
      hoodieHiveClient.updatePartitionsToTable(updatePartitions);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to sync partitions for table " + cfg.tableName, e);
    }
  }

  private List<String> filterPartitions(List<PartitionEvent> events, PartitionEventType eventType) {
    return events.stream().filter(s -> s.eventType == eventType).map(s -> s.storagePartition)
        .collect(Collectors.toList());
  }

  public static void main(String[] args) throws Exception {
    // parse the params
    final HiveSyncConfig cfg = new HiveSyncConfig();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    FileSystem fs = FSUtils.getFs(cfg.basePath, new Configuration());
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(fs.getConf());
    new HiveSyncTool(cfg, hiveConf, fs).syncHoodieTable();
  }
}
