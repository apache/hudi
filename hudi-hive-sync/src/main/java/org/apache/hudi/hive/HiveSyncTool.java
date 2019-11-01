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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.HoodieHiveClient.PartitionEvent;
import org.apache.hudi.hive.HoodieHiveClient.PartitionEvent.PartitionEventType;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tool to sync a hoodie HDFS table with a hive metastore table. Either use it as a api
 * HiveSyncTool.syncHoodieTable(HiveSyncConfig) or as a command line java -cp hoodie-hive-sync.jar HiveSyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync hive table schema Also this will sync the
 * partitions incrementally (all the partitions modified since the last commit)
 */
@SuppressWarnings("WeakerAccess")
public class HiveSyncTool {

  private static final Logger LOG = LogManager.getLogger(HiveSyncTool.class);
  public static final String SUFFIX_SNAPSHOT_TABLE = "_rt";
  public static final String SUFFIX_READ_OPTIMIZED_TABLE = "_ro";

  private final HiveSyncConfig cfg;
  private final HoodieHiveClient hoodieHiveClient;
  private final String snapshotTableName;
  private final Option<String> roTableTableName;

  public HiveSyncTool(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    this.hoodieHiveClient = new HoodieHiveClient(cfg, configuration, fs);
    this.cfg = cfg;
    // Set partitionFields to empty, when the NonPartitionedExtractor is used
    if (NonPartitionedExtractor.class.getName().equals(cfg.partitionValueExtractorClass)) {
      LOG.warn("Set partitionFields to empty, since the NonPartitionedExtractor is used");
      cfg.partitionFields = new ArrayList<>();
    }
    switch (hoodieHiveClient.getTableType()) {
      case COPY_ON_WRITE:
        this.snapshotTableName = cfg.tableName;
        this.roTableTableName = Option.empty();
        break;
      case MERGE_ON_READ:
        this.snapshotTableName = cfg.tableName + SUFFIX_SNAPSHOT_TABLE;
        this.roTableTableName = cfg.skipROSuffix ? Option.of(cfg.tableName) :
            Option.of(cfg.tableName + SUFFIX_READ_OPTIMIZED_TABLE);
        break;
      default:
        LOG.error("Unknown table type " + hoodieHiveClient.getTableType());
        throw new InvalidTableException(hoodieHiveClient.getBasePath());
    }
  }

  public void syncHoodieTable() {
    try {
      switch (hoodieHiveClient.getTableType()) {
        case COPY_ON_WRITE:
          syncHoodieTable(snapshotTableName, false);
          break;
        case MERGE_ON_READ:
          // sync a RO table for MOR
          syncHoodieTable(roTableTableName.get(), false);
          // sync a RT table for MOR
          syncHoodieTable(snapshotTableName, true);
          break;
        default:
          LOG.error("Unknown table type " + hoodieHiveClient.getTableType());
          throw new InvalidTableException(hoodieHiveClient.getBasePath());
      }
    } catch (RuntimeException re) {
      LOG.error("Got runtime exception when hive syncing", re);
    } finally {
      hoodieHiveClient.close();
    }
  }

  private void syncHoodieTable(String tableName, boolean useRealtimeInputFormat) {
    LOG.info("Trying to sync hoodie table " + tableName + " with base path " + hoodieHiveClient.getBasePath()
        + " of type " + hoodieHiveClient.getTableType());
    // Check if the necessary table exists
    boolean tableExists = hoodieHiveClient.doesTableExist(tableName);

    // check if the database exists else create it
    try {
      hoodieHiveClient.updateHiveSQL("create database if not exists " + cfg.databaseName);
    } catch (Exception e) {
      // this is harmless since table creation will fail anyways, creation of DB is needed for in-memory testing
      LOG.warn("Unable to create database", e);
    }

    // Get the parquet schema for this table looking at the latest commit
    MessageType schema = hoodieHiveClient.getDataSchema();
    // Sync schema if needed
    syncSchema(tableName, tableExists, useRealtimeInputFormat, schema);

    LOG.info("Schema sync complete. Syncing partitions for " + tableName);
    // Get the last time we successfully synced partitions
    Option<String> lastCommitTimeSynced = Option.empty();
    if (tableExists) {
      lastCommitTimeSynced = hoodieHiveClient.getLastCommitTimeSynced(tableName);
    }
    LOG.info("Last commit time synced was found to be " + lastCommitTimeSynced.orElse("null"));
    List<String> writtenPartitionsSince = hoodieHiveClient.getPartitionsWrittenToSince(lastCommitTimeSynced);
    LOG.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());
    // Sync the partitions if needed
    syncPartitions(tableName, writtenPartitionsSince);

    hoodieHiveClient.updateLastCommitTimeSynced(tableName);
    LOG.info("Sync complete for " + tableName);
  }

  /**
   * Get the latest schema from the last commit and check if its in sync with the hive table schema. If not, evolves the
   * table schema.
   *
   * @param tableExists - does table exist
   * @param schema - extracted schema
   */
  private void syncSchema(String tableName, boolean tableExists, boolean useRealTimeInputFormat, MessageType schema) {
    // Check and sync schema
    if (!tableExists) {
      LOG.info("Hive table " + tableName + " is not found. Creating it");
      HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(cfg.baseFileFormat.toUpperCase());
      String inputFormatClassName = HoodieInputFormatUtils.getInputFormatClassName(baseFileFormat, useRealTimeInputFormat,
          new Configuration());

      if (baseFileFormat.equals(HoodieFileFormat.PARQUET) && cfg.usePreApacheInputFormat) {
        // Parquet input format had an InputFormat class visible under the old naming scheme.
        inputFormatClassName = useRealTimeInputFormat
            ? com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat.class.getName()
            : com.uber.hoodie.hadoop.HoodieInputFormat.class.getName();
      }

      String outputFormatClassName = HoodieInputFormatUtils.getOutputFormatClassName(baseFileFormat);
      String serDeFormatClassName = HoodieInputFormatUtils.getSerDeClassName(baseFileFormat);

      // Custom serde will not work with ALTER TABLE REPLACE COLUMNS
      // https://github.com/apache/hive/blob/release-1.1.0/ql/src/java/org/apache/hadoop/hive
      // /ql/exec/DDLTask.java#L3488
      hoodieHiveClient.createTable(tableName, schema, inputFormatClassName, outputFormatClassName, serDeFormatClassName);
    } else {
      // Check if the table schema has evolved
      Map<String, String> tableSchema = hoodieHiveClient.getTableSchema(tableName);
      SchemaDifference schemaDiff = HiveSchemaUtil.getSchemaDifference(schema, tableSchema, cfg.partitionFields);
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for " + tableName);
        hoodieHiveClient.updateTableDefinition(tableName, schema);
      } else {
        LOG.info("No Schema difference for " + tableName);
      }
    }
  }

  /**
   * Syncs the list of storage parititions passed in (checks if the partition is in hive, if not adds it or if the
   * partition path does not match, it updates the partition path).
   */
  private void syncPartitions(String tableName, List<String> writtenPartitionsSince) {
    try {
      List<Partition> hivePartitions = hoodieHiveClient.scanTablePartitions(tableName);
      List<PartitionEvent> partitionEvents =
          hoodieHiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
      List<String> newPartitions = filterPartitions(partitionEvents, PartitionEventType.ADD);
      LOG.info("New Partitions " + newPartitions);
      hoodieHiveClient.addPartitionsToTable(tableName, newPartitions);
      List<String> updatePartitions = filterPartitions(partitionEvents, PartitionEventType.UPDATE);
      LOG.info("Changed Partitions " + updatePartitions);
      hoodieHiveClient.updatePartitionsToTable(tableName, updatePartitions);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to sync partitions for table " + tableName, e);
    }
  }

  private List<String> filterPartitions(List<PartitionEvent> events, PartitionEventType eventType) {
    return events.stream().filter(s -> s.eventType == eventType).map(s -> s.storagePartition)
        .collect(Collectors.toList());
  }

  public static void main(String[] args) {
    // parse the params
    final HiveSyncConfig cfg = new HiveSyncConfig();
    JCommander cmd = new JCommander(cfg, null, args);
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
