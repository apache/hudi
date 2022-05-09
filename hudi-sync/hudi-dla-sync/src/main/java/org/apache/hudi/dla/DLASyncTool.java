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

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.dla.util.Utils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tool to sync a hoodie table with a dla table. Either use it as a api
 * DLASyncTool.syncHoodieTable(DLASyncConfig) or as a command line java -cp hoodie-hive.jar DLASyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync dla table schema Also this will sync the
 * partitions incrementally (all the partitions modified since the last commit)
 */
@SuppressWarnings("WeakerAccess")
public class DLASyncTool extends AbstractSyncTool {

  private static final Logger LOG = LogManager.getLogger(DLASyncTool.class);
  public static final String SUFFIX_SNAPSHOT_TABLE = "_rt";
  public static final String SUFFIX_READ_OPTIMIZED_TABLE = "_ro";

  private final DLASyncConfig cfg;
  private final HoodieDLAClient hoodieDLAClient;
  private final String snapshotTableName;
  private final Option<String> roTableTableName;

  public DLASyncTool(TypedProperties properties, Configuration conf, FileSystem fs) {
    super(properties, conf, fs);
    this.hoodieDLAClient = new HoodieDLAClient(Utils.propertiesToConfig(properties), fs);
    this.cfg = Utils.propertiesToConfig(properties);
    switch (hoodieDLAClient.getTableType()) {
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
        LOG.error("Unknown table type " + hoodieDLAClient.getTableType());
        throw new InvalidTableException(hoodieDLAClient.getBasePath());
    }
  }

  @Override
  public void syncHoodieTable() {
    try {
      switch (hoodieDLAClient.getTableType()) {
        case COPY_ON_WRITE:
          syncHoodieTable(snapshotTableName, false);
          break;
        case MERGE_ON_READ:
          // sync a RO table for MOR
          syncHoodieTable(roTableTableName.get(), false);
          // sync a RT table for MOR
          if (!cfg.skipRTSync) {
            syncHoodieTable(snapshotTableName, true);
          }
          break;
        default:
          LOG.error("Unknown table type " + hoodieDLAClient.getTableType());
          throw new InvalidTableException(hoodieDLAClient.getBasePath());
      }
    } catch (RuntimeException re) {
      throw new HoodieException("Got runtime exception when dla syncing " + cfg.tableName, re);
    } finally {
      hoodieDLAClient.close();
    }
  }

  private void syncHoodieTable(String tableName, boolean useRealtimeInputFormat) {
    LOG.info("Trying to sync hoodie table " + tableName + " with base path " + hoodieDLAClient.getBasePath()
        + " of type " + hoodieDLAClient.getTableType());
    // Check if the necessary table exists
    boolean tableExists = hoodieDLAClient.tableExists(tableName);
    // Get the parquet schema for this table looking at the latest commit
    MessageType schema = hoodieDLAClient.getDataSchema();
    // Sync schema if needed
    syncSchema(tableName, tableExists, useRealtimeInputFormat, schema);

    LOG.info("Schema sync complete. Syncing partitions for " + tableName);
    // Get the last time we successfully synced partitions
    // TODO : once DLA supports alter table properties
    Option<String> lastCommitTimeSynced = Option.empty();
    /*if (tableExists) {
      lastCommitTimeSynced = hoodieDLAClient.getLastCommitTimeSynced(tableName);
    }*/
    LOG.info("Last commit time synced was found to be " + lastCommitTimeSynced.orElse("null"));
    List<String> writtenPartitionsSince = hoodieDLAClient.getPartitionsWrittenToSince(lastCommitTimeSynced);
    LOG.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());
    // Sync the partitions if needed
    syncPartitions(tableName, writtenPartitionsSince);

    hoodieDLAClient.updateLastCommitTimeSynced(tableName);
    LOG.info("Sync complete for " + tableName);
  }

  /**
   * Get the latest schema from the last commit and check if its in sync with the dla table schema. If not, evolves the
   * table schema.
   *
   * @param tableExists - does table exist
   * @param schema - extracted schema
   */
  private void syncSchema(String tableName, boolean tableExists, boolean useRealTimeInputFormat, MessageType schema) {
    // Check and sync schema
    if (!tableExists) {
      LOG.info("DLA table " + tableName + " is not found. Creating it");

      String inputFormatClassName = HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, useRealTimeInputFormat);

      // Custom serde will not work with ALTER TABLE REPLACE COLUMNS
      // https://github.com/apache/hive/blob/release-1.1.0/ql/src/java/org/apache/hadoop/hive
      // /ql/exec/DDLTask.java#L3488
      hoodieDLAClient.createTable(tableName, schema, inputFormatClassName, MapredParquetOutputFormat.class.getName(),
          ParquetHiveSerDe.class.getName(), new HashMap<>(), new HashMap<>());
    } else {
      // Check if the table schema has evolved
      Map<String, String> tableSchema = hoodieDLAClient.getTableSchema(tableName);
      SchemaDifference schemaDiff = HiveSchemaUtil.getSchemaDifference(schema, tableSchema, cfg.partitionFields, cfg.supportTimestamp);
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for " + tableName);
        hoodieDLAClient.updateTableDefinition(tableName, schemaDiff);
      } else {
        LOG.info("No Schema difference for " + tableName);
      }
    }
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in dla, if not adds it or if the
   * partition path does not match, it updates the partition path).
   */
  private void syncPartitions(String tableName, List<String> writtenPartitionsSince) {
    try {
      if (cfg.partitionFields.isEmpty()) {
        LOG.info("not a partitioned table.");
        return;
      }
      Map<List<String>, String> partitions = hoodieDLAClient.scanTablePartitions(tableName);
      List<AbstractSyncHoodieClient.PartitionEvent> partitionEvents =
          hoodieDLAClient.getPartitionEvents(partitions, writtenPartitionsSince);
      List<String> newPartitions = filterPartitions(partitionEvents, AbstractSyncHoodieClient.PartitionEvent.PartitionEventType.ADD);
      LOG.info("New Partitions " + newPartitions);
      hoodieDLAClient.addPartitionsToTable(tableName, newPartitions);
      List<String> updatePartitions = filterPartitions(partitionEvents, AbstractSyncHoodieClient.PartitionEvent.PartitionEventType.UPDATE);
      LOG.info("Changed Partitions " + updatePartitions);
      hoodieDLAClient.updatePartitionsToTable(tableName, updatePartitions);
    } catch (Exception e) {
      throw new HoodieException("Failed to sync partitions for table " + tableName, e);
    }
  }

  private List<String> filterPartitions(List<AbstractSyncHoodieClient.PartitionEvent> events, AbstractSyncHoodieClient.PartitionEvent.PartitionEventType eventType) {
    return events.stream().filter(s -> s.eventType == eventType).map(s -> s.storagePartition)
        .collect(Collectors.toList());
  }

  public static void main(String[] args) {
    // parse the params
    final DLASyncConfig cfg = new DLASyncConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    Configuration hadoopConf = new Configuration();
    FileSystem fs = FSUtils.getFs(cfg.basePath, hadoopConf);
    new DLASyncTool(Utils.configToProperties(cfg), hadoopConf, fs).syncHoodieTable();
  }
}
