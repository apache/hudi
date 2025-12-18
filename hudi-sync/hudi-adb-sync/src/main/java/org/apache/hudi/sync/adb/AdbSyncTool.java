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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HadoopConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionEvent.PartitionEventType;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_AUTO_CREATE_DATABASE;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_DROP_TABLE_BEFORE_CREATION;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SERDE_PROPERTIES;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SKIP_RO_SUFFIX;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SKIP_RT_SYNC;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SUPPORT_TIMESTAMP;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE;
import static org.apache.hudi.sync.adb.AdbSyncConfig.ADB_SYNC_TABLE_PROPERTIES;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_SPARK_VERSION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

/**
 * Adb sync tool is mainly used to sync hoodie tables to Alibaba Cloud AnalyticDB(ADB),
 * it can be used as API `AdbSyncTool.syncHoodieTable(AdbSyncConfig)` or as command
 * line `java -cp hoodie-hive.jar AdbSyncTool [args]`
 *
 * <p>
 * This utility will get the schema from the latest commit and will sync ADB table schema,
 * incremental partitions will be synced as well.
 */
@SuppressWarnings("WeakerAccess")
public class AdbSyncTool extends HoodieSyncTool {
  private static final Logger LOG = LoggerFactory.getLogger(AdbSyncTool.class);

  public static final String SUFFIX_SNAPSHOT_TABLE = "_rt";
  public static final String SUFFIX_READ_OPTIMIZED_TABLE = "_ro";

  private final AdbSyncConfig config;
  private final String databaseName;
  private final String tableName;
  private final HoodieAdbJdbcClient syncClient;
  private final String snapshotTableName;
  private final Option<String> roTableTableName;

  public AdbSyncTool(Properties props) {
    this(props, HadoopConfigUtils.createHadoopConf(props), Option.empty());
  }

  public AdbSyncTool(Properties props, Configuration hadoopConf, Option<HoodieTableMetaClient> metaClientOption) {
    super(props, hadoopConf);
    this.config = new AdbSyncConfig(props);
    this.databaseName = config.getString(META_SYNC_DATABASE_NAME);
    this.tableName = config.getString(META_SYNC_TABLE_NAME);
    this.syncClient = new HoodieAdbJdbcClient(config, metaClientOption.orElseGet(() -> buildMetaClient(config)));
    switch (syncClient.getTableType()) {
      case COPY_ON_WRITE:
        this.snapshotTableName = tableName;
        this.roTableTableName = Option.empty();
        break;
      case MERGE_ON_READ:
        this.snapshotTableName = tableName + SUFFIX_SNAPSHOT_TABLE;
        this.roTableTableName = config.getBoolean(ADB_SYNC_SKIP_RO_SUFFIX) ? Option.of(tableName)
            : Option.of(tableName + SUFFIX_READ_OPTIMIZED_TABLE);
        break;
      default:
        throw new HoodieAdbSyncException("Unknown table type:" + syncClient.getTableType()
            + ", basePath:" + syncClient.getBasePath());
    }
  }

  @Override
  public void close() {
    if (syncClient != null) {
      syncClient.close();
    }
  }

  @Override
  public void syncHoodieTable() {
    try {
      switch (syncClient.getTableType()) {
        case COPY_ON_WRITE:
          syncHoodieTable(snapshotTableName, false, false);
          break;
        case MERGE_ON_READ:
          // Sync a ro table for MOR table
          syncHoodieTable(roTableTableName.get(), false, true);
          // Sync a rt table for MOR table
          if (!config.getBoolean(ADB_SYNC_SKIP_RT_SYNC)) {
            syncHoodieTable(snapshotTableName, true, false);
          }
          break;
        default:
          throw new HoodieAdbSyncException("Unknown table type:" + syncClient.getTableType()
              + ", basePath:" + syncClient.getBasePath());
      }
    } catch (Exception re) {
      throw new HoodieAdbSyncException("Sync hoodie table to ADB failed, tableName:" + tableName, re);
    } finally {
      syncClient.close();
    }
  }

  private void syncHoodieTable(String tableName, boolean useRealtimeInputFormat, boolean readAsOptimized) throws Exception {
    LOG.info("Try to sync hoodie table, tableName:{}, path:{}, tableType:{}",
        tableName, syncClient.getBasePath(), syncClient.getTableType());

    if (config.getBoolean(ADB_SYNC_AUTO_CREATE_DATABASE)) {
      try {
        synchronized (AdbSyncTool.class) {
          if (!syncClient.databaseExists(databaseName)) {
            syncClient.createDatabase(databaseName);
          }
        }
      } catch (Exception e) {
        throw new HoodieAdbSyncException("Failed to create database:" + databaseName
            + ", useRealtimeInputFormat = " + useRealtimeInputFormat, e);
      }
    } else if (!syncClient.databaseExists(databaseName)) {
      throw new HoodieAdbSyncException("ADB database does not exists:" + databaseName);
    }

    if (config.getBoolean(ADB_SYNC_DROP_TABLE_BEFORE_CREATION)) {
      LOG.info("Drop table before creation, tableName:{}", tableName);
      syncClient.dropTable(tableName);
    }

    boolean tableExists = syncClient.tableExists(tableName);

    // Get the parquet schema for this table looking at the latest commit
    HoodieSchema schema = syncClient.getStorageSchema();

    // Sync schema if needed
    syncSchema(tableName, tableExists, useRealtimeInputFormat, readAsOptimized, schema);
    LOG.info("Sync schema complete, start syncing partitions for table:{}", tableName);

    // Get the last time we successfully synced partitions
    Option<String> lastCommitTimeSynced = Option.empty();
    if (tableExists) {
      lastCommitTimeSynced = syncClient.getLastCommitTimeSynced(tableName);
    }
    LOG.info("Last commit time synced was found:{}", lastCommitTimeSynced.orElse("null"));

    // Scan synced partitions
    List<String> writtenPartitionsSince;
    if (config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
      writtenPartitionsSince = new ArrayList<>();
    } else {
      writtenPartitionsSince = syncClient.getWrittenPartitionsSince(lastCommitTimeSynced, Option.empty());
    }
    LOG.info("Scan partitions complete, partitionNum:{}", writtenPartitionsSince.size());

    // Sync the partitions if needed
    syncPartitions(tableName, writtenPartitionsSince);

    // Update sync commit time
    // whether to skip syncing commit time stored in tbl properties, since it is time-consuming.
    if (!config.getBoolean(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC)) {
      syncClient.updateLastCommitTimeSynced(tableName);
    }
    LOG.info("Sync complete for table:{}", tableName);
  }

  /**
   * Get the latest schema from the last commit and check if its in sync with the ADB
   * table schema. If not, evolves the table schema.
   *
   * @param tableName              The table to be synced
   * @param tableExists            Whether target table exists
   * @param useRealTimeInputFormat Whether using realtime input format
   * @param readAsOptimized        Whether read as optimized table
   * @param schema                 The extracted schema
   */
  private void syncSchema(String tableName, boolean tableExists, boolean useRealTimeInputFormat,
      boolean readAsOptimized, HoodieSchema schema) {
    // Append spark table properties & serde properties
    Map<String, String> tableProperties = ConfigUtils.toMap(config.getString(ADB_SYNC_TABLE_PROPERTIES));
    Map<String, String> serdeProperties = ConfigUtils.toMap(config.getString(ADB_SYNC_SERDE_PROPERTIES));
    if (config.getBoolean(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE)) {
      Map<String, String> sparkTableProperties = SparkDataSourceTableUtils.getSparkTableProperties(config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          config.getString(META_SYNC_SPARK_VERSION), config.getInt(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD), schema);
      Map<String, String> sparkSerdeProperties = SparkDataSourceTableUtils.getSparkSerdeProperties(readAsOptimized, config.getString(META_SYNC_BASE_PATH));
      tableProperties.putAll(sparkTableProperties);
      serdeProperties.putAll(sparkSerdeProperties);
      LOG.info("Sync as spark datasource table, tableName:{}, tableExists:{}, tableProperties:{}, sederProperties:{}",
          tableName, tableExists, tableProperties, serdeProperties);
    }

    // Check and sync schema
    if (!tableExists) {
      LOG.info("ADB table [{}] is not found, creating it", tableName);
      String inputFormatClassName = HoodieInputFormatUtils.getInputFormatClassName(HoodieFileFormat.PARQUET, useRealTimeInputFormat);

      // Custom serde will not work with ALTER TABLE REPLACE COLUMNS
      // https://github.com/apache/hive/blob/release-1.1.0/ql/src/java/org/apache/hadoop/hive
      // /ql/exec/DDLTask.java#L3488
      syncClient.createTable(tableName, schema, inputFormatClassName, MapredParquetOutputFormat.class.getName(),
          ParquetHiveSerDe.class.getName(), serdeProperties, tableProperties);
    } else {
      // Check if the table schema has evolved
      Map<String, String> tableSchema = syncClient.getMetastoreSchema(tableName);
      SchemaDifference schemaDiff = HiveSchemaUtil.getSchemaDifference(schema, tableSchema, config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          config.getBoolean(ADB_SYNC_SUPPORT_TIMESTAMP));
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for table:{}", tableName);
        syncClient.updateTableDefinition(tableName, schemaDiff);
      } else {
        LOG.info("No Schema difference for table:{}", tableName);
      }
    }
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in adb, if not adds it or if the
   * partition path does not match, it updates the partition path).
   */
  private void syncPartitions(String tableName, List<String> writtenPartitionsSince) {
    try {
      if (config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
        LOG.info("Not a partitioned table.");
        return;
      }

      Map<List<String>, String> partitions = syncClient.scanTablePartitions(tableName);
      List<PartitionEvent> partitionEvents = syncClient.getPartitionEvents(partitions, writtenPartitionsSince);
      List<String> newPartitions = filterPartitions(partitionEvents, PartitionEventType.ADD);
      LOG.info("New Partitions:{}", newPartitions);
      syncClient.addPartitionsToTable(tableName, newPartitions);
      List<String> updatePartitions = filterPartitions(partitionEvents, PartitionEventType.UPDATE);
      LOG.info("Changed Partitions:{}", updatePartitions);
      syncClient.updatePartitionsToTable(tableName, updatePartitions);
    } catch (Exception e) {
      throw new HoodieAdbSyncException("Failed to sync partitions for table:" + tableName, e);
    }
  }

  private List<String> filterPartitions(List<PartitionEvent> events, PartitionEventType eventType) {
    return events.stream().filter(s -> s.eventType == eventType)
        .map(s -> s.storagePartition).collect(Collectors.toList());
  }

  public static void main(String[] args) {
    final AdbSyncConfig.AdbSyncConfigParams params = new AdbSyncConfig.AdbSyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
      System.exit(0);
    }
    new AdbSyncTool(params.toProps()).syncHoodieTable();
  }
}
