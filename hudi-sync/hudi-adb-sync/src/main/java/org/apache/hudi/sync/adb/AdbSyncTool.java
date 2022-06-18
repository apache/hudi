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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionEvent.PartitionEventType;
import org.apache.hudi.sync.common.util.ConfigUtils;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  private final AdbSyncConfig adbSyncConfig;
  private final String databaseName;
  private final String tableName;
  private final HoodieAdbJdbcClient hoodieAdbClient;
  private final String snapshotTableName;
  private final Option<String> roTableTableName;

  public AdbSyncTool(AdbSyncConfig config) {
    super(config);
    this.adbSyncConfig = config;
    this.databaseName = config.getString(META_SYNC_DATABASE_NAME);
    this.tableName = config.getString(META_SYNC_TABLE_NAME);
    this.hoodieAdbClient = new HoodieAdbJdbcClient(adbSyncConfig);
    switch (hoodieAdbClient.getTableType()) {
      case COPY_ON_WRITE:
        this.snapshotTableName = tableName;
        this.roTableTableName = Option.empty();
        break;
      case MERGE_ON_READ:
        this.snapshotTableName = tableName + SUFFIX_SNAPSHOT_TABLE;
        this.roTableTableName = adbSyncConfig.getBoolean(ADB_SYNC_SKIP_RO_SUFFIX) ? Option.of(tableName)
            : Option.of(tableName + SUFFIX_READ_OPTIMIZED_TABLE);
        break;
      default:
        throw new HoodieAdbSyncException("Unknown table type:" + hoodieAdbClient.getTableType()
            + ", basePath:" + hoodieAdbClient.getBasePath());
    }
  }

  @Override
  public void close() {
    if (hoodieAdbClient != null) {
      hoodieAdbClient.close();
    }
  }

  @Override
  public void syncHoodieTable() {
    try {
      switch (hoodieAdbClient.getTableType()) {
        case COPY_ON_WRITE:
          syncHoodieTable(snapshotTableName, false, false);
          break;
        case MERGE_ON_READ:
          // Sync a ro table for MOR table
          syncHoodieTable(roTableTableName.get(), false, true);
          // Sync a rt table for MOR table
          if (!adbSyncConfig.getBoolean(ADB_SYNC_SKIP_RT_SYNC)) {
            syncHoodieTable(snapshotTableName, true, false);
          }
          break;
        default:
          throw new HoodieAdbSyncException("Unknown table type:" + hoodieAdbClient.getTableType()
              + ", basePath:" + hoodieAdbClient.getBasePath());
      }
    } catch (Exception re) {
      throw new HoodieAdbSyncException("Sync hoodie table to ADB failed, tableName:" + tableName, re);
    } finally {
      hoodieAdbClient.close();
    }
  }

  private void syncHoodieTable(String tableName, boolean useRealtimeInputFormat, boolean readAsOptimized) throws Exception {
    LOG.info("Try to sync hoodie table, tableName:{}, path:{}, tableType:{}",
        tableName, hoodieAdbClient.getBasePath(), hoodieAdbClient.getTableType());

    if (adbSyncConfig.getBoolean(ADB_SYNC_AUTO_CREATE_DATABASE)) {
      try {
        synchronized (AdbSyncTool.class) {
          if (!hoodieAdbClient.databaseExists(databaseName)) {
            hoodieAdbClient.createDatabase(databaseName);
          }
        }
      } catch (Exception e) {
        throw new HoodieAdbSyncException("Failed to create database:" + databaseName
            + ", useRealtimeInputFormat = " + useRealtimeInputFormat, e);
      }
    } else if (!hoodieAdbClient.databaseExists(databaseName)) {
      throw new HoodieAdbSyncException("ADB database does not exists:" + databaseName);
    }

    // Currently HoodieBootstrapRelation does support reading bootstrap MOR rt table,
    // so we disable the syncAsSparkDataSourceTable here to avoid read such kind table
    // by the data source way (which will use the HoodieBootstrapRelation).
    // TODO after we support bootstrap MOR rt table in HoodieBootstrapRelation[HUDI-2071],
    //  we can remove this logical.
    if (hoodieAdbClient.isBootstrap()
        && hoodieAdbClient.getTableType() == HoodieTableType.MERGE_ON_READ
        && !readAsOptimized) {
      adbSyncConfig.setValue(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE, "false");
      LOG.info("Disable sync as spark datasource table for mor rt table:{}", tableName);
    }

    if (adbSyncConfig.getBoolean(ADB_SYNC_DROP_TABLE_BEFORE_CREATION)) {
      LOG.info("Drop table before creation, tableName:{}", tableName);
      hoodieAdbClient.dropTable(tableName);
    }

    boolean tableExists = hoodieAdbClient.tableExists(tableName);

    // Get the parquet schema for this table looking at the latest commit
    MessageType schema = hoodieAdbClient.getStorageSchema();

    // Sync schema if needed
    syncSchema(tableName, tableExists, useRealtimeInputFormat, readAsOptimized, schema);
    LOG.info("Sync schema complete, start syncing partitions for table:{}", tableName);

    // Get the last time we successfully synced partitions
    Option<String> lastCommitTimeSynced = Option.empty();
    if (tableExists) {
      lastCommitTimeSynced = hoodieAdbClient.getLastCommitTimeSynced(tableName);
    }
    LOG.info("Last commit time synced was found:{}", lastCommitTimeSynced.orElse("null"));

    // Scan synced partitions
    List<String> writtenPartitionsSince;
    if (adbSyncConfig.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
      writtenPartitionsSince = new ArrayList<>();
    } else {
      writtenPartitionsSince = hoodieAdbClient.getPartitionsWrittenToSince(lastCommitTimeSynced);
    }
    LOG.info("Scan partitions complete, partitionNum:{}", writtenPartitionsSince.size());

    // Sync the partitions if needed
    syncPartitions(tableName, writtenPartitionsSince);

    // Update sync commit time
    // whether to skip syncing commit time stored in tbl properties, since it is time consuming.
    if (!adbSyncConfig.getBoolean(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC)) {
      hoodieAdbClient.updateLastCommitTimeSynced(tableName);
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
      boolean readAsOptimized, MessageType schema) {
    // Append spark table properties & serde properties
    Map<String, String> tableProperties = ConfigUtils.toMap(adbSyncConfig.getString(ADB_SYNC_TABLE_PROPERTIES));
    Map<String, String> serdeProperties = ConfigUtils.toMap(adbSyncConfig.getString(ADB_SYNC_SERDE_PROPERTIES));
    if (adbSyncConfig.getBoolean(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE)) {
      Map<String, String> sparkTableProperties = SparkDataSourceTableUtils.getSparkTableProperties(adbSyncConfig.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          adbSyncConfig.getString(META_SYNC_SPARK_VERSION), adbSyncConfig.getInt(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD), schema);
      Map<String, String> sparkSerdeProperties = SparkDataSourceTableUtils.getSparkSerdeProperties(readAsOptimized, adbSyncConfig.getString(META_SYNC_BASE_PATH));
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
      hoodieAdbClient.createTable(tableName, schema, inputFormatClassName, MapredParquetOutputFormat.class.getName(),
          ParquetHiveSerDe.class.getName(), serdeProperties, tableProperties);
    } else {
      // Check if the table schema has evolved
      Map<String, String> tableSchema = hoodieAdbClient.getMetastoreSchema(tableName);
      SchemaDifference schemaDiff = HiveSchemaUtil.getSchemaDifference(schema, tableSchema, adbSyncConfig.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          adbSyncConfig.getBoolean(ADB_SYNC_SUPPORT_TIMESTAMP));
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for table:{}", tableName);
        hoodieAdbClient.updateTableDefinition(tableName, schemaDiff);
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
      if (adbSyncConfig.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
        LOG.info("Not a partitioned table.");
        return;
      }

      Map<List<String>, String> partitions = hoodieAdbClient.scanTablePartitions(tableName);
      List<PartitionEvent> partitionEvents = hoodieAdbClient.getPartitionEvents(partitions, writtenPartitionsSince);
      List<String> newPartitions = filterPartitions(partitionEvents, PartitionEventType.ADD);
      LOG.info("New Partitions:{}", newPartitions);
      hoodieAdbClient.addPartitionsToTable(tableName, newPartitions);
      List<String> updatePartitions = filterPartitions(partitionEvents, PartitionEventType.UPDATE);
      LOG.info("Changed Partitions:{}", updatePartitions);
      hoodieAdbClient.updatePartitionsToTable(tableName, updatePartitions);
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
    if (params.help) {
      cmd.usage();
      System.exit(0);
    }
    AdbSyncConfig config = new AdbSyncConfig(params.toProps());
    new AdbSyncTool(config).syncHoodieTable();
  }
}
