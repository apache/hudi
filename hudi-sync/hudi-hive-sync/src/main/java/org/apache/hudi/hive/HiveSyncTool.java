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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionEvent.PartitionEventType;
import org.apache.hudi.sync.common.util.ConfigUtils;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_COMMENT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.METASTORE_URIS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_SPARK_VERSION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * Tool to sync a hoodie HDFS table with a hive metastore table. Either use it as a api
 * HiveSyncTool.syncHoodieTable(HiveSyncConfig) or as a command line java -cp hoodie-hive-sync.jar HiveSyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync hive table schema Also this will sync the
 * partitions incrementally (all the partitions modified since the last commit)
 */
@SuppressWarnings("WeakerAccess")
public class HiveSyncTool extends HoodieSyncTool implements AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(HiveSyncTool.class);
  public static final String SUFFIX_SNAPSHOT_TABLE = "_rt";
  public static final String SUFFIX_READ_OPTIMIZED_TABLE = "_ro";

  protected final HiveSyncConfig config;
  protected final String databaseName;
  protected final String tableName;
  protected HoodieSyncClient syncClient;
  protected String snapshotTableName;
  protected Option<String> roTableName;

  public HiveSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    HiveSyncConfig config = new HiveSyncConfig(props, hadoopConf);
    this.config = config;
    this.databaseName = config.getStringOrDefault(META_SYNC_DATABASE_NAME);
    this.tableName = config.getStringOrDefault(META_SYNC_TABLE_NAME);
    initSyncClient(config);
    initTableNameVars(config);
  }

  protected void initSyncClient(HiveSyncConfig config) {
    try {
      this.syncClient = new HoodieHiveSyncClient(config);
    } catch (RuntimeException e) {
      if (config.getBoolean(HIVE_IGNORE_EXCEPTIONS)) {
        LOG.error("Got runtime exception when hive syncing, but continuing as ignoreExceptions config is set ", e);
      } else {
        throw new HoodieHiveSyncException("Got runtime exception when hive syncing", e);
      }
    }
  }

  private void initTableNameVars(HiveSyncConfig config) {
    final String tableName = config.getStringOrDefault(META_SYNC_TABLE_NAME);
    if (syncClient != null) {
      switch (syncClient.getTableType()) {
        case COPY_ON_WRITE:
          this.snapshotTableName = tableName;
          this.roTableName = Option.empty();
          break;
        case MERGE_ON_READ:
          this.snapshotTableName = tableName + SUFFIX_SNAPSHOT_TABLE;
          this.roTableName = config.getBoolean(HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE)
              ? Option.of(tableName)
              : Option.of(tableName + SUFFIX_READ_OPTIMIZED_TABLE);
          break;
        default:
          LOG.error("Unknown table type " + syncClient.getTableType());
          throw new InvalidTableException(syncClient.getBasePath());
      }
    }
  }

  @Override
  public void syncHoodieTable() {
    try {
      if (syncClient != null) {
        LOG.info("Syncing target hoodie table with hive table("
            + tableId(databaseName, tableName) + "). Hive metastore URL :"
            + config.getString(METASTORE_URIS) + ", basePath :"
            + config.getString(META_SYNC_BASE_PATH));

        doSync();
      }
    } catch (RuntimeException re) {
      throw new HoodieException("Got runtime exception when hive syncing " + tableName, re);
    } finally {
      close();
    }
  }

  protected void doSync() {
    switch (syncClient.getTableType()) {
      case COPY_ON_WRITE:
        syncHoodieTable(snapshotTableName, false, false);
        break;
      case MERGE_ON_READ:
        // sync a RO table for MOR
        syncHoodieTable(roTableName.get(), false, true);
        // sync a RT table for MOR
        syncHoodieTable(snapshotTableName, true, false);
        break;
      default:
        LOG.error("Unknown table type " + syncClient.getTableType());
        throw new InvalidTableException(syncClient.getBasePath());
    }
  }

  @Override
  public void close() {
    if (syncClient != null) {
      try {
        syncClient.close();
      } catch (Exception e) {
        throw new HoodieHiveSyncException("Fail to close sync client.", e);
      }
    }
  }

  protected void syncHoodieTable(String tableName, boolean useRealtimeInputFormat, boolean readAsOptimized) {
    LOG.info("Trying to sync hoodie table " + tableName + " with base path " + syncClient.getBasePath()
        + " of type " + syncClient.getTableType());

    // check if the database exists else create it
    if (config.getBoolean(HIVE_AUTO_CREATE_DATABASE)) {
      try {
        if (!syncClient.databaseExists(databaseName)) {
          syncClient.createDatabase(databaseName);
        }
      } catch (Exception e) {
        // this is harmless since table creation will fail anyways, creation of DB is needed for in-memory testing
        LOG.warn("Unable to create database", e);
      }
    } else {
      if (!syncClient.databaseExists(databaseName)) {
        LOG.error("Hive database does not exist " + databaseName);
        throw new HoodieHiveSyncException("hive database does not exist " + databaseName);
      }
    }

    // Check if the necessary table exists
    boolean tableExists = syncClient.tableExists(tableName);

    // Get the parquet schema for this table looking at the latest commit
    MessageType schema = syncClient.getStorageSchema();

    // Currently HoodieBootstrapRelation does support reading bootstrap MOR rt table,
    // so we disable the syncAsSparkDataSourceTable here to avoid read such kind table
    // by the data source way (which will use the HoodieBootstrapRelation).
    // TODO after we support bootstrap MOR rt table in HoodieBootstrapRelation[HUDI-2071], we can remove this logical.
    if (syncClient.isBootstrap()
        && syncClient.getTableType() == HoodieTableType.MERGE_ON_READ
        && !readAsOptimized) {
      config.setValue(HIVE_SYNC_AS_DATA_SOURCE_TABLE, "false");
    }

    // Sync schema if needed
    boolean schemaChanged = syncSchema(tableName, tableExists, useRealtimeInputFormat, readAsOptimized, schema);

    LOG.info("Schema sync complete. Syncing partitions for " + tableName);
    // Get the last time we successfully synced partitions
    Option<String> lastCommitTimeSynced = Option.empty();
    if (tableExists) {
      lastCommitTimeSynced = syncClient.getLastCommitTimeSynced(tableName);
    }
    LOG.info("Last commit time synced was found to be " + lastCommitTimeSynced.orElse("null"));
    List<String> writtenPartitionsSince = syncClient.getWrittenPartitionsSince(lastCommitTimeSynced);
    LOG.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());

    // Sync the partitions if needed
    // find dropped partitions, if any, in the latest commit
    Set<String> droppedPartitions = syncClient.getDroppedPartitionsSince(lastCommitTimeSynced);
    boolean partitionsChanged = syncPartitions(tableName, writtenPartitionsSince, droppedPartitions);
    boolean meetSyncConditions = schemaChanged || partitionsChanged;
    if (!config.getBoolean(META_SYNC_CONDITIONAL_SYNC) || meetSyncConditions) {
      syncClient.updateLastCommitTimeSynced(tableName);
    }
    LOG.info("Sync complete for " + tableName);
  }

  /**
   * Get the latest schema from the last commit and check if its in sync with the hive table schema. If not, evolves the
   * table schema.
   *
   * @param tableExists does table exist
   * @param schema      extracted schema
   */
  private boolean syncSchema(String tableName, boolean tableExists, boolean useRealTimeInputFormat,
      boolean readAsOptimized, MessageType schema) {
    // Append spark table properties & serde properties
    Map<String, String> tableProperties = ConfigUtils.toMap(config.getString(HIVE_TABLE_PROPERTIES));
    Map<String, String> serdeProperties = ConfigUtils.toMap(config.getString(HIVE_TABLE_SERDE_PROPERTIES));
    if (config.getBoolean(HIVE_SYNC_AS_DATA_SOURCE_TABLE)) {
      Map<String, String> sparkTableProperties = SparkDataSourceTableUtils.getSparkTableProperties(config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          config.getStringOrDefault(META_SYNC_SPARK_VERSION), config.getIntOrDefault(HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD), schema);
      Map<String, String> sparkSerdeProperties = SparkDataSourceTableUtils.getSparkSerdeProperties(readAsOptimized, config.getString(META_SYNC_BASE_PATH));
      tableProperties.putAll(sparkTableProperties);
      serdeProperties.putAll(sparkSerdeProperties);
    }
    boolean schemaChanged = false;
    // Check and sync schema
    if (!tableExists) {
      LOG.info("Hive table " + tableName + " is not found. Creating it");
      HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT).toUpperCase());
      String inputFormatClassName = HoodieInputFormatUtils.getInputFormatClassName(baseFileFormat, useRealTimeInputFormat);

      if (baseFileFormat.equals(HoodieFileFormat.PARQUET) && config.getBooleanOrDefault(HIVE_USE_PRE_APACHE_INPUT_FORMAT)) {
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
      syncClient.createTable(tableName, schema, inputFormatClassName,
          outputFormatClassName, serDeFormatClassName, serdeProperties, tableProperties);
      schemaChanged = true;
    } else {
      // Check if the table schema has evolved
      Map<String, String> tableSchema = syncClient.getMetastoreSchema(tableName);
      SchemaDifference schemaDiff = HiveSchemaUtil.getSchemaDifference(schema, tableSchema, config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          config.getBooleanOrDefault(HIVE_SUPPORT_TIMESTAMP_TYPE));
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for " + tableName);
        syncClient.updateTableSchema(tableName, schema);
        // Sync the table properties if the schema has changed
        if (config.getString(HIVE_TABLE_PROPERTIES) != null || config.getBoolean(HIVE_SYNC_AS_DATA_SOURCE_TABLE)) {
          syncClient.updateTableProperties(tableName, tableProperties);
          syncClient.updateSerdeProperties(tableName, serdeProperties);
          LOG.info("Sync table properties for " + tableName + ", table properties is: " + tableProperties);
        }
        schemaChanged = true;
      } else {
        LOG.info("No Schema difference for " + tableName);
      }
    }

    if (config.getBoolean(HIVE_SYNC_COMMENT)) {
      List<FieldSchema> fromMetastore = syncClient.getMetastoreFieldSchemas(tableName);
      List<FieldSchema> fromStorage = syncClient.getStorageFieldSchemas();
      syncClient.updateTableComments(tableName, fromMetastore, fromStorage);
    }
    return schemaChanged;
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in hive, if not adds it or if the
   * partition path does not match, it updates the partition path).
   */
  private boolean syncPartitions(String tableName, List<String> writtenPartitionsSince, Set<String> droppedPartitions) {
    boolean partitionsChanged;
    try {
      List<Partition> hivePartitions = syncClient.getAllPartitions(tableName);
      List<PartitionEvent> partitionEvents =
          syncClient.getPartitionEvents(hivePartitions, writtenPartitionsSince, droppedPartitions);

      List<String> newPartitions = filterPartitions(partitionEvents, PartitionEventType.ADD);
      if (!newPartitions.isEmpty()) {
        LOG.info("New Partitions " + newPartitions);
        syncClient.addPartitionsToTable(tableName, newPartitions);
      }

      List<String> updatePartitions = filterPartitions(partitionEvents, PartitionEventType.UPDATE);
      if (!updatePartitions.isEmpty()) {
        LOG.info("Changed Partitions " + updatePartitions);
        syncClient.updatePartitionsToTable(tableName, updatePartitions);
      }

      List<String> dropPartitions = filterPartitions(partitionEvents, PartitionEventType.DROP);
      if (!dropPartitions.isEmpty()) {
        LOG.info("Drop Partitions " + dropPartitions);
        syncClient.dropPartitions(tableName, dropPartitions);
      }

      partitionsChanged = !updatePartitions.isEmpty() || !newPartitions.isEmpty() || !dropPartitions.isEmpty();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to sync partitions for table " + tableName, e);
    }
    return partitionsChanged;
  }

  private List<String> filterPartitions(List<PartitionEvent> events, PartitionEventType eventType) {
    return events.stream().filter(s -> s.eventType == eventType).map(s -> s.storagePartition)
        .collect(Collectors.toList());
  }

  public static void main(String[] args) {
    final HiveSyncConfig.HiveSyncConfigParams params = new HiveSyncConfig.HiveSyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
      System.exit(0);
    }
    new HiveSyncTool(params.toProps(), new Configuration()).syncHoodieTable();
  }
}
