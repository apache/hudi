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
import org.apache.hudi.common.model.HoodieSyncTableStrategy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.hive.util.PartitionFilterGenerator;
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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_FILTER_PUSHDOWN_ENABLED;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_COMMENT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_OMIT_METADATA_FIELDS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_TABLE_STRATEGY;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.METASTORE_URIS;
import static org.apache.hudi.hive.util.HiveSchemaUtil.getSchemaDifference;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_INCREMENTAL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_SNAPSHOT_WITH_TABLE_NAME;
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

  private static final Logger LOG = LoggerFactory.getLogger(HiveSyncTool.class);
  public static final String SUFFIX_SNAPSHOT_TABLE = "_rt";
  public static final String SUFFIX_READ_OPTIMIZED_TABLE = "_ro";

  private HiveSyncConfig config;
  private final String databaseName;
  private final String tableName;

  protected HoodieSyncClient syncClient;
  protected String snapshotTableName;
  protected Option<String> roTableName;

  private String hiveSyncTableStrategy;

  public HiveSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    String metastoreUris = props.getProperty(METASTORE_URIS.key());
    // Give precedence to HiveConf.ConfVars.METASTOREURIS if it is set.
    // Else if user has provided HiveSyncConfigHolder.METASTORE_URIS, then set that in hadoop conf.
    if (isNullOrEmpty(hadoopConf.get(HiveConf.ConfVars.METASTOREURIS.varname)) && nonEmpty(metastoreUris)) {
      LOG.info(String.format("Setting %s = %s", HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris));
      hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);
    }
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
          this.hiveSyncTableStrategy = config.getStringOrDefault(HIVE_SYNC_TABLE_STRATEGY).toUpperCase();
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
            + tableId(databaseName, tableName) + "). Hive metastore URL from HiveConf:"
            + config.getHiveConf().get(HiveConf.ConfVars.METASTOREURIS.varname) + "). Hive metastore URL from HiveSyncConfig:"
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
        switch (HoodieSyncTableStrategy.valueOf(hiveSyncTableStrategy)) {
          case RO :
            // sync a RO table for MOR
            syncHoodieTable(tableName, false, true);
            break;
          case RT :
            // sync a RT table for MOR
            syncHoodieTable(tableName, true, false);
            break;
          default:
            // sync a RO table for MOR
            syncHoodieTable(roTableName.get(), false, true);
            // sync a RT table for MOR
            syncHoodieTable(snapshotTableName, true, false);
            // sync origin table for MOR
            if (config.getBoolean(META_SYNC_SNAPSHOT_WITH_TABLE_NAME)) {
              syncHoodieTable(tableName, true, false);
            }
        }
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
    if (config != null) {
      config = null;
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

    final boolean tableExists = syncClient.tableExists(tableName);

    // Get the parquet schema for this table looking at the latest commit
    MessageType schema = syncClient.getStorageSchema(!config.getBoolean(HIVE_SYNC_OMIT_METADATA_FIELDS));
    boolean schemaChanged;
    boolean propertiesChanged;
    if (tableExists) {
      schemaChanged = syncSchema(tableName, schema);
      propertiesChanged = syncProperties(tableName, useRealtimeInputFormat, readAsOptimized, schema);
    } else {
      syncFirstTime(tableName, useRealtimeInputFormat, readAsOptimized, schema);
      schemaChanged = true;
      propertiesChanged = true;
    }

    boolean syncIncremental = config.getBoolean(META_SYNC_INCREMENTAL);
    Option<String> lastCommitTimeSynced = (tableExists && syncIncremental)
        ? syncClient.getLastCommitTimeSynced(tableName) : Option.empty();
    Option<String> lastCommitCompletionTimeSynced = (tableExists && syncIncremental)
        ? syncClient.getLastCommitCompletionTimeSynced(tableName) : Option.empty();
    if (syncIncremental) {
      LOG.info(String.format("Last commit time synced was found to be %s, last commit completion time is found to be %s",
          lastCommitTimeSynced.orElse("null"), lastCommitCompletionTimeSynced.orElse("null")));
    } else {
      LOG.info(
          "Executing a full partition sync operation since {} is set to false.",
          META_SYNC_INCREMENTAL.key());
    }

    boolean partitionsChanged;
    if (!lastCommitTimeSynced.isPresent()
        || syncClient.getActiveTimeline().isBeforeTimelineStarts(lastCommitTimeSynced.get())) {
      // If the last commit time synced is before the start of the active timeline,
      // the Hive sync falls back to list all partitions on storage, instead of
      // reading active and archived timelines for written partitions.
      LOG.info("Sync all partitions given the last commit time synced is empty or "
          + "before the start of the active timeline. Listing all partitions in "
          + config.getString(META_SYNC_BASE_PATH)
          + ", file system: " + config.getHadoopFileSystem());
      partitionsChanged = syncAllPartitions(tableName);
    } else {
      List<String> writtenPartitionsSince = syncClient.getWrittenPartitionsSince(lastCommitTimeSynced, lastCommitCompletionTimeSynced);
      LOG.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());

      // Sync the partitions if needed
      // find dropped partitions, if any, in the latest commit
      Set<String> droppedPartitions = syncClient.getDroppedPartitionsSince(lastCommitTimeSynced, lastCommitCompletionTimeSynced);
      partitionsChanged = syncPartitions(tableName, writtenPartitionsSince, droppedPartitions);
    }

    boolean meetSyncConditions = schemaChanged || propertiesChanged || partitionsChanged;
    if (!config.getBoolean(META_SYNC_CONDITIONAL_SYNC) || meetSyncConditions) {
      syncClient.updateLastCommitTimeSynced(tableName);
    }
    LOG.info("Sync complete for " + tableName);
  }

  private Map<String, String> getTableProperties(MessageType schema) {
    Map<String, String> tableProperties = ConfigUtils.toMap(config.getString(HIVE_TABLE_PROPERTIES));
    if (config.getBoolean(HIVE_SYNC_AS_DATA_SOURCE_TABLE)) {
      Map<String, String> sparkTableProperties = SparkDataSourceTableUtils.getSparkTableProperties(config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
          config.getStringOrDefault(META_SYNC_SPARK_VERSION), config.getIntOrDefault(HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD), schema);
      tableProperties.putAll(sparkTableProperties);
    }
    return tableProperties;
  }

  private Map<String, String> getSerdeProperties(boolean readAsOptimized) {
    Map<String, String> serdeProperties = ConfigUtils.toMap(config.getString(HIVE_TABLE_SERDE_PROPERTIES));
    if (config.getBoolean(HIVE_SYNC_AS_DATA_SOURCE_TABLE)) {
      Map<String, String> sparkSerdeProperties = SparkDataSourceTableUtils.getSparkSerdeProperties(readAsOptimized, config.getString(META_SYNC_BASE_PATH));
      serdeProperties.putAll(sparkSerdeProperties);
    }
    return serdeProperties;
  }

  private void syncFirstTime(String tableName, boolean useRealTimeInputFormat, boolean readAsOptimized, MessageType schema) {
    LOG.info("Sync table {} for the first time.", tableName);
    HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT).toUpperCase());
    String inputFormatClassName = getInputFormatClassName(baseFileFormat, useRealTimeInputFormat, config.getBooleanOrDefault(HIVE_USE_PRE_APACHE_INPUT_FORMAT));
    String outputFormatClassName = getOutputFormatClassName(baseFileFormat);
    String serDeFormatClassName = getSerDeClassName(baseFileFormat);
    Map<String, String> serdeProperties = getSerdeProperties(readAsOptimized);
    Map<String, String> tableProperties = getTableProperties(schema);

    // Custom serde will not work with ALTER TABLE REPLACE COLUMNS
    // https://github.com/apache/hive/blob/release-1.1.0/ql/src/java/org/apache/hadoop/hive
    // /ql/exec/DDLTask.java#L3488
    syncClient.createTable(tableName, schema, inputFormatClassName,
        outputFormatClassName, serDeFormatClassName, serdeProperties, tableProperties);
  }

  private boolean syncSchema(String tableName, MessageType schema) {
    boolean schemaChanged = false;

    // Check if the table schema has evolved
    Map<String, String> tableSchema = syncClient.getMetastoreSchema(tableName);
    SchemaDifference schemaDiff = getSchemaDifference(schema, tableSchema,
        config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
        config.getBooleanOrDefault(HIVE_SUPPORT_TIMESTAMP_TYPE));
    if (schemaDiff.isEmpty()) {
      LOG.info("No Schema difference for {}\nMessageType: {}", tableName, schema);
    } else {
      LOG.info("Schema difference found for {}. Updated schema: {}", tableName, schema);
      syncClient.updateTableSchema(tableName, schema);
      schemaChanged = true;
    }

    if (config.getBoolean(HIVE_SYNC_COMMENT)) {
      List<FieldSchema> fromMetastore = syncClient.getMetastoreFieldSchemas(tableName);
      List<FieldSchema> fromStorage = syncClient.getStorageFieldSchemas();
      boolean commentsChanged = syncClient.updateTableComments(tableName, fromMetastore, fromStorage);
      schemaChanged = schemaChanged || commentsChanged;
    }
    return schemaChanged;
  }

  private boolean syncProperties(String tableName, boolean useRealTimeInputFormat, boolean readAsOptimized, MessageType schema) {
    boolean propertiesChanged = false;

    Map<String, String> serdeProperties = getSerdeProperties(readAsOptimized);
    boolean serdePropertiesUpdated = syncClient.updateSerdeProperties(tableName, serdeProperties, useRealTimeInputFormat);
    propertiesChanged = propertiesChanged || serdePropertiesUpdated;

    Map<String, String> tableProperties = getTableProperties(schema);
    boolean tablePropertiesUpdated = syncClient.updateTableProperties(tableName, tableProperties);
    propertiesChanged = propertiesChanged || tablePropertiesUpdated;

    return propertiesChanged;
  }

  /**
   * Fetch partitions from meta service, will try to push down more filters to avoid fetching
   * too many unnecessary partitions.
   *
   * @param writtenPartitions partitions has been added, updated, or dropped since last synced.
   */
  private List<Partition> getTablePartitions(String tableName, List<String> writtenPartitions) {
    if (!config.getBooleanOrDefault(HIVE_SYNC_FILTER_PUSHDOWN_ENABLED)) {
      return syncClient.getAllPartitions(tableName);
    }

    List<String> partitionKeys = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).stream()
        .map(String::toLowerCase)
        .collect(Collectors.toList());

    List<FieldSchema> partitionFields = syncClient.getMetastoreFieldSchemas(tableName)
        .stream()
        .filter(f -> partitionKeys.contains(f.getName()))
        .collect(Collectors.toList());

    return syncClient.getPartitionsByFilter(tableName,
        PartitionFilterGenerator.generatePushDownFilter(writtenPartitions, partitionFields, config));
  }

  /**
   * Syncs all partitions on storage to the metastore, by only making incremental changes.
   *
   * @param tableName The table name in the metastore.
   * @return {@code true} if one or more partition(s) are changed in the metastore;
   * {@code false} otherwise.
   */
  private boolean syncAllPartitions(String tableName) {
    try {
      if (config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
        return false;
      }

      List<Partition> allPartitionsInMetastore = syncClient.getAllPartitions(tableName);
      List<String> allPartitionsOnStorage = syncClient.getAllPartitionPathsOnStorage();
      return syncPartitions(
          tableName,
          syncClient.getPartitionEvents(allPartitionsInMetastore, allPartitionsOnStorage));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to sync partitions for table " + tableName, e);
    }
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in hive, if not adds it or if the
   * partition path does not match, it updates the partition path).
   *
   * @param tableName              The table name in the metastore.
   * @param writtenPartitionsSince Partitions has been added, updated, or dropped since last synced.
   * @param droppedPartitions      Partitions that are dropped since last sync.
   * @return {@code true} if one or more partition(s) are changed in the metastore;
   * {@code false} otherwise.
   */
  private boolean syncPartitions(String tableName, List<String> writtenPartitionsSince, Set<String> droppedPartitions) {
    try {
      if (writtenPartitionsSince.isEmpty() || config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
        return false;
      }

      List<Partition> hivePartitions = getTablePartitions(tableName, writtenPartitionsSince);
      return syncPartitions(
          tableName,
          syncClient.getPartitionEvents(
              hivePartitions, writtenPartitionsSince, droppedPartitions));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to sync partitions for table " + tableName, e);
    }
  }

  /**
   * Syncs added, updated, and dropped partitions to the metastore.
   *
   * @param tableName          The table name in the metastore.
   * @param partitionEventList The partition change event list.
   * @return {@code true} if one or more partition(s) are changed in the metastore;
   * {@code false} otherwise.
   */
  private boolean syncPartitions(String tableName, List<PartitionEvent> partitionEventList) {
    List<String> newPartitions = filterPartitions(partitionEventList, PartitionEventType.ADD);
    if (!newPartitions.isEmpty()) {
      LOG.info("New Partitions " + newPartitions);
      syncClient.addPartitionsToTable(tableName, newPartitions);
    }

    List<String> updatePartitions = filterPartitions(partitionEventList, PartitionEventType.UPDATE);
    if (!updatePartitions.isEmpty()) {
      LOG.info("Changed Partitions " + updatePartitions);
      syncClient.updatePartitionsToTable(tableName, updatePartitions);
    }

    List<String> dropPartitions = filterPartitions(partitionEventList, PartitionEventType.DROP);
    if (!dropPartitions.isEmpty()) {
      LOG.info("Drop Partitions " + dropPartitions);
      syncClient.dropPartitions(tableName, dropPartitions);
    }

    return !updatePartitions.isEmpty() || !newPartitions.isEmpty() || !dropPartitions.isEmpty();
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
