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
import org.apache.hudi.common.model.HoodieSyncTableStrategy;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionEvent.PartitionEventType;
import org.apache.hudi.sync.common.util.SparkDataSourceTableUtils;

import com.beust.jcommander.JCommander;
import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_FILTER_PUSHDOWN_ENABLED;
import static org.apache.hudi.hive.HiveSyncConfig.RECREATE_HIVE_TABLE_ON_ERROR;
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
import static org.apache.hudi.hive.HiveSyncConfigHolder.METASTORE_URIS;
import static org.apache.hudi.hive.util.HiveSchemaUtil.getSchemaDifference;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_INCREMENTAL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_SNAPSHOT_WITH_TABLE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_SPARK_VERSION;
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

  protected HiveSyncConfig config;
  private String databaseName;
  private String tableName;

  protected HoodieSyncClient syncClient;
  protected String snapshotTableName;
  protected Option<String> roTableName;

  private String hiveSyncTableStrategy;

  public HiveSyncTool(Properties props, Configuration hadoopConf) {
    this(props, hadoopConf, Option.empty());
  }

  public HiveSyncTool(Properties props, Configuration hadoopConf, Option<HoodieTableMetaClient> metaClientOption) {
    super(props, hadoopConf);
    String configuredMetastoreUris = props.getProperty(METASTORE_URIS.key());

    final Configuration hadoopConfForSync; // the configuration to use for this instance of the sync tool
    if (nonEmpty(configuredMetastoreUris)) {
      // if metastore uri is configured, we can create a new configuration with the value set
      hadoopConfForSync = new Configuration(hadoopConf);
      hadoopConfForSync.set(HiveConf.ConfVars.METASTOREURIS.varname, configuredMetastoreUris);
    } else {
      // if the user did not provide any URIs, then we can use the provided configuration
      hadoopConfForSync = hadoopConf;
    }

    this.config = new HiveSyncConfig(props, hadoopConfForSync);
    HoodieTableMetaClient metaClient = metaClientOption.orElseGet(() -> buildMetaClient(config));
    initSyncClient(config, metaClient);
    // initSyncClient leaves syncClient as null
    if (!Objects.isNull(this.syncClient)) {
      this.tableName = this.syncClient.getTableName();
      this.databaseName = this.syncClient.getDatabaseName();
    }
    initTableNameVars(config);
  }

  protected void initSyncClient(HiveSyncConfig config, HoodieTableMetaClient metaClient) {
    try {
      this.syncClient = new HoodieHiveSyncClient(config, metaClient);
    } catch (RuntimeException e) {
      if (config.getBoolean(HIVE_IGNORE_EXCEPTIONS)) {
        LOG.error("Got runtime exception when hive syncing, but continuing as ignoreExceptions config is set ", e);
      } else {
        throw new HoodieHiveSyncException("Got runtime exception when hive syncing", e);
      }
    }
  }

  private void initTableNameVars(HiveSyncConfig config) {
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

  public String getTableName() {
    return this.tableName;
  }

  public String getDatabaseName() {
    return this.databaseName;
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
    // create database if needed
    checkAndCreateDatabase();

    switch (syncClient.getTableType()) {
      case COPY_ON_WRITE:
        syncHoodieTable(snapshotTableName, false, false);
        break;
      case MERGE_ON_READ:
        switch (HoodieSyncTableStrategy.valueOf(hiveSyncTableStrategy)) {
          case RO:
            // sync a RO table for MOR
            syncHoodieTable(tableName, false, true);
            break;
          case RT:
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

    final boolean tableExists = syncClient.tableExists(tableName);
    // if table exists and location of the metastore table doesn't match the hoodie base path, recreate the table
    if (tableExists && !FSUtils.comparePathsWithoutScheme(syncClient.getBasePath(), syncClient.getTableLocation(tableName))) {
      LOG.info("basepath is updated for the table {}", tableName);
      recreateAndSyncHiveTable(tableName, useRealtimeInputFormat, readAsOptimized);
      return;
    }

    // Check if any sync is required
    if (tableExists && isIncrementalSync() && isAlreadySynced(tableName)) {
      LOG.info("Table {} is already synced with the latest commit.", tableName);
      return;
    }
    // Get the parquet schema for this table looking at the latest commit
    HoodieSchema schema = syncClient.getStorageSchema(!config.getBoolean(HIVE_SYNC_OMIT_METADATA_FIELDS));

    boolean schemaChanged;
    boolean propertiesChanged;
    try {
      if (tableExists) {
        schemaChanged = syncSchema(tableName, schema);
        propertiesChanged = syncProperties(tableName, useRealtimeInputFormat, readAsOptimized, schema);
      } else {
        syncFirstTime(tableName, useRealtimeInputFormat, readAsOptimized, schema);
        schemaChanged = true;
        propertiesChanged = true;
      }

      boolean partitionsChanged = validateAndSyncPartitions(tableName, tableExists);
      boolean meetSyncConditions = schemaChanged || propertiesChanged || partitionsChanged;
      if (!config.getBoolean(META_SYNC_CONDITIONAL_SYNC) || meetSyncConditions) {
        syncClient.updateLastCommitTimeSynced(tableName);
      }
      LOG.info("Sync complete for {}", tableName);
    } catch (HoodieHiveSyncException ex) {
      if (shouldRecreateAndSyncTable()) {
        LOG.warn("Failed to sync the table {}. Attempting trying to recreate", tableName, ex);
        recreateAndSyncHiveTable(tableName, useRealtimeInputFormat, readAsOptimized);
      } else {
        throw new HoodieHiveSyncException("failed to sync the table " + tableName, ex);
      }
    }
  }

  private boolean isAlreadySynced(String tableName) {
    return syncClient.getLastCommitTimeSynced(tableName)
        .map(lastCommit -> {
          Option<String> lastCompletion =
              syncClient.getLastCommitCompletionTimeSynced(tableName);
          String currentLastCommit = syncClient.getActiveTimeline().lastInstant().map(HoodieInstant::requestedTime).orElse(null);
          return Objects.equals(lastCommit, currentLastCommit) && lastCompletion.map(clientLastCompletion ->
              Objects.equals(clientLastCompletion, syncClient.getActiveTimeline().getLatestCompletionTime().orElse(null))).orElse(true);
        })
        .orElse(false);
  }

  private void checkAndCreateDatabase() {
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
        LOG.error("Hive database does not exist {}", databaseName);
        throw new HoodieHiveSyncException("hive database does not exist " + databaseName);
      }
    }
  }

  private boolean validateAndSyncPartitions(String tableName, boolean tableExists) {
    boolean syncIncremental = isIncrementalSync();
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
      LOG.info("Partitions dropped since last sync: {}", droppedPartitions.size());
      partitionsChanged = syncPartitions(tableName, writtenPartitionsSince, droppedPartitions);
    }
    return partitionsChanged;
  }

  private boolean isIncrementalSync() {
    return config.getBoolean(META_SYNC_INCREMENTAL);
  }

  protected boolean shouldRecreateAndSyncTable() {
    return config.getBooleanOrDefault(RECREATE_HIVE_TABLE_ON_ERROR);
  }

  private void recreateAndSyncHiveTable(String tableName, boolean useRealtimeInputFormat, boolean readAsOptimized) {
    LOG.info("recreating and syncing the table {}", tableName);
    Timer.Context timerContext = metrics.getRecreateAndSyncTimer();
    HoodieSchema schema = syncClient.getStorageSchema(!config.getBoolean(HIVE_SYNC_OMIT_METADATA_FIELDS));
    try {
      createOrReplaceTable(tableName, useRealtimeInputFormat, readAsOptimized, schema);
      syncAllPartitions(tableName);
      syncClient.updateLastCommitTimeSynced(tableName);
      if (Objects.nonNull(timerContext)) {
        long durationInNs = timerContext.stop();
        metrics.updateRecreateAndSyncDurationInMs(durationInNs);
      }
    } catch (HoodieHiveSyncException ex) {
      metrics.incrementRecreateAndSyncFailureCounter();
      throw new HoodieHiveSyncException("failed to recreate the table for " + tableName, ex);
    }
  }

  private void createOrReplaceTable(String tableName, boolean useRealtimeInputFormat, boolean readAsOptimized, HoodieSchema schema) {
    HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT).toUpperCase());
    String inputFormatClassName = getInputFormatClassName(baseFileFormat, useRealtimeInputFormat);
    String outputFormatClassName = getOutputFormatClassName(baseFileFormat);
    String serDeFormatClassName = getSerDeClassName(baseFileFormat);
    Map<String, String> serdeProperties = getSerdeProperties(readAsOptimized);
    Map<String, String> tableProperties = getTableProperties(schema);
    syncClient.createOrReplaceTable(tableName, schema, inputFormatClassName,
        outputFormatClassName, serDeFormatClassName, serdeProperties, tableProperties);
  }

  private Map<String, String> getTableProperties(HoodieSchema schema) {
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

  private void syncFirstTime(String tableName, boolean useRealTimeInputFormat, boolean readAsOptimized, HoodieSchema schema) {
    LOG.info("Sync table {} for the first time.", tableName);
    HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT).toUpperCase());
    String inputFormatClassName = getInputFormatClassName(baseFileFormat, useRealTimeInputFormat);
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

  private boolean syncSchema(String tableName, HoodieSchema schema) {
    boolean schemaChanged = false;

    // Check if the table schema has evolved
    Map<String, String> tableSchema = syncClient.getMetastoreSchema(tableName);
    SchemaDifference schemaDiff = getSchemaDifference(schema, tableSchema,
        config.getSplitStrings(META_SYNC_PARTITION_FIELDS),
        config.getBooleanOrDefault(HIVE_SUPPORT_TIMESTAMP_TYPE));
    if (schemaDiff.isEmpty()) {
      LOG.info("No Schema difference for {}.", tableName);
    } else {
      LOG.info("Schema difference found for {}. Updated schema: {}", tableName, schema);
      syncClient.updateTableSchema(tableName, schema, schemaDiff);
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

  private boolean syncProperties(String tableName, boolean useRealTimeInputFormat, boolean readAsOptimized, HoodieSchema schema) {
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

    return syncClient.getPartitionsFromList(tableName, writtenPartitions);
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
      if (config.shouldNotSyncPartitionMetadata() || config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
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
      if (config.shouldNotSyncPartitionMetadata() || writtenPartitionsSince.isEmpty() || config.getSplitStrings(META_SYNC_PARTITION_FIELDS).isEmpty()) {
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
