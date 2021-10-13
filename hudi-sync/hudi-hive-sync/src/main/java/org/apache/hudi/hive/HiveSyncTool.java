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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.util.ConfigUtils;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.hive.util.Parquet2SparkSchemaUtils;

import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent.PartitionEventType;
import org.apache.hudi.sync.common.AbstractSyncTool;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

/**
 * Tool to sync a hoodie HDFS table with a hive metastore table. Either use it as a api
 * HiveSyncTool.syncHoodieTable(HiveSyncConfig) or as a command line java -cp hoodie-hive-sync.jar HiveSyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync hive table schema Also this will sync the
 * partitions incrementally (all the partitions modified since the last commit)
 */
@SuppressWarnings("WeakerAccess")
public class HiveSyncTool extends AbstractSyncTool {

  private static final Logger LOG = LogManager.getLogger(HiveSyncTool.class);
  public static final String SUFFIX_SNAPSHOT_TABLE = "_rt";
  public static final String SUFFIX_READ_OPTIMIZED_TABLE = "_ro";

  protected final HiveSyncConfig cfg;
  protected HoodieHiveClient hoodieHiveClient = null;
  protected String snapshotTableName = null;
  protected Option<String> roTableName = null;

  public HiveSyncTool(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(configuration.getAllProperties(), fs);

    try {
      this.hoodieHiveClient = new HoodieHiveClient(cfg, configuration, fs);
    } catch (RuntimeException e) {
      if (cfg.ignoreExceptions) {
        LOG.error("Got runtime exception when hive syncing, but continuing as ignoreExceptions config is set ", e);
      } else {
        throw new HoodieHiveSyncException("Got runtime exception when hive syncing", e);
      }
    }

    this.cfg = cfg;
    // Set partitionFields to empty, when the NonPartitionedExtractor is used
    if (NonPartitionedExtractor.class.getName().equals(cfg.partitionValueExtractorClass)) {
      LOG.warn("Set partitionFields to empty, since the NonPartitionedExtractor is used");
      cfg.partitionFields = new ArrayList<>();
    }
    if (hoodieHiveClient != null) {
      switch (hoodieHiveClient.getTableType()) {
        case COPY_ON_WRITE:
          this.snapshotTableName = cfg.tableName;
          this.roTableName = Option.empty();
          break;
        case MERGE_ON_READ:
          this.snapshotTableName = cfg.tableName + SUFFIX_SNAPSHOT_TABLE;
          this.roTableName = cfg.skipROSuffix ? Option.of(cfg.tableName) :
              Option.of(cfg.tableName + SUFFIX_READ_OPTIMIZED_TABLE);
          break;
        default:
          LOG.error("Unknown table type " + hoodieHiveClient.getTableType());
          throw new InvalidTableException(hoodieHiveClient.getBasePath());
      }
    }
  }

  @Override
  public void syncHoodieTable() {
    try {
      if (hoodieHiveClient != null) {
        doSync();
      }
    } catch (RuntimeException re) {
      throw new HoodieException("Got runtime exception when hive syncing " + cfg.tableName, re);
    } finally {
      if (hoodieHiveClient != null) {
        hoodieHiveClient.close();
      }
    }
  }

  protected void doSync() {
    switch (hoodieHiveClient.getTableType()) {
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
        LOG.error("Unknown table type " + hoodieHiveClient.getTableType());
        throw new InvalidTableException(hoodieHiveClient.getBasePath());
    }
  }

  protected void syncHoodieTable(String tableName, boolean useRealtimeInputFormat,
                               boolean readAsOptimized) {
    LOG.info("Trying to sync hoodie table " + tableName + " with base path " + hoodieHiveClient.getBasePath()
        + " of type " + hoodieHiveClient.getTableType());

    // check if the database exists else create it
    if (cfg.autoCreateDatabase) {
      try {
        if (!hoodieHiveClient.doesDataBaseExist(cfg.databaseName)) {
          hoodieHiveClient.createDatabase(cfg.databaseName);
        }
      } catch (Exception e) {
        // this is harmless since table creation will fail anyways, creation of DB is needed for in-memory testing
        LOG.warn("Unable to create database", e);
      }
    } else {
      if (!hoodieHiveClient.doesDataBaseExist(cfg.databaseName)) {
        throw new HoodieHiveSyncException("hive database does not exist " + cfg.databaseName);
      }
    }

    // Check if the necessary table exists
    boolean tableExists = hoodieHiveClient.doesTableExist(tableName);

    // Get the parquet schema for this table looking at the latest commit
    MessageType schema = hoodieHiveClient.getDataSchema();

    // Currently HoodieBootstrapRelation does support reading bootstrap MOR rt table,
    // so we disable the syncAsSparkDataSourceTable here to avoid read such kind table
    // by the data source way (which will use the HoodieBootstrapRelation).
    // TODO after we support bootstrap MOR rt table in HoodieBootstrapRelation[HUDI-2071], we can remove this logical.
    if (hoodieHiveClient.isBootstrap()
            && hoodieHiveClient.getTableType() == HoodieTableType.MERGE_ON_READ
            && !readAsOptimized) {
      cfg.syncAsSparkDataSourceTable = false;
    }
    // Sync schema if needed
    syncSchema(tableName, tableExists, useRealtimeInputFormat, readAsOptimized, schema);

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
  private void syncSchema(String tableName, boolean tableExists, boolean useRealTimeInputFormat,
                          boolean readAsOptimized, MessageType schema) {
    // Append spark table properties & serde properties
    Map<String, String> tableProperties = ConfigUtils.toMap(cfg.tableProperties);
    Map<String, String> serdeProperties = ConfigUtils.toMap(cfg.serdeProperties);
    if (cfg.syncAsSparkDataSourceTable) {
      Map<String, String> sparkTableProperties = getSparkTableProperties(cfg.sparkSchemaLengthThreshold, schema);
      Map<String, String> sparkSerdeProperties = getSparkSerdeProperties(readAsOptimized);
      tableProperties.putAll(sparkTableProperties);
      serdeProperties.putAll(sparkSerdeProperties);
    }
    // Check and sync schema
    if (!tableExists) {
      LOG.info("Hive table " + tableName + " is not found. Creating it");
      HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(cfg.baseFileFormat.toUpperCase());
      String inputFormatClassName = HoodieInputFormatUtils.getInputFormatClassName(baseFileFormat, useRealTimeInputFormat);

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
      hoodieHiveClient.createTable(tableName, schema, inputFormatClassName,
          outputFormatClassName, serDeFormatClassName, serdeProperties, tableProperties);
    } else {
      // Check if the table schema has evolved
      Map<String, String> tableSchema = hoodieHiveClient.getTableSchema(tableName);
      SchemaDifference schemaDiff = HiveSchemaUtil.getSchemaDifference(schema, tableSchema, cfg.partitionFields, cfg.supportTimestamp);
      if (!schemaDiff.isEmpty()) {
        LOG.info("Schema difference found for " + tableName);
        hoodieHiveClient.updateTableDefinition(tableName, schema);
        // Sync the table properties if the schema has changed
        if (cfg.tableProperties != null) {
          hoodieHiveClient.updateTableProperties(tableName, tableProperties);
          LOG.info("Sync table properties for " + tableName + ", table properties is: " + cfg.tableProperties);
        }
      } else {
        LOG.info("No Schema difference for " + tableName);
      }
    }
  }

  /**
   * Get Spark Sql related table properties. This is used for spark datasource table.
   * @param schema  The schema to write to the table.
   * @return A new parameters added the spark's table properties.
   */
  private Map<String, String> getSparkTableProperties(int schemaLengthThreshold, MessageType schema)  {
    // Convert the schema and partition info used by spark sql to hive table properties.
    // The following code refers to the spark code in
    // https://github.com/apache/spark/blob/master/sql/hive/src/main/scala/org/apache/spark/sql/hive/HiveExternalCatalog.scala
    GroupType originGroupType = schema.asGroupType();
    List<String> partitionNames = cfg.partitionFields;
    List<Type> partitionCols = new ArrayList<>();
    List<Type> dataCols = new ArrayList<>();
    Map<String, Type> column2Field = new HashMap<>();

    for (Type field : originGroupType.getFields()) {
      column2Field.put(field.getName(), field);
    }
    // Get partition columns and data columns.
    for (String partitionName : partitionNames) {
      // Default the unknown partition fields to be String.
      // Keep the same logical with HiveSchemaUtil#getPartitionKeyType.
      partitionCols.add(column2Field.getOrDefault(partitionName,
              new PrimitiveType(Type.Repetition.REQUIRED, BINARY, partitionName, UTF8)));
    }

    for (Type field : originGroupType.getFields()) {
      if (!partitionNames.contains(field.getName())) {
        dataCols.add(field);
      }
    }

    List<Type> reOrderedFields = new ArrayList<>();
    reOrderedFields.addAll(dataCols);
    reOrderedFields.addAll(partitionCols);
    GroupType reOrderedType = new GroupType(originGroupType.getRepetition(), originGroupType.getName(), reOrderedFields);

    Map<String, String> sparkProperties = new HashMap<>();
    sparkProperties.put("spark.sql.sources.provider", "hudi");
    // Split the schema string to multi-parts according the schemaLengthThreshold size.
    String schemaString = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(reOrderedType);
    int numSchemaPart = (schemaString.length() + schemaLengthThreshold - 1) / schemaLengthThreshold;
    sparkProperties.put("spark.sql.sources.schema.numParts", String.valueOf(numSchemaPart));
    // Add each part of schema string to sparkProperties
    for (int i = 0; i < numSchemaPart; i++) {
      int start = i * schemaLengthThreshold;
      int end = Math.min(start + schemaLengthThreshold, schemaString.length());
      sparkProperties.put("spark.sql.sources.schema.part." + i, schemaString.substring(start, end));
    }
    // Add partition columns
    if (!partitionNames.isEmpty()) {
      sparkProperties.put("spark.sql.sources.schema.numPartCols", String.valueOf(partitionNames.size()));
      for (int i = 0; i < partitionNames.size(); i++) {
        sparkProperties.put("spark.sql.sources.schema.partCol." + i, partitionNames.get(i));
      }
    }
    return sparkProperties;
  }

  private Map<String, String> getSparkSerdeProperties(boolean readAsOptimized) {
    Map<String, String> sparkSerdeProperties = new HashMap<>();
    sparkSerdeProperties.put("path", cfg.basePath);
    sparkSerdeProperties.put(ConfigUtils.IS_QUERY_AS_RO_TABLE, String.valueOf(readAsOptimized));
    return sparkSerdeProperties;
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in hive, if not adds it or if the
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
