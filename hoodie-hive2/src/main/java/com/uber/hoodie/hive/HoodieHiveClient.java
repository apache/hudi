/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package com.uber.hoodie.hive;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Reader;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.InvalidDatasetException;
import com.uber.hoodie.hive.util.SchemaUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

@SuppressWarnings("ConstantConditions")
public class HoodieHiveClient {

  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  // Make sure we have the hive JDBC driver in classpath
  private static String driverName = HiveDriver.class.getName();

  static {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + driverName + " in classpath. ", e);
    }
  }

  private static Logger LOG = LoggerFactory.getLogger(HoodieHiveClient.class);
  private final HoodieTableMetaClient metaClient;
  private final HoodieTableType tableType;
  private final PartitionValueExtractor partitionValueExtractor;
  private HiveMetaStoreClient client;
  private HiveSyncConfig syncConfig;
  private FileSystem fs;
  private Connection connection;
  private HoodieTimeline activeTimeline;

  HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    this.syncConfig = cfg;
    this.fs = fs;
    this.metaClient = new HoodieTableMetaClient(fs, cfg.basePath, true);
    this.tableType = metaClient.getTableType();

    LOG.info("Creating hive connection " + cfg.jdbcUrl);
    createHiveConnection();
    try {
      this.client = new HiveMetaStoreClient(configuration);
    } catch (MetaException e) {
      throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient", e);
    }

    try {
      this.partitionValueExtractor = (PartitionValueExtractor) Class
          .forName(cfg.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + cfg.partitionValueExtractorClass,
          e);
    }

    activeTimeline = metaClient.getActiveTimeline().getCommitsAndCompactionsTimeline()
        .filterCompletedInstants();
  }

  public HoodieTimeline getActiveTimeline() {
    return activeTimeline;
  }

  /**
   * Add the (NEW) partitons to the table
   */
  void addPartitionsToTable(List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + syncConfig.tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + syncConfig.tableName);
    String sql = constructAddPartitions(partitionsToAdd);
    updateHiveSQL(sql);
  }

  /**
   * Partition path has changed - update the path for te following partitions
   */
  void updatePartitionsToTable(List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + syncConfig.tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + syncConfig.tableName);
    List<String> sqls = constructChangePartitions(changedPartitions);
    for (String sql : sqls) {
      updateHiveSQL(sql);
    }
  }

  private String constructAddPartitions(List<String> partitions) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(syncConfig.databaseName).append(".").append(syncConfig.tableName)
        .append(" ADD IF NOT EXISTS ");
    for (String partition : partitions) {

      StringBuilder partBuilder = new StringBuilder();
      List<String> partitionValues = partitionValueExtractor
          .extractPartitionValuesInPath(partition);
      Preconditions.checkArgument(syncConfig.partitionFields.size() == partitionValues.size(),
          "Partition key parts " + syncConfig.partitionFields
              + " does not match with partition values " + partitionValues
              + ". Check partition strategy. ");
      for (int i = 0; i < syncConfig.partitionFields.size(); i++) {
        partBuilder.append(syncConfig.partitionFields.get(i)).append("=").append("'")
            .append(partitionValues.get(i)).append("'");
      }

      String fullPartitionPath = new Path(syncConfig.basePath, partition).toString();
      alterSQL.append("  PARTITION (").append(partBuilder.toString()).append(") LOCATION '")
          .append(fullPartitionPath).append("' ");
    }
    return alterSQL.toString();
  }

  private List<String> constructChangePartitions(List<String> partitions) {
    List<String> changePartitions = Lists.newArrayList();
    String alterTable = "ALTER TABLE " + syncConfig.databaseName + "." + syncConfig.tableName;
    for (String partition : partitions) {
      StringBuilder partBuilder = new StringBuilder();
      List<String> partitionValues = partitionValueExtractor
          .extractPartitionValuesInPath(partition);
      Preconditions.checkArgument(syncConfig.partitionFields.size() == partitionValues.size(),
          "Partition key parts " + syncConfig.partitionFields
              + " does not match with partition values " + partitionValues
              + ". Check partition strategy. ");
      for (int i = 0; i < syncConfig.partitionFields.size(); i++) {
        partBuilder.append(syncConfig.partitionFields.get(i)).append("=").append("'")
            .append(partitionValues.get(i)).append("'");
      }

      String fullPartitionPath = new Path(syncConfig.basePath, partition).toString();
      String changePartition =
          alterTable + " PARTITION (" + partBuilder.toString() + ") SET LOCATION '"
              + "hdfs://nameservice1" + fullPartitionPath + "'";
      changePartitions.add(changePartition);
    }
    return changePartitions;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need
   * to be added or updated. Generate a list of PartitionEvent based on the changes required.
   */
  List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions,
      List<String> partitionStoragePartitions) {
    Map<String, String> paths = Maps.newHashMap();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      Collections.sort(hivePartitionValues);
      String fullTablePartitionPath = Path
          .getPathWithoutSchemeAndAuthority(new Path(tablePartition.getSd().getLocation())).toUri()
          .getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = Lists.newArrayList();
    for (String storagePartition : partitionStoragePartitions) {
      String fullStoragePartitionPath = new Path(syncConfig.basePath, storagePartition).toString();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor
          .extractPartitionValuesInPath(storagePartition);
      Collections.sort(storagePartitionValues);
      String storageValue = String.join(", ", storagePartitionValues);
      if (!paths.containsKey(storageValue)) {
        events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
      } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
        events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
      }
    }
    return events;
  }


  /**
   * Scan table partitions
   */
  List<Partition> scanTablePartitions() throws TException {
    return client
        .listPartitions(syncConfig.databaseName, syncConfig.tableName, (short) -1);
  }

  void updateTableDefinition(MessageType newSchema) {
    try {
      String newSchemaStr = SchemaUtil.generateSchemaString(newSchema);
      // Cascade clause should not be present for non-partitioned tables
      String cascadeClause = syncConfig.partitionFields.size() > 0 ? " cascade" : "";
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append("`")
          .append(syncConfig.databaseName).append(".").append(syncConfig.tableName).append("`")
          .append(" REPLACE COLUMNS(")
          .append(newSchemaStr).append(" )").append(cascadeClause);
      LOG.info("Creating table with " + sqlBuilder);
      updateHiveSQL(sqlBuilder.toString());
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + syncConfig.tableName, e);
    }
  }

  void createTable(MessageType storageSchema,
      String inputFormatClass, String outputFormatClass, String serdeClass) {
    try {
      String createSQLQuery = SchemaUtil
          .generateCreateDDL(storageSchema, syncConfig, inputFormatClass,
              outputFormatClass, serdeClass);
      LOG.info("Creating table with " + createSQLQuery);
      updateHiveSQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + syncConfig.tableName, e);
    }
  }

  /**
   * Get the table schema
   */
  Map<String, String> getTableSchema() {
    if (!doesTableExist()) {
      throw new IllegalArgumentException(
          "Failed to get schema for table " + syncConfig.tableName + " does not exist");
    }
    Map<String, String> schema = Maps.newHashMap();
    ResultSet result = null;
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      result = databaseMetaData
          .getColumns(null, syncConfig.databaseName, syncConfig.tableName, null);
      while (result.next()) {
        String columnName = result.getString(4);
        String columnType = result.getString(6);
        schema.put(columnName, columnType);
      }
      return schema;
    } catch (SQLException e) {
      throw new HoodieHiveSyncException(
          "Failed to get table schema for " + syncConfig.tableName, e);
    } finally {
      closeQuietly(result, null);
    }
  }

  /**
   * Gets the schema for a hoodie dataset.
   * Depending on the type of table, read from any file written in the latest commit.
   * We will assume that the schema has not changed within a single atomic write.
   *
   * @return Parquet schema for this dataset
   */
  @SuppressWarnings("WeakerAccess")
  public MessageType getDataSchema() {
    try {
      switch (tableType) {
        case COPY_ON_WRITE:
          // If this is COW, get the last commit and read the schema from a file written in the last commit
          HoodieInstant lastCommit = activeTimeline.lastInstant()
              .orElseThrow(() -> new InvalidDatasetException(syncConfig.basePath));
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(activeTimeline.getInstantDetails(lastCommit).get());
          String filePath = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
              .orElseThrow(() -> new IllegalArgumentException(
                  "Could not find any data file written for commit " + lastCommit
                      + ", could not get schema for dataset " + metaClient.getBasePath()));
          return readSchemaFromDataFile(new Path(filePath));
        case MERGE_ON_READ:
          // If this is MOR, depending on whether the latest commit is a delta commit or compaction commit
          // Get a datafile written and get the schema from that file
          Optional<HoodieInstant> lastCompactionCommit = metaClient.getActiveTimeline()
              .getCompactionTimeline().filterCompletedInstants().lastInstant();
          LOG.info("Found the last compaction commit as " + lastCompactionCommit);

          Optional<HoodieInstant> lastDeltaCommitAfterCompaction = Optional.empty();
          if (lastCompactionCommit.isPresent()) {
            lastDeltaCommitAfterCompaction = metaClient.getActiveTimeline()
                .getDeltaCommitTimeline()
                .filterCompletedInstants()
                .findInstantsAfter(lastCompactionCommit.get().getTimestamp(), Integer.MAX_VALUE).lastInstant();
          }
          LOG.info("Found the last delta commit after last compaction as "
              + lastDeltaCommitAfterCompaction);

          if (lastDeltaCommitAfterCompaction.isPresent()) {
            HoodieInstant lastDeltaCommit = lastDeltaCommitAfterCompaction.get();
            // read from the log file wrote
            commitMetadata = HoodieCommitMetadata
                .fromBytes(activeTimeline.getInstantDetails(lastDeltaCommit).get());
            filePath = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().filter(s -> s.contains(
                HoodieLogFile.DELTA_EXTENSION)).findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                    "Could not find any data file written for commit " + lastDeltaCommit
                        + ", could not get schema for dataset " + metaClient.getBasePath()));
            return readSchemaFromLogFile(lastCompactionCommit, new Path(filePath));
          } else {
            return readSchemaFromLastCompaction(lastCompactionCommit);
          }
        default:
          LOG.error("Unknown table type " + tableType);
          throw new InvalidDatasetException(syncConfig.basePath);
      }
    } catch (IOException e) {
      throw new HoodieHiveSyncException(
          "Failed to get dataset schema for " + syncConfig.tableName, e);
    }
  }

  /**
   * Read schema from a data file from the last compaction commit done.
   *
   * @param lastCompactionCommitOpt
   * @return
   * @throws IOException
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MessageType readSchemaFromLastCompaction(Optional<HoodieInstant> lastCompactionCommitOpt)
      throws IOException {
    HoodieInstant lastCompactionCommit = lastCompactionCommitOpt.orElseThrow(
        () -> new HoodieHiveSyncException(
            "Could not read schema from last compaction, no compaction commits found on path "
                + syncConfig.basePath));

    // Read from the compacted file wrote
    HoodieCompactionMetadata compactionMetadata = HoodieCompactionMetadata
        .fromBytes(activeTimeline.getInstantDetails(lastCompactionCommit).get());
    String filePath = compactionMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
        .orElseThrow(() -> new IllegalArgumentException(
            "Could not find any data file written for compaction " + lastCompactionCommit
                + ", could not get schema for dataset " + metaClient.getBasePath()));
    return readSchemaFromDataFile(new Path(filePath));
  }

  /**
   * Read the schema from the log file on path
   *
   * @param lastCompactionCommitOpt
   * @param path
   * @return
   * @throws IOException
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MessageType readSchemaFromLogFile(Optional<HoodieInstant> lastCompactionCommitOpt,
      Path path) throws IOException {
    Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(path), null);
    HoodieAvroDataBlock lastBlock = null;
    while (reader.hasNext()) {
      HoodieLogBlock block = reader.next();
      if (block instanceof HoodieAvroDataBlock) {
        lastBlock = (HoodieAvroDataBlock) block;
      }
    }
    if (lastBlock != null) {
      return new org.apache.parquet.avro.AvroSchemaConverter().convert(lastBlock.getSchema());
    }
    // Fall back to read the schema from last compaction
    LOG.info("Falling back to read the schema from last compaction " + lastCompactionCommitOpt);
    return readSchemaFromLastCompaction(lastCompactionCommitOpt);
  }

  /**
   * Read the parquet schema from a parquet File
   */
  private MessageType readSchemaFromDataFile(Path parquetFilePath) throws IOException {
    LOG.info("Reading schema from " + parquetFilePath);
    if (!fs.exists(parquetFilePath)) {
      throw new IllegalArgumentException(
          "Failed to read schema from data file " + parquetFilePath
              + ". File does not exist.");
    }
    ParquetMetadata fileFooter =
        ParquetFileReader.readFooter(fs.getConf(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    return fileFooter.getFileMetaData().getSchema();
  }

  /**
   * @return true if the configured table exists
   */
  boolean doesTableExist() {
    try {
      return client.tableExists(syncConfig.databaseName, syncConfig.tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException(
          "Failed to check if table exists " + syncConfig.tableName, e);
    }
  }

  /**
   * Execute a update in hive metastore with this SQL
   *
   * @param s SQL to execute
   */
  void updateHiveSQL(String s) {
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      LOG.info("Executing SQL " + s);
      stmt.execute(s);
    } catch (SQLException e) {
      throw new HoodieHiveSyncException("Failed in executing SQL " + s, e);
    } finally {
      closeQuietly(null, stmt);
    }
  }


  private void createHiveConnection() {
    if (connection == null) {
      BasicDataSource ds = new BasicDataSource();
      ds.setDriverClassName(driverName);
      ds.setUrl(getHiveJdbcUrlWithDefaultDBName());
      ds.setUsername(syncConfig.hiveUser);
      ds.setPassword(syncConfig.hivePass);
      LOG.info("Getting Hive Connection from Datasource " + ds);
      try {
        this.connection = ds.getConnection();
      } catch (SQLException e) {
        throw new HoodieHiveSyncException(
            "Cannot create hive connection " + getHiveJdbcUrlWithDefaultDBName(), e);
      }
    }
  }

  private String getHiveJdbcUrlWithDefaultDBName() {
    String hiveJdbcUrl = syncConfig.jdbcUrl;
    String urlAppend = null;
    // If the hive url contains addition properties like ;transportMode=http;httpPath=hs2
    if (hiveJdbcUrl.contains(";")) {
      urlAppend = hiveJdbcUrl.substring(hiveJdbcUrl.indexOf(";"));
      hiveJdbcUrl = hiveJdbcUrl.substring(0, hiveJdbcUrl.indexOf(";"));
    }
    if (!hiveJdbcUrl.endsWith("/")) {
      hiveJdbcUrl = hiveJdbcUrl + "/";
    }
    return hiveJdbcUrl + syncConfig.databaseName + (urlAppend == null ? "" : urlAppend);
  }

  private static void closeQuietly(ResultSet resultSet, Statement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
      if (resultSet != null) {
        resultSet.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close the resultset opened ", e);
    }
  }

  public String getBasePath() {
    return metaClient.getBasePath();
  }

  HoodieTableType getTableType() {
    return tableType;
  }

  public FileSystem getFs() {
    return fs;
  }

  Optional<String> getLastCommitTimeSynced() {
    // Get the last commit time from the TBLproperties
    try {
      Table database = client.getTable(syncConfig.databaseName, syncConfig.tableName);
      return Optional
          .ofNullable(database.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to get the last commit time synced from the database", e);
    }
  }

  void close() {
    try {
      if (connection != null) {
        connection.close();
      }
      if(client != null) {
        client.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close connection ", e);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  List<String> getPartitionsWrittenToSince(Optional<String> lastCommitTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      LOG.info("Last commit time synced is not known, listing all partitions");
      try {
        return FSUtils
            .getAllPartitionPaths(fs, syncConfig.basePath, syncConfig.assumeDatePartitioning);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to list all partitions in " + syncConfig.basePath, e);
      }
    } else {
      LOG.info("Last commit time synced is " + lastCommitTimeSynced.get()
          + ", Getting commits since then");

      HoodieTimeline timelineToSync = activeTimeline
          .findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE);
      return timelineToSync.getInstants().map(s -> {
        try {
          return HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(s).get());
        } catch (IOException e) {
          throw new HoodieIOException(
              "Failed to get partitions written since " + lastCommitTimeSynced, e);
        }
      }).flatMap(s -> s.getPartitionToWriteStats().keySet().stream()).distinct()
          .collect(Collectors.toList());
    }
  }

  void updateLastCommitTimeSynced() {
    // Set the last commit time from the TBLproperties
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      Table table = client.getTable(syncConfig.databaseName, syncConfig.tableName);
      table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced);
      client.alter_table(syncConfig.databaseName, syncConfig.tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to get update last commit time synced to " + lastCommitSynced, e);
    }

  }

  /**
   * Partition Event captures any partition that needs to be added or updated
   */
  static class PartitionEvent {

    public enum PartitionEventType {ADD, UPDATE}

    PartitionEventType eventType;
    String storagePartition;

    PartitionEvent(
        PartitionEventType eventType, String storagePartition) {
      this.eventType = eventType;
      this.storagePartition = storagePartition;
    }

    static PartitionEvent newPartitionAddEvent(String storagePartition) {
      return new PartitionEvent(PartitionEventType.ADD, storagePartition);
    }

    static PartitionEvent newPartitionUpdateEvent(String storagePartition) {
      return new PartitionEvent(PartitionEventType.UPDATE, storagePartition);
    }
  }
}
