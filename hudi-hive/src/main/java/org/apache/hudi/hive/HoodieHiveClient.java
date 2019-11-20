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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.InvalidDatasetException;
import org.apache.hudi.hive.util.SchemaUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

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

  private static Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
  private final HoodieTableMetaClient metaClient;
  private final HoodieTableType tableType;
  private final PartitionValueExtractor partitionValueExtractor;
  private IMetaStoreClient client;
  private HiveSyncConfig syncConfig;
  private FileSystem fs;
  private Connection connection;
  private HoodieTimeline activeTimeline;
  private HiveConf configuration;

  public HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    this.syncConfig = cfg;
    this.fs = fs;
    this.metaClient = new HoodieTableMetaClient(fs.getConf(), cfg.basePath, true);
    this.tableType = metaClient.getTableType();

    this.configuration = configuration;
    // Support both JDBC and metastore based implementations for backwards compatiblity. Future users should
    // disable jdbc and depend on metastore client for all hive registrations
    if (cfg.useJdbc) {
      LOG.info("Creating hive connection " + cfg.jdbcUrl);
      createHiveConnection();
    }
    try {
      this.client = Hive.get(configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient", e);
    }

    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(cfg.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + cfg.partitionValueExtractorClass, e);
    }

    activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
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
    alterSQL.append(syncConfig.databaseName).append(".").append(syncConfig.tableName).append(" ADD IF NOT EXISTS ");
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
      alterSQL.append("  PARTITION (").append(partitionClause).append(") LOCATION '").append(fullPartitionPath)
          .append("' ");
    }
    return alterSQL.toString();
  }

  /**
   * Generate Hive Partition from partition values
   * 
   * @param partition Partition path
   * @return
   */
  private String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    Preconditions.checkArgument(syncConfig.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + syncConfig.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < syncConfig.partitionFields.size(); i++) {
      partBuilder.add("`" + syncConfig.partitionFields.get(i) + "`=" + "'" + partitionValues.get(i) + "'");
    }
    return partBuilder.stream().collect(Collectors.joining(","));
  }

  private List<String> constructChangePartitions(List<String> partitions) {
    List<String> changePartitions = Lists.newArrayList();
    // Hive 2.x doesn't like db.table name for operations, hence we need to change to using the database first
    String useDatabase = "USE " + syncConfig.databaseName;
    changePartitions.add(useDatabase);
    String alterTable = "ALTER TABLE " + syncConfig.tableName;
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
      String changePartition =
          alterTable + " PARTITION (" + partitionClause + ") SET LOCATION '" + fullPartitionPath + "'";
      changePartitions.add(changePartition);
    }
    return changePartitions;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions) {
    Map<String, String> paths = Maps.newHashMap();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      Collections.sort(hivePartitionValues);
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getSd().getLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = Lists.newArrayList();
    for (String storagePartition : partitionStoragePartitions) {
      String fullStoragePartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, storagePartition).toString();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);
      Collections.sort(storagePartitionValues);
      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }
    return events;
  }

  /**
   * Scan table partitions
   */
  public List<Partition> scanTablePartitions() throws TException {
    return client.listPartitions(syncConfig.databaseName, syncConfig.tableName, (short) -1);
  }

  void updateTableDefinition(MessageType newSchema) {
    try {
      String newSchemaStr = SchemaUtil.generateSchemaString(newSchema, syncConfig.partitionFields);
      // Cascade clause should not be present for non-partitioned tables
      String cascadeClause = syncConfig.partitionFields.size() > 0 ? " cascade" : "";
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append("`").append(syncConfig.databaseName)
          .append(".").append(syncConfig.tableName).append("`").append(" REPLACE COLUMNS(").append(newSchemaStr)
          .append(" )").append(cascadeClause);
      LOG.info("Updating table definition with " + sqlBuilder);
      updateHiveSQL(sqlBuilder.toString());
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + syncConfig.tableName, e);
    }
  }

  void createTable(MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass) {
    try {
      String createSQLQuery =
          SchemaUtil.generateCreateDDL(storageSchema, syncConfig, inputFormatClass, outputFormatClass, serdeClass);
      LOG.info("Creating table with " + createSQLQuery);
      updateHiveSQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + syncConfig.tableName, e);
    }
  }

  /**
   * Get the table schema
   */
  public Map<String, String> getTableSchema() {
    if (syncConfig.useJdbc) {
      if (!doesTableExist()) {
        throw new IllegalArgumentException(
            "Failed to get schema for table " + syncConfig.tableName + " does not exist");
      }
      Map<String, String> schema = Maps.newHashMap();
      ResultSet result = null;
      try {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        result = databaseMetaData.getColumns(null, syncConfig.databaseName, syncConfig.tableName, null);
        while (result.next()) {
          String columnName = result.getString(4);
          String columnType = result.getString(6);
          if ("DECIMAL".equals(columnType)) {
            int columnSize = result.getInt("COLUMN_SIZE");
            int decimalDigits = result.getInt("DECIMAL_DIGITS");
            columnType += String.format("(%s,%s)", columnSize, decimalDigits);
          }
          schema.put(columnName, columnType);
        }
        return schema;
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Failed to get table schema for " + syncConfig.tableName, e);
      } finally {
        closeQuietly(result, null);
      }
    } else {
      return getTableSchemaUsingMetastoreClient();
    }
  }

  public Map<String, String> getTableSchemaUsingMetastoreClient() {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = this.client.getTable(syncConfig.databaseName, syncConfig.tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(f -> f.getName(), f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(f -> f.getName(), f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      LOG.info(String.format("Time taken to getTableSchema: %s ms", (end - start)));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + syncConfig.tableName, e);
    }
  }

  /**
   * Gets the schema for a hoodie dataset. Depending on the type of table, read from any file written in the latest
   * commit. We will assume that the schema has not changed within a single atomic write.
   *
   * @return Parquet schema for this dataset
   */
  @SuppressWarnings("WeakerAccess")
  public MessageType getDataSchema() {
    try {
      switch (tableType) {
        case COPY_ON_WRITE:
          // If this is COW, get the last commit and read the schema from a file written in the
          // last commit
          HoodieInstant lastCommit =
              activeTimeline.lastInstant().orElseThrow(() -> new InvalidDatasetException(syncConfig.basePath));
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(activeTimeline.getInstantDetails(lastCommit).get(), HoodieCommitMetadata.class);
          String filePath = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
              .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for commit "
                  + lastCommit + ", could not get schema for dataset " + metaClient.getBasePath() + ", Metadata :"
                  + commitMetadata));
          return readSchemaFromDataFile(new Path(filePath));
        case MERGE_ON_READ:
          // If this is MOR, depending on whether the latest commit is a delta commit or
          // compaction commit
          // Get a datafile written and get the schema from that file
          Option<HoodieInstant> lastCompactionCommit =
              metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
          LOG.info("Found the last compaction commit as " + lastCompactionCommit);

          Option<HoodieInstant> lastDeltaCommit;
          if (lastCompactionCommit.isPresent()) {
            lastDeltaCommit = metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants()
                .findInstantsAfter(lastCompactionCommit.get().getTimestamp(), Integer.MAX_VALUE).lastInstant();
          } else {
            lastDeltaCommit =
                metaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
          }
          LOG.info("Found the last delta commit " + lastDeltaCommit);

          if (lastDeltaCommit.isPresent()) {
            HoodieInstant lastDeltaInstant = lastDeltaCommit.get();
            // read from the log file wrote
            commitMetadata = HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(lastDeltaInstant).get(),
                HoodieCommitMetadata.class);
            Pair<String, HoodieFileFormat> filePathWithFormat =
                commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream()
                    .filter(s -> s.contains(HoodieLogFile.DELTA_EXTENSION)).findAny()
                    .map(f -> Pair.of(f, HoodieFileFormat.HOODIE_LOG)).orElseGet(() -> {
                      // No Log files in Delta-Commit. Check if there are any parquet files
                      return commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream()
                          .filter(s -> s.contains((metaClient.getTableConfig().getROFileFormat().getFileExtension())))
                          .findAny().map(f -> Pair.of(f, HoodieFileFormat.PARQUET)).orElseThrow(() -> {
                            return new IllegalArgumentException("Could not find any data file written for commit "
                                + lastDeltaInstant + ", could not get schema for dataset " + metaClient.getBasePath()
                                + ", CommitMetadata :" + commitMetadata);
                          });
                    });
            switch (filePathWithFormat.getRight()) {
              case HOODIE_LOG:
                return readSchemaFromLogFile(lastCompactionCommit, new Path(filePathWithFormat.getLeft()));
              case PARQUET:
                return readSchemaFromDataFile(new Path(filePathWithFormat.getLeft()));
              default:
                throw new IllegalArgumentException("Unknown file format :" + filePathWithFormat.getRight()
                    + " for file " + filePathWithFormat.getLeft());
            }
          } else {
            return readSchemaFromLastCompaction(lastCompactionCommit);
          }
        default:
          LOG.error("Unknown table type " + tableType);
          throw new InvalidDatasetException(syncConfig.basePath);
      }
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to get dataset schema for " + syncConfig.tableName, e);
    }
  }

  /**
   * Read schema from a data file from the last compaction commit done.
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MessageType readSchemaFromLastCompaction(Option<HoodieInstant> lastCompactionCommitOpt) throws IOException {
    HoodieInstant lastCompactionCommit = lastCompactionCommitOpt.orElseThrow(() -> new HoodieHiveSyncException(
        "Could not read schema from last compaction, no compaction commits found on path " + syncConfig.basePath));

    // Read from the compacted file wrote
    HoodieCommitMetadata compactionMetadata = HoodieCommitMetadata
        .fromBytes(activeTimeline.getInstantDetails(lastCompactionCommit).get(), HoodieCommitMetadata.class);
    String filePath = compactionMetadata.getFileIdAndFullPaths(metaClient.getBasePath()).values().stream().findAny()
        .orElseThrow(() -> new IllegalArgumentException("Could not find any data file written for compaction "
            + lastCompactionCommit + ", could not get schema for dataset " + metaClient.getBasePath()));
    return readSchemaFromDataFile(new Path(filePath));
  }

  /**
   * Read the schema from the log file on path
   */
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private MessageType readSchemaFromLogFile(Option<HoodieInstant> lastCompactionCommitOpt, Path path)
      throws IOException {
    MessageType messageType = SchemaUtil.readSchemaFromLogFile(fs, path);
    // Fall back to read the schema from last compaction
    if (messageType == null) {
      LOG.info("Falling back to read the schema from last compaction " + lastCompactionCommitOpt);
      return readSchemaFromLastCompaction(lastCompactionCommitOpt);
    }
    return messageType;
  }

  /**
   * Read the parquet schema from a parquet File
   */
  private MessageType readSchemaFromDataFile(Path parquetFilePath) throws IOException {
    LOG.info("Reading schema from " + parquetFilePath);
    if (!fs.exists(parquetFilePath)) {
      throw new IllegalArgumentException(
          "Failed to read schema from data file " + parquetFilePath + ". File does not exist.");
    }
    ParquetMetadata fileFooter =
        ParquetFileReader.readFooter(fs.getConf(), parquetFilePath, ParquetMetadataConverter.NO_FILTER);
    return fileFooter.getFileMetaData().getSchema();
  }

  /**
   * @return true if the configured table exists
   */
  public boolean doesTableExist() {
    try {
      return client.tableExists(syncConfig.databaseName, syncConfig.tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + syncConfig.tableName, e);
    }
  }

  /**
   * Execute a update in hive metastore with this SQL
   *
   * @param s SQL to execute
   */
  public void updateHiveSQL(String s) {
    if (syncConfig.useJdbc) {
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
    } else {
      updateHiveSQLUsingHiveDriver(s);
    }
  }

  /**
   * Execute a update in hive using Hive Driver
   *
   * @param sql SQL statement to execute
   */
  public CommandProcessorResponse updateHiveSQLUsingHiveDriver(String sql) throws HoodieHiveSyncException {
    List<CommandProcessorResponse> responses = updateHiveSQLs(Arrays.asList(sql));
    return responses.get(responses.size() - 1);
  }

  private List<CommandProcessorResponse> updateHiveSQLs(List<String> sqls) throws HoodieHiveSyncException {
    SessionState ss = null;
    org.apache.hadoop.hive.ql.Driver hiveDriver = null;
    List<CommandProcessorResponse> responses = new ArrayList<>();
    try {
      final long startTime = System.currentTimeMillis();
      ss = SessionState.start(configuration);
      hiveDriver = new org.apache.hadoop.hive.ql.Driver(configuration);
      final long endTime = System.currentTimeMillis();
      LOG.info(String.format("Time taken to start SessionState and create Driver: %s ms", (endTime - startTime)));
      for (String sql : sqls) {
        final long start = System.currentTimeMillis();
        responses.add(hiveDriver.run(sql));
        final long end = System.currentTimeMillis();
        LOG.info(String.format("Time taken to execute [%s]: %s ms", sql, (end - start)));
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL", e);
    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException ie) {
          LOG.error("Error while closing SessionState", ie);
        }
      }
      if (hiveDriver != null) {
        try {
          hiveDriver.close();
        } catch (Exception e) {
          LOG.error("Error while closing hiveDriver", e);
        }
      }
    }
    return responses;
  }

  private void createHiveConnection() {
    if (connection == null) {
      try {
        Class.forName(HiveDriver.class.getCanonicalName());
      } catch (ClassNotFoundException e) {
        LOG.error("Unable to load Hive driver class", e);
        return;
      }

      try {
        this.connection = DriverManager.getConnection(syncConfig.jdbcUrl, syncConfig.hiveUser, syncConfig.hivePass);
        LOG.info("Successfully established Hive connection to  " + syncConfig.jdbcUrl);
      } catch (SQLException e) {
        throw new HoodieHiveSyncException("Cannot create hive connection " + getHiveJdbcUrlWithDefaultDBName(), e);
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
    return hiveJdbcUrl + (urlAppend == null ? "" : urlAppend);
  }

  private static void closeQuietly(ResultSet resultSet, Statement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close the statement opened ", e);
    }

    try {
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

  public Option<String> getLastCommitTimeSynced() {
    // Get the last commit time from the TBLproperties
    try {
      Table database = client.getTable(syncConfig.databaseName, syncConfig.tableName);
      return Option.ofNullable(database.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the database", e);
    }
  }

  public void close() {
    try {
      if (connection != null) {
        connection.close();
      }
      if (client != null) {
        Hive.closeCurrent();
        client = null;
      }
    } catch (SQLException e) {
      LOG.error("Could not close connection ", e);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  List<String> getPartitionsWrittenToSince(Option<String> lastCommitTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      LOG.info("Last commit time synced is not known, listing all partitions in " + syncConfig.basePath + ",FS :" + fs);
      try {
        return FSUtils.getAllPartitionPaths(fs, syncConfig.basePath, syncConfig.assumeDatePartitioning);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to list all partitions in " + syncConfig.basePath, e);
      }
    } else {
      LOG.info("Last commit time synced is " + lastCommitTimeSynced.get() + ", Getting commits since then");

      HoodieTimeline timelineToSync = activeTimeline.findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE);
      return timelineToSync.getInstants().map(s -> {
        try {
          return HoodieCommitMetadata.fromBytes(activeTimeline.getInstantDetails(s).get(), HoodieCommitMetadata.class);
        } catch (IOException e) {
          throw new HoodieIOException("Failed to get partitions written since " + lastCommitTimeSynced, e);
        }
      }).flatMap(s -> s.getPartitionToWriteStats().keySet().stream()).distinct().collect(Collectors.toList());
    }
  }

  List<String> getAllTables(String db) throws Exception {
    return client.getAllTables(db);
  }

  void updateLastCommitTimeSynced() {
    // Set the last commit time from the TBLproperties
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      Table table = client.getTable(syncConfig.databaseName, syncConfig.tableName);
      table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced);
      client.alter_table(syncConfig.databaseName, syncConfig.tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
    }

  }

  /**
   * Partition Event captures any partition that needs to be added or updated
   */
  static class PartitionEvent {

    public enum PartitionEventType {
      ADD, UPDATE
    }

    PartitionEventType eventType;
    String storagePartition;

    PartitionEvent(PartitionEventType eventType, String storagePartition) {
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

  public IMetaStoreClient getClient() {
    return client;
  }
}
