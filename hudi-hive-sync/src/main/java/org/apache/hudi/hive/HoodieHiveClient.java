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
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieHiveClient {

  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  // Make sure we have the hive JDBC driver in classpath
  private static String driverName = HiveDriver.class.getName();
  private static final String HIVE_ESCAPE_CHARACTER = HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;

  static {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Could not find " + driverName + " in classpath. ", e);
    }
  }

  private static final Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
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
   * Add the (NEW) partitions to the table.
   */
  void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    String sql = constructAddPartitions(tableName, partitionsToAdd);
    updateHiveSQL(sql);
  }

  /**
   * Partition path has changed - update the path for te following partitions.
   */
  void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    List<String> sqls = constructChangePartitions(tableName, changedPartitions);
    for (String sql : sqls) {
      updateHiveSQL(sql);
    }
  }

  private String constructAddPartitions(String tableName, List<String> partitions) {
    StringBuilder alterSQL = new StringBuilder("ALTER TABLE ");
    alterSQL.append(HIVE_ESCAPE_CHARACTER).append(syncConfig.databaseName)
            .append(HIVE_ESCAPE_CHARACTER).append(".").append(HIVE_ESCAPE_CHARACTER)
            .append(tableName).append(HIVE_ESCAPE_CHARACTER).append(" ADD IF NOT EXISTS ");
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
      alterSQL.append("  PARTITION (").append(partitionClause).append(") LOCATION '").append(fullPartitionPath)
          .append("' ");
    }
    return alterSQL.toString();
  }

  /**
   * Generate Hive Partition from partition values.
   *
   * @param partition Partition path
   * @return
   */
  private String getPartitionClause(String partition) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(syncConfig.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + syncConfig.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < syncConfig.partitionFields.size(); i++) {
      partBuilder.add("`" + syncConfig.partitionFields.get(i) + "`='" + partitionValues.get(i) + "'");
    }
    return String.join(",", partBuilder);
  }

  private List<String> constructChangePartitions(String tableName, List<String> partitions) {
    List<String> changePartitions = new ArrayList<>();
    // Hive 2.x doesn't like db.table name for operations, hence we need to change to using the database first
    String useDatabase = "USE " + HIVE_ESCAPE_CHARACTER + syncConfig.databaseName + HIVE_ESCAPE_CHARACTER;
    changePartitions.add(useDatabase);
    String alterTable = "ALTER TABLE " + HIVE_ESCAPE_CHARACTER + tableName + HIVE_ESCAPE_CHARACTER;
    for (String partition : partitions) {
      String partitionClause = getPartitionClause(partition);
      Path partitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition);
      String partitionScheme = partitionPath.toUri().getScheme();
      String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
              ? FSUtils.getDFSFullPartitionPath(fs, partitionPath) : partitionPath.toString();
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
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      Collections.sort(hivePartitionValues);
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getSd().getLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
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
   * Scan table partitions.
   */
  public List<Partition> scanTablePartitions(String tableName) throws TException {
    return client.listPartitions(syncConfig.databaseName, tableName, (short) -1);
  }

  void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      String newSchemaStr = HiveSchemaUtil.generateSchemaString(newSchema, syncConfig.partitionFields);
      // Cascade clause should not be present for non-partitioned tables
      String cascadeClause = syncConfig.partitionFields.size() > 0 ? " cascade" : "";
      StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ").append(HIVE_ESCAPE_CHARACTER)
              .append(syncConfig.databaseName).append(HIVE_ESCAPE_CHARACTER).append(".")
              .append(HIVE_ESCAPE_CHARACTER).append(tableName)
              .append(HIVE_ESCAPE_CHARACTER).append(" REPLACE COLUMNS(")
              .append(newSchemaStr).append(" )").append(cascadeClause);
      LOG.info("Updating table definition with " + sqlBuilder);
      updateHiveSQL(sqlBuilder.toString());
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass) {
    try {
      String createSQLQuery =
          HiveSchemaUtil.generateCreateDDL(tableName, storageSchema, syncConfig, inputFormatClass, outputFormatClass, serdeClass);
      LOG.info("Creating table with " + createSQLQuery);
      updateHiveSQL(createSQLQuery);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  /**
   * Get the table schema.
   */
  public Map<String, String> getTableSchema(String tableName) {
    if (syncConfig.useJdbc) {
      if (!doesTableExist(tableName)) {
        throw new IllegalArgumentException(
            "Failed to get schema for table " + tableName + " does not exist");
      }
      Map<String, String> schema = new HashMap<>();
      ResultSet result = null;
      try {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        result = databaseMetaData.getColumns(null, syncConfig.databaseName, tableName, null);
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
        throw new HoodieHiveSyncException("Failed to get table schema for " + tableName, e);
      } finally {
        closeQuietly(result, null);
      }
    } else {
      return getTableSchemaUsingMetastoreClient(tableName);
    }
  }

  public Map<String, String> getTableSchemaUsingMetastoreClient(String tableName) {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = this.client.getTable(syncConfig.databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      LOG.info(String.format("Time taken to getTableSchema: %s ms", (end - start)));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  /**
   * Gets the schema for a hoodie table. Depending on the type of table, try to read schema from commit metadata if
   * present, else fallback to reading from any file written in the latest commit. We will assume that the schema has
   * not changed within a single atomic write.
   *
   * @return Parquet schema for this table
   */
  public MessageType getDataSchema() {
    try {
      return new TableSchemaResolver(metaClient).getTableParquetSchema();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to read data schema", e);
    }
  }

  /**
   * @return true if the configured table exists
   */
  public boolean doesTableExist(String tableName) {
    try {
      return client.tableExists(syncConfig.databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  /**
   * Execute a update in hive metastore with this SQL.
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
   * Execute a update in hive using Hive Driver.
   *
   * @param sql SQL statement to execute
   */
  public CommandProcessorResponse updateHiveSQLUsingHiveDriver(String sql) {
    List<CommandProcessorResponse> responses = updateHiveSQLs(Collections.singletonList(sql));
    return responses.get(responses.size() - 1);
  }

  private List<CommandProcessorResponse> updateHiveSQLs(List<String> sqls) {
    SessionState ss = null;
    org.apache.hadoop.hive.ql.Driver hiveDriver = null;
    List<CommandProcessorResponse> responses = new ArrayList<>();
    try {
      final long startTime = System.currentTimeMillis();
      ss = SessionState.start(configuration);
      ss.setCurrentDatabase(syncConfig.databaseName);
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

  public Option<String> getLastCommitTimeSynced(String tableName) {
    // Get the last commit time from the TBLproperties
    try {
      Table database = client.getTable(syncConfig.databaseName, tableName);
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

  void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the TBLproperties
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced);
      client.alter_table(syncConfig.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
    }
  }

  /**
   * Partition Event captures any partition that needs to be added or updated.
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