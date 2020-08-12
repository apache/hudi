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

package org.apache.hudi.hive.client;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieHiveClient extends AbstractSyncHoodieClient {

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

  private static final Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
  protected final PartitionValueExtractor partitionValueExtractor;
  protected IMetaStoreClient client;
  protected HiveSyncConfig syncConfig;
  protected FileSystem fs;
  protected Connection connection;
  protected HoodieTimeline activeTimeline;
  protected HiveConf configuration;

  public HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg.basePath, cfg.assumeDatePartitioning, fs);
    this.syncConfig = cfg;
    this.fs = fs;

    this.configuration = configuration;

    if (cfg.hiveClientClass.equals(HoodieHiveJDBCClient.class.getName())) {
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
  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    addPartitionsUsingMetastoreClient(tableName, partitionsToAdd);
  }

  /**
   * Partition path has changed - update the path for te following partitions.
   */
  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    changePartitionsUsingMetastoreClient(tableName, changedPartitions);
  }

  private void addPartitionsUsingMetastoreClient(String tableName, List<String> partitionsToAdd) {
    try {
      org.apache.hadoop.hive.ql.metadata.Table table = new org.apache.hadoop.hive.ql.metadata.Table(
          client.getTable(syncConfig.databaseName, tableName));
      List<Partition> hivePartitionsToAdd = new ArrayList<>();
      for (String partition : partitionsToAdd) {
        Path fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition);
        Map<String, String> partitionToSpec = client.partitionNameToSpec(
            HiveSchemaUtil.getHiveFormatPartitionString(partition, syncConfig, partitionValueExtractor));
        hivePartitionsToAdd.add(org.apache.hadoop.hive.ql.metadata.Partition
            .createMetaPartitionObject(table, partitionToSpec, fullPartitionPath));
      }
      client.add_partitions(hivePartitionsToAdd);
    } catch (HiveException | TException e) {
      throw new HoodieHiveSyncException("Failed to add partition for " + tableName, e);
    }
  }

  private void changePartitionsUsingMetastoreClient(String tableName, List<String> changedPartitions) {
    try {
      for (String partition : changedPartitions) {
        Partition newPartition = client.getPartition(syncConfig.databaseName, tableName,
            HiveSchemaUtil.getHiveFormatPartitionString(partition, syncConfig, partitionValueExtractor));
        Path partitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition);
        String partitionScheme = partitionPath.toUri().getScheme();
        String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
            ? FSUtils.getDFSFullPartitionPath(fs, partitionPath) : partitionPath.toString();
        newPartition.getSd().setLocation(fullPartitionPath);
        client.alter_partition(syncConfig.databaseName, tableName, newPartition);
      }
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to change partition for " + tableName, e);
    }
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  public List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions) {
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

  public void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      String sql = HiveSchemaUtil.generateUpdateTableDefinitionDDL(tableName, newSchema, syncConfig);
      updateHiveSQL(sql);
    } catch (IOException e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass) {
    createTableUsingMetastoreClient(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass);
  }

  private void createTableUsingMetastoreClient(String tableName, MessageType storageSchema, String inputFormatClass,
      String outputFormatClass, String serdeClass) {
    try {
      org.apache.hadoop.hive.ql.metadata.Table table = new org.apache.hadoop.hive.ql.metadata.Table(syncConfig.databaseName, tableName);
      table.setInputFormatClass(inputFormatClass);
      table.setOutputFormatClass(outputFormatClass);
      table.setSerializationLib(serdeClass);
      table.setOwner(syncConfig.hiveUser);
      table.setDataLocation(new Path(syncConfig.basePath));

      table.setProperty("EXTERNAL", "TRUE");
      table.setTableType(TableType.EXTERNAL_TABLE);

      Map<String, String> hiveSchema = HiveSchemaUtil.convertParquetSchemaToHiveSchema(storageSchema, false);
      for (Map.Entry<String, String> entry : hiveSchema.entrySet()) {
        if (!syncConfig.partitionFields.contains(entry.getKey())) {
          table.getCols().add(new FieldSchema(entry.getKey(), entry.getValue(), ""));
        }
      }
      for (String partitionField : syncConfig.partitionFields) {
        table.getPartCols().add(new FieldSchema(partitionField, HiveSchemaUtil.getPartitionKeyTypeForHiveThriftFormat(
            hiveSchema, partitionField), ""));
      }
      Hive.get(configuration).createTable(table, true);
    } catch (HiveException | IOException e) {
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  /**
   * Get the table schema.
   */
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    return getTableSchemaUsingMetastoreClient(tableName);
  }

  private Map<String, String> getTableSchemaUsingMetastoreClient(String tableName) {
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
   * @return true if the configured table exists
   */
  @Override
  public boolean doesTableExist(String tableName) {
    try {
      return client.tableExists(syncConfig.databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  /**
   * @param databaseName
   * @return  true if the configured database exists
   */
  public boolean doesDataBaseExist(String databaseName) {
    try {
      Database database = client.getDatabase(databaseName);
      if (database != null && databaseName.equals(database.getName())) {
        return true;
      }
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if database exists " + databaseName, e);
    }
    return false;
  }

  /**
   * Execute a update in hive metastore with this SQL.
   *
   * @param s SQL to execute
   */
  @Override
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
      if (client != null) {
        Hive.closeCurrent();
        client = null;
      }
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      LOG.error("Could not close connection ", e);
    }
  }

  List<String> getAllTables(String db) throws Exception {
    return client.getAllTables(db);
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
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

  // TODO: can be removed after updateTableDefinition is supported by Hive Metastore
  public void updateHiveSQL(String s) {
    if (syncConfig.hiveClientClass.equals(HoodieHiveJDBCClient.class.getName())) {
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
  // TODO: can be removed after updateTableDefinition is supported by Hive Metastore
  public CommandProcessorResponse updateHiveSQLUsingHiveDriver(String sql) {
    List<CommandProcessorResponse> responses = updateHiveSQLs(Collections.singletonList(sql));
    return responses.get(responses.size() - 1);
  }

  // TODO: can be removed after updateTableDefinition is supported by Hive Metastore
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

  // TODO: can be removed after updateTableDefinition is supported by Hive Metastore
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

  // TODO: can be removed after updateTableDefinition is supported by Hive Metastore
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
}