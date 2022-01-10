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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.hudi.hive.ddl.DDLExecutor;
import org.apache.hudi.hive.ddl.HMSDDLExecutor;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.hive.ddl.JDBCExecutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;

public class HoodieHiveClient extends AbstractSyncHoodieClient {

  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  private static final String HIVE_ESCAPE_CHARACTER = HiveSchemaUtil.HIVE_ESCAPE_CHARACTER;

  private static final Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
  private final PartitionValueExtractor partitionValueExtractor;
  private final HoodieTimeline activeTimeline;
  DDLExecutor ddlExecutor;
  private IMetaStoreClient client;
  private final HiveSyncConfig syncConfig;

  public HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg.basePath, cfg.assumeDatePartitioning, cfg.useFileListingFromMetadata,  cfg.withOperationField, fs);
    this.syncConfig = cfg;

    // Support JDBC, HiveQL and metastore based implementations for backwards compatibility. Future users should
    // disable jdbc and depend on metastore client for all hive registrations
    try {
      if (!StringUtils.isNullOrEmpty(cfg.syncMode)) {
        HiveSyncMode syncMode = HiveSyncMode.of(cfg.syncMode);
        switch (syncMode) {
          case HMS:
            ddlExecutor = new HMSDDLExecutor(configuration, cfg, fs);
            break;
          case HIVEQL:
            ddlExecutor = new HiveQueryDDLExecutor(cfg, fs, configuration);
            break;
          case JDBC:
            ddlExecutor = new JDBCExecutor(cfg, fs);
            break;
          default:
            throw new HoodieHiveSyncException("Invalid sync mode given " + cfg.syncMode);
        }
      } else {
        ddlExecutor = cfg.useJdbc ? new JDBCExecutor(cfg, fs) : new HiveQueryDDLExecutor(cfg, fs, configuration);
      }
      this.client = Hive.get(configuration).getMSC();
    } catch (Exception e) {
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
    ddlExecutor.addPartitionsToTable(tableName, partitionsToAdd);
  }

  /**
   * Partition path has changed - update the path for te following partitions.
   */
  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    ddlExecutor.updatePartitionsToTable(tableName, changedPartitions);
  }

  /**
   * Partition path has changed - drop the following partitions.
   */
  @Override
  public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
    ddlExecutor.dropPartitionsToTable(tableName, partitionsToDrop);
  }

  /**
   * Update the table properties to the table.
   */
  @Override
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return;
    }
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        table.putToParameters(entry.getKey(), entry.getValue());
      }
      client.alter_table(syncConfig.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table properties for table: "
          + tableName, e);
    }
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions) {
    return getPartitionEvents(tablePartitions, partitionStoragePartitions, false);
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions, boolean isDropPartition) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
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

      if (isDropPartition) {
        events.add(PartitionEvent.newPartitionDropEvent(storagePartition));
      } else {
        if (!storagePartitionValues.isEmpty()) {
          String storageValue = String.join(", ", storagePartitionValues);
          if (!paths.containsKey(storageValue)) {
            events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
          } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
            events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
          }
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
    ddlExecutor.updateTableDefinition(tableName, newSchema);
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties) {
    ddlExecutor.createTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
  }

  /**
   * Get the table schema.
   */
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    if (!doesTableExist(tableName)) {
      throw new IllegalArgumentException(
          "Failed to get schema for table " + tableName + " does not exist");
    }
    return ddlExecutor.getTableSchema(tableName);
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
   * @return true if the configured database exists
   */
  public boolean doesDataBaseExist(String databaseName) {
    try {
      client.getDatabase(databaseName);
      return true;
    } catch (NoSuchObjectException noSuchObjectException) {
      // NoSuchObjectException is thrown when there is no existing database of the name.
      return false;
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if database exists " + databaseName, e);
    }
  }

  public void createDatabase(String databaseName) {
    ddlExecutor.createDatabase(databaseName);
  }

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

  public Option<String> getLastReplicatedTime(String tableName) {
    // Get the last replicated time from the TBLproperties
    try {
      Table database = client.getTable(syncConfig.databaseName, tableName);
      return Option.ofNullable(database.getParameters().getOrDefault(GLOBALLY_CONSISTENT_READ_TIMESTAMP, null));
    } catch (NoSuchObjectException e) {
      LOG.warn("the said table not found in hms " + syncConfig.databaseName + "." + tableName);
      return Option.empty();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last replicated time from the database", e);
    }
  }

  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    if (!activeTimeline.filterCompletedInstants().getInstants()
            .anyMatch(i -> i.getTimestamp().equals(timeStamp))) {
      throw new HoodieHiveSyncException(
          "Not a valid completed timestamp " + timeStamp + " for table " + tableName);
    }
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      table.putToParameters(GLOBALLY_CONSISTENT_READ_TIMESTAMP, timeStamp);
      client.alter_table(syncConfig.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to update last replicated time to " + timeStamp + " for " + tableName, e);
    }
  }

  public void deleteLastReplicatedTimeStamp(String tableName) {
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      String timestamp = table.getParameters().remove(GLOBALLY_CONSISTENT_READ_TIMESTAMP);
      client.alter_table(syncConfig.databaseName, tableName, table);
      if (timestamp != null) {
        LOG.info("deleted last replicated timestamp " + timestamp + " for table " + tableName);
      }
    } catch (NoSuchObjectException e) {
      // this is ok the table doesn't even exist.
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to delete last replicated timestamp for " + tableName, e);
    }
  }

  public void close() {
    try {
      ddlExecutor.close();
      if (client != null) {
        Hive.closeCurrent();
        client = null;
      }
    } catch (Exception e) {
      LOG.error("Could not close connection ", e);
    }
  }

  List<String> getAllTables(String db) throws Exception {
    return client.getAllTables(db);
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the TBLproperties
    Option<String> lastCommitSynced = activeTimeline.lastInstant().map(HoodieInstant::getTimestamp);
    if (lastCommitSynced.isPresent()) {
      try {
        Table table = client.getTable(syncConfig.databaseName, tableName);
        table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
        client.alter_table(syncConfig.databaseName, tableName, table);
      } catch (Exception e) {
        throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
      }
    }
  }
}
