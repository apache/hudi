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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.hive.ddl.DDLExecutor;
import org.apache.hudi.hive.ddl.HMSDDLExecutor;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.hive.ddl.JDBCExecutor;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class implements logic to sync a Hudi table with either the Hive server or the Hive Metastore.
 */
public class HoodieHiveClient extends AbstractHiveSyncHoodieClient {

  private static final Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
  DDLExecutor ddlExecutor;
  private IMetaStoreClient client;

  public HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg, configuration, fs);

    // Support JDBC, HiveQL and metastore based implementations for backwards compatibility. Future users should
    // disable jdbc and depend on metastore client for all hive registrations
    try {
      if (!StringUtils.isNullOrEmpty(cfg.hiveSyncConfigParams.syncMode)) {
        HiveSyncMode syncMode = HiveSyncMode.of(cfg.hiveSyncConfigParams.syncMode);
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
            throw new HoodieHiveSyncException("Invalid sync mode given " + cfg.hiveSyncConfigParams.syncMode);
        }
      } else {
        ddlExecutor = cfg.hiveSyncConfigParams.useJdbc ? new JDBCExecutor(cfg, fs) : new HiveQueryDDLExecutor(cfg, fs, configuration);
      }
      this.client = Hive.get(configuration).getMSC();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient", e);
    }
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
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
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
      Table table = client.getTable(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        table.putToParameters(entry.getKey(), entry.getValue());
      }
      client.alter_table(syncConfig.hoodieSyncConfigParams.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table properties for table: "
          + tableName, e);
    }
  }

  /**
   * Scan table partitions.
   *
   * @deprecated Use {@link #getAllPartitions} instead.
   */
  @Deprecated
  public List<org.apache.hadoop.hive.metastore.api.Partition> scanTablePartitions(String tableName) throws TException {
    return client.listPartitions(syncConfig.hoodieSyncConfigParams.databaseName, tableName, (short) -1);
  }

  @Override
  public void updateTableDefinition(String tableName, MessageType newSchema) {
    ddlExecutor.updateTableDefinition(tableName, newSchema);
  }

  @Override
  public List<Partition> getAllPartitions(String tableName) {
    try {
      return client.listPartitions(syncConfig.hoodieSyncConfigParams.databaseName, tableName, (short) -1)
          .stream()
          .map(p -> new Partition(p.getValues(), p.getSd().getLocation()))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to get all partitions for table " + tableId(syncConfig.hoodieSyncConfigParams.databaseName, tableName), e);
    }
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
    if (!tableExists(tableName)) {
      throw new IllegalArgumentException(
          "Failed to get schema for table " + tableName + " does not exist");
    }
    return ddlExecutor.getTableSchema(tableName);
  }

  @Deprecated
  @Override
  public boolean doesTableExist(String tableName) {
    return tableExists(tableName);
  }

  @Override
  public boolean tableExists(String tableName) {
    try {
      return client.tableExists(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  @Deprecated
  public boolean doesDataBaseExist(String databaseName) {
    return databaseExists(databaseName);
  }

  @Override
  public boolean databaseExists(String databaseName) {
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

  @Override
  public void createDatabase(String databaseName) {
    ddlExecutor.createDatabase(databaseName);
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    // Get the last commit time from the TBLproperties
    try {
      Table table = client.getTable(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
      return Option.ofNullable(table.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the table " + tableName, e);
    }
  }

  public Option<String> getLastReplicatedTime(String tableName) {
    // Get the last replicated time from the TBLproperties
    try {
      Table table = client.getTable(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
      return Option.ofNullable(table.getParameters().getOrDefault(GLOBALLY_CONSISTENT_READ_TIMESTAMP, null));
    } catch (NoSuchObjectException e) {
      LOG.warn("the said table not found in hms " + syncConfig.hoodieSyncConfigParams.databaseName + "." + tableName);
      return Option.empty();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last replicated time from the table " + tableName, e);
    }
  }

  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    if (!activeTimeline.filterCompletedInstants().getInstants()
            .anyMatch(i -> i.getTimestamp().equals(timeStamp))) {
      throw new HoodieHiveSyncException(
          "Not a valid completed timestamp " + timeStamp + " for table " + tableName);
    }
    try {
      Table table = client.getTable(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
      table.putToParameters(GLOBALLY_CONSISTENT_READ_TIMESTAMP, timeStamp);
      client.alter_table(syncConfig.hoodieSyncConfigParams.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to update last replicated time to " + timeStamp + " for " + tableName, e);
    }
  }

  public void deleteLastReplicatedTimeStamp(String tableName) {
    try {
      Table table = client.getTable(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
      String timestamp = table.getParameters().remove(GLOBALLY_CONSISTENT_READ_TIMESTAMP);
      client.alter_table(syncConfig.hoodieSyncConfigParams.databaseName, tableName, table);
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

  @Override
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

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the TBLproperties
    Option<String> lastCommitSynced = activeTimeline.lastInstant().map(HoodieInstant::getTimestamp);
    if (lastCommitSynced.isPresent()) {
      try {
        Table table = client.getTable(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
        table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
        client.alter_table(syncConfig.hoodieSyncConfigParams.databaseName, tableName, table);
      } catch (Exception e) {
        throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
      }
    }
  }

  @Override
  public List<FieldSchema> getTableCommentUsingMetastoreClient(String tableName) {
    try {
      return client.getSchema(syncConfig.hoodieSyncConfigParams.databaseName, tableName);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table comments for : " + tableName, e);
    }
  }

  @Override
  public void updateTableComments(String tableName, List<FieldSchema> oldSchema, List<Schema.Field> newSchema) {
    Map<String,String> newComments = newSchema.stream().collect(Collectors.toMap(field -> field.name().toLowerCase(Locale.ROOT), field -> StringUtils.isNullOrEmpty(field.doc()) ? "" : field.doc()));
    updateTableComments(tableName,oldSchema,newComments);
  }

  @Override
  public void updateTableComments(String tableName, List<FieldSchema> oldSchema, Map<String,String> newComments) {
    Map<String,String> oldComments = oldSchema.stream().collect(Collectors.toMap(fieldSchema -> fieldSchema.getName().toLowerCase(Locale.ROOT),
        fieldSchema -> StringUtils.isNullOrEmpty(fieldSchema.getComment()) ? "" : fieldSchema.getComment()));
    Map<String,String> types = oldSchema.stream().collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getType));
    Map<String, ImmutablePair<String,String>> alterComments = new HashMap<>();
    oldComments.forEach((name,comment) -> {
      String newComment = newComments.getOrDefault(name,"");
      if (!newComment.equals(comment)) {
        alterComments.put(name,new ImmutablePair<>(types.get(name),newComment));
      }
    });
    if (alterComments.size() > 0) {
      ddlExecutor.updateTableComments(tableName, alterComments);
    } else {
      LOG.info(String.format("No comment difference of %s ",tableName));
    }
  }
}
