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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.util.HivePartitionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class offers DDL executor backed by the hive.ql Driver This class preserves the old useJDBC = false way of doing things.
 */
@Slf4j
public class HiveQueryDDLExecutor extends QueryBasedDDLExecutor {

  private final IMetaStoreClient metaStoreClient;
  private SessionState sessionState;
  private Driver hiveDriver;

  public HiveQueryDDLExecutor(HiveSyncConfig config, IMetaStoreClient metaStoreClient) {
    super(config);
    this.metaStoreClient = metaStoreClient;
    try {
      this.sessionState = new SessionState(config.getHiveConf(),
          UserGroupInformation.getCurrentUser().getShortUserName());
      SessionState.start(this.sessionState);
      this.sessionState.setCurrentDatabase(databaseName);
      this.hiveDriver = new org.apache.hadoop.hive.ql.Driver(config.getHiveConf());
    } catch (Exception e) {
      if (sessionState != null) {
        try {
          this.sessionState.close();
        } catch (IOException ioException) {
          log.error("Error while closing SessionState", ioException);
        }
      }
      if (this.hiveDriver != null) {
        this.hiveDriver.close();
      }
      throw new HoodieHiveSyncException("Failed to create HiveQueryDDL object", e);
    }
  }

  @Override
  public void runSQL(String sql) {
    updateHiveSQLs(Collections.singletonList(sql));
  }

  private List<CommandProcessorResponse> updateHiveSQLs(List<String> sqls) {
    List<CommandProcessorResponse> responses = new ArrayList<>();
    try {
      for (String sql : sqls) {
        if (hiveDriver != null) {
          HoodieTimer timer = HoodieTimer.start();
          responses.add(hiveDriver.run(sql));
          log.info("Time taken to execute [{}]: {} ms", sql, timer.endTimer());
        }
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed in executing SQL", e);
    }
    return responses;
  }

  //TODO Duplicating it here from HMSDLExecutor as HiveQueryQL has no way of doing it on its own currently. Need to refactor it
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = metaStoreClient.getTable(databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      log.info("Time taken to getTableSchema: {} ms", (end - start));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  @Override
  public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
    if (partitionsToDrop.isEmpty()) {
      log.info("No partitions to drop for {}", tableName);
      return;
    }

    log.info("Drop partitions {} on {}", partitionsToDrop.size(), tableName);
    try {
      for (String dropPartition : partitionsToDrop) {
        if (HivePartitionUtil.partitionExists(metaStoreClient, tableName, dropPartition, partitionValueExtractor,
            config)) {
          String partitionClause =
              HivePartitionUtil.getPartitionClauseForDrop(dropPartition, partitionValueExtractor, config);
          metaStoreClient.dropPartition(databaseName, tableName, partitionClause, false);
        }
        log.info("Drop partition {} on {}", dropPartition, tableName);
      }
    } catch (Exception e) {
      log.error("{} drop partition failed", tableId(databaseName, tableName), e);
      throw new HoodieHiveSyncException(tableId(databaseName, tableName) + " drop partition failed", e);
    }
  }

  @Override
  public void close() {
    if (metaStoreClient != null) {
      Hive.closeCurrent();
    }
    if (hiveDriver != null) {
      hiveDriver.close();
    }
  }
}
