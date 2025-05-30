/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.sync.datahub;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HadoopConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_CONDITIONAL_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.datahub.DataHubTableProperties.HoodieTableMetadata;
import static org.apache.hudi.sync.datahub.DataHubTableProperties.getTableProperties;
import static org.apache.hudi.sync.datahub.config.DataHubSyncConfig.META_SYNC_DATAHUB_TABLE_NAME;

/**
 * To sync with DataHub via REST APIs.
 *
 * @Experimental
 * @see <a href="https://datahubproject.io/">https://datahubproject.io/</a>
 */
public class DataHubSyncTool extends HoodieSyncTool {
  private static final Logger LOG = LoggerFactory.getLogger(DataHubSyncTool.class);

  protected final DataHubSyncConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected DataHubSyncClient syncClient;
  private final String tableName;

  public DataHubSyncTool(Properties props) {
    this(props, HadoopConfigUtils.createHadoopConf(props), Option.empty());
  }

  public DataHubSyncTool(Properties props, Configuration hadoopConf, Option<HoodieTableMetaClient> metaClientOption) {
    super(props, hadoopConf);
    this.config = new DataHubSyncConfig(props);
    this.tableName = config.getStringOrDefault(META_SYNC_DATAHUB_TABLE_NAME, config.getString(META_SYNC_TABLE_NAME));
    this.metaClient = metaClientOption.orElseGet(() -> buildMetaClient(config));
    this.syncClient = new DataHubSyncClient(config, metaClient);
  }

  @Override
  public void syncHoodieTable() {
    try {
      LOG.info("Syncing target Hoodie table with DataHub dataset({}). DataHub URL: {}, basePath: {}",
          tableName, config.getDataHubServerEndpoint(), config.getString(META_SYNC_BASE_PATH));

      syncSchema();
      syncTableProperties();
      updateLastCommitTimeIfNeeded();

      LOG.info("Sync completed for table {}", tableName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to sync table " + tableName + " to DataHub", e);
    } finally {
      close();
    }
  }

  private void syncSchema() throws Exception {
    syncClient.updateTableSchema(tableName, null, null);
    LOG.info("Schema synced for table {}", tableName);
  }

  private void syncTableProperties() throws Exception {
    MessageType storageSchema = syncClient.getStorageSchema();
    HoodieTableMetadata tableMetadata = new HoodieTableMetadata(metaClient, storageSchema);
    Map<String, String> tableProperties = getTableProperties(config, tableMetadata);
    syncClient.updateTableProperties(tableName, tableProperties);
    LOG.info("Properties synced for table {}", tableName);
  }

  private void updateLastCommitTimeIfNeeded() throws Exception {
    boolean shouldUpdateLastCommitTime = !config.getBoolean(META_SYNC_CONDITIONAL_SYNC);
    if (shouldUpdateLastCommitTime) {
      syncClient.updateLastCommitTimeSynced(tableName);
      LOG.info("Updated last sync time for table {}", tableName);
    }
  }

  @Override
  public void close() {
    if (syncClient != null) {
      try {
        syncClient.close();
        syncClient = null;
      } catch (Exception e) {
        LOG.error("Error closing DataHub sync client", e);
      }
    }
  }

  public static void main(String[] args) {
    final DataHubSyncConfig.DataHubSyncConfigParams params = new DataHubSyncConfig.DataHubSyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
      System.exit(0);
    }
    new DataHubSyncTool(params.toProps()).syncHoodieTable();
  }
}