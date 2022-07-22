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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

import com.beust.jcommander.JCommander;

import java.util.Properties;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

/**
 * To sync with DataHub via REST APIs.
 *
 * @Experimental
 * @see <a href="https://datahubproject.io/">https://datahubproject.io/</a>
 */
public class DataHubSyncTool extends HoodieSyncTool {

  protected final DataHubSyncConfig config;

  public DataHubSyncTool(Properties props) {
    super(props);
    this.config = new DataHubSyncConfig(props);
  }

  /**
   * Sync to a DataHub Dataset.
   *
   * @implNote DataHub sync is an experimental feature, which overwrites the DataHub Dataset's schema
   * and last commit time sync'ed upon every invocation.
   */
  @Override
  public void syncHoodieTable() {
    try (DataHubSyncClient syncClient = new DataHubSyncClient(config)) {
      syncClient.updateTableSchema(config.getString(META_SYNC_TABLE_NAME), null);
      syncClient.updateLastCommitTimeSynced(config.getString(META_SYNC_TABLE_NAME), Option.empty());
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
