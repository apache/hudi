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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.sync.datahub.config.DataHubSyncConfig;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * To sync with DataHub via REST APIs.
 *
 * @Experimental
 * @see <a href="https://datahubproject.io/">https://datahubproject.io/</a>
 */
public class DataHubSyncTool extends AbstractSyncTool {

  private final DataHubSyncConfig config;

  public DataHubSyncTool(TypedProperties props, Configuration conf, FileSystem fs) {
    this(new DataHubSyncConfig(props), conf, fs);
  }

  public DataHubSyncTool(DataHubSyncConfig config, Configuration conf, FileSystem fs) {
    super(config.getProps(), conf, fs);
    this.config = config;
  }

  /**
   * Sync to a DataHub Dataset.
   *
   * @implNote DataHub sync is an experimental feature, which overwrites the DataHub Dataset's schema
   * and last commit time sync'ed upon every invocation.
   */
  @Override
  public void syncHoodieTable() {
    try (DataHubSyncClient syncClient = new DataHubSyncClient(config, conf, fs)) {
      syncClient.updateTableDefinition(config.tableName);
      syncClient.updateLastCommitTimeSynced(config.tableName);
    }
  }

  public static void main(String[] args) {
    final DataHubSyncConfig cfg = new DataHubSyncConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    FileSystem fs = FSUtils.getFs(cfg.basePath, new Configuration());
    new DataHubSyncTool(cfg, fs.getConf(), fs).syncHoodieTable();
  }
}
