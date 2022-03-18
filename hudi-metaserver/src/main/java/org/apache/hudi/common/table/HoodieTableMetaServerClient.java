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

package org.apache.hudi.common.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.config.HoodieMetaServerConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieMetaServerBasedTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.client.HoodieMetaServerClient;
import org.apache.hudi.metaserver.client.RetryingHoodieMetaServerClient;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * HoodieTableMetaClient implementation for hoodie table whose metadata is stored in the hoodie meta server.
 */
public class HoodieTableMetaServerClient extends HoodieTableMetaClient {
  private static final Logger LOG = LogManager.getLogger(HoodieTableMetaServerClient.class);

  private String databaseName;
  private String tableName;
  private Table table;
  private HoodieMetaServerClient metaServerClient;

  public HoodieTableMetaServerClient(Configuration conf, ConsistencyGuardConfig consistencyGuardConfig, FileSystemRetryConfig fileSystemRetryConfig,
                                     String databaseName, String tableName, HoodieMetaServerConfig config) {
    super(conf, config.getString(HoodieWriteConfig.BASE_PATH), false, consistencyGuardConfig, Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION),
        config.getString(HoodieTableConfig.PAYLOAD_CLASS_NAME), fileSystemRetryConfig);
    if (databaseName == null || tableName == null) {
      throw new HoodieException("The database and table name have to be specified");
    }
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.metaServerConfig = config;
    this.metaServerClient = RetryingHoodieMetaServerClient.getProxy(config);
    this.table = initOrGetTable(databaseName, tableName, config);
    // TODO: transfer table parameters to table config
    this.tableConfig = new HoodieTableConfig();
    tableConfig.setTableVersion(HoodieTableVersion.THREE);
    tableConfig.setAll(config.getProps());
  }

  private Table initOrGetTable(String db, String tb, HoodieMetaServerConfig config) {
    Table table;
    try {
      table = metaServerClient.getTable(databaseName, tableName);
    } catch (HoodieException e) {
      if (e.getCause() instanceof NoSuchObjectException) {
        String user = "";
        try {
          user = UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException ioException) {
          LOG.info("Failed to get the user", ioException);
        }
        LOG.info(String.format("Table %s.%s doesn't exist, will create it.", db, tb));
        table = new Table();
        table.setDbName(db);
        table.setTableName(tb);
        table.setLocation(config.getString(HoodieWriteConfig.BASE_PATH));
        table.setOwner(user);
        table.setTableType(config.getString(HoodieTableConfig.TYPE.key()));
        metaServerClient.createTable(table);
        table = metaServerClient.getTable(databaseName, tableName);
      } else {
        throw e;
      }
    }
    return table;
  }

  /**
   * @return Hoodie Table Type
   */
  public HoodieTableType getTableType() {
    return HoodieTableType.valueOf(table.getTableType());
  }

  /**
   * Get the active instants as a timeline.
   *
   * @return Active instants timeline
   */
  public synchronized HoodieActiveTimeline getActiveTimeline() {
    if (activeTimeline == null) {
      activeTimeline = new HoodieMetaServerBasedTimeline(this, metaServerConfig);
    }
    return activeTimeline;
  }

  /**
   * Reload ActiveTimeline.
   *
   * @return Active instants timeline
   */
  public synchronized HoodieActiveTimeline reloadActiveTimeline() {
    activeTimeline = new HoodieMetaServerBasedTimeline(this, metaServerConfig);
    return activeTimeline;
  }

  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(Set<String> includedExtensions,
      boolean applyLayoutVersionFilters) {
    throw new HoodieException("Unsupport operation");
  }

  public List<HoodieInstant> scanHoodieInstantsFromFileSystem(Path timelinePath, Set<String> includedExtensions,
      boolean applyLayoutVersionFilters) {
    throw new HoodieException("Unsupport operation");
  }

  public void setBasePath(String basePath) {
    throw new HoodieException("Unsupport operation");
  }

  public void setMetaPath(String metaPath) {
    throw new HoodieException("Unsupport operation");
  }

  public void setActiveTimeline(HoodieActiveTimeline activeTimeline) {
    throw new HoodieException("Unsupport operation");
  }

  public HoodieMetaServerClient getMetaServerClient() {
    return metaServerClient;
  }

}
