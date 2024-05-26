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

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FileSystemRetryConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieMetaserverBasedTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.client.HoodieMetaserverClient;
import org.apache.hudi.metaserver.client.HoodieMetaserverClientProxy;
import org.apache.hudi.metaserver.thrift.NoSuchObjectException;
import org.apache.hudi.metaserver.thrift.Table;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * HoodieTableMetaClient implementation for hoodie table whose metadata is stored in the hoodie metaserver.
 */
public class HoodieTableMetaserverClient extends HoodieTableMetaClient {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTableMetaserverClient.class);

  private final String databaseName;
  private final String tableName;
  private final Table table;
  private final transient HoodieMetaserverClient metaserverClient;

  public HoodieTableMetaserverClient(HoodieStorage storage, String basePath, ConsistencyGuardConfig consistencyGuardConfig,
                                     String mergerStrategy, FileSystemRetryConfig fileSystemRetryConfig,
                                     Option<String> databaseName, Option<String> tableName, HoodieMetaserverConfig config) {
    super(storage, basePath, false, consistencyGuardConfig, Option.of(TimelineLayoutVersion.CURR_LAYOUT_VERSION),
        config.getString(HoodieTableConfig.PAYLOAD_CLASS_NAME), mergerStrategy, fileSystemRetryConfig);
    this.databaseName = databaseName.isPresent() ? databaseName.get() : tableConfig.getDatabaseName();
    this.tableName = tableName.isPresent() ? tableName.get() : tableConfig.getTableName();
    this.metaserverConfig = config;
    this.metaserverClient = HoodieMetaserverClientProxy.getProxy(config);
    this.table = initOrGetTable(config);
    // TODO: transfer table parameters to table config
    tableConfig.setTableVersion(HoodieTableVersion.current());
    tableConfig.setAll(config.getProps());
  }

  private Table initOrGetTable(HoodieMetaserverConfig config) {
    checkArgument(nonEmpty(databaseName), "database name is required.");
    checkArgument(nonEmpty(tableName), "table name is required.");
    Table table;
    try {
      table = metaserverClient.getTable(databaseName, tableName);
    } catch (HoodieException e) {
      if (e.getCause() instanceof NoSuchObjectException) {
        String user = "";
        try {
          user = UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException ioException) {
          LOG.info("Failed to get the user", ioException);
        }
        LOG.info(String.format("Table %s.%s doesn't exist, will create it.", databaseName, tableName));
        table = new Table();
        table.setDatabaseName(databaseName);
        table.setTableName(tableName);
        table.setLocation(config.getString(HoodieWriteConfig.BASE_PATH));
        table.setOwner(user);
        table.setTableType(config.getString(HoodieTableConfig.TYPE.key()));
        metaserverClient.createTable(table);
        table = metaserverClient.getTable(databaseName, tableName);
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
      activeTimeline = new HoodieMetaserverBasedTimeline(this, metaserverConfig);
    }
    return activeTimeline;
  }

  /**
   * Reload ActiveTimeline.
   *
   * @return Active instants timeline
   */
  public synchronized HoodieActiveTimeline reloadActiveTimeline() {
    activeTimeline = new HoodieMetaserverBasedTimeline(this, metaserverConfig);
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

  public HoodieMetaserverClient getMetaserverClient() {
    return metaserverClient;
  }

}
