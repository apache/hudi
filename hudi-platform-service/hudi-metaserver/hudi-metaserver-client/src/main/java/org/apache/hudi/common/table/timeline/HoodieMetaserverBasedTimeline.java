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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.config.HoodieMetaserverConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.client.HoodieMetaserverClient;
import org.apache.hudi.metaserver.client.HoodieMetaserverClientProxy;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

/**
 * Active timeline for hoodie table whose metadata is stored in the hoodie meta server instead of file system.
 */
public class HoodieMetaserverBasedTimeline extends HoodieActiveTimeline {
  private final String databaseName;
  private final String tableName;
  private final HoodieMetaserverClient metaserverClient;
  
  public HoodieMetaserverBasedTimeline(HoodieTableMetaClient metaClient, HoodieMetaserverConfig config) {
    this.metaClient = metaClient;
    this.metaserverClient = HoodieMetaserverClientProxy.getProxy(config);
    this.databaseName = config.getString(HoodieTableConfig.DATABASE_NAME.key());
    this.tableName = config.getString(HoodieTableConfig.NAME.key());
    this.setInstants(metaserverClient.listInstants(databaseName, tableName, 24));
  }

  @Override
  protected void deleteInstantFile(HoodieInstant instant) {
    metaserverClient.deleteInstant(databaseName, tableName, instant);
  }

  @Override
  public void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data, boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    metaserverClient.transitionInstantState(databaseName, tableName, fromInstant, toInstant, data);
  }

  @Override
  public void createFileInMetaPath(String filename, Option<byte[]> content, boolean allowOverwrite) {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(filename), 0, false, (short) 0, 0, 0);
    HoodieInstant instant = new HoodieInstant(pathInfo);
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    metaserverClient.createNewInstant(databaseName, tableName, instant, Option.empty());
  }

  @Override
  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    throw new HoodieException("Unsupported now");
  }

  @Override
  protected Option<byte[]> readDataFromPath(StoragePath detailPath) {
    StoragePathInfo pathInfo = new StoragePathInfo(detailPath, 0, false, (short) 0, 0, 0);
    HoodieInstant instant = new HoodieInstant(pathInfo);
    return metaserverClient.getInstantMetadata(databaseName, tableName, instant);
  }

  @Override
  public HoodieMetaserverBasedTimeline reload() {
    return new HoodieMetaserverBasedTimeline(metaClient, metaClient.getMetaserverConfig());
  }
}
