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
import org.apache.hudi.io.storage.HoodieFileStatus;
import org.apache.hudi.io.storage.HoodieLocation;
import org.apache.hudi.metaserver.client.HoodieMetaserverClient;
import org.apache.hudi.metaserver.client.HoodieMetaserverClientProxy;

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
  protected void transitionStateToComplete(boolean shouldLock, HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    metaserverClient.transitionInstantState(databaseName, tableName, fromInstant, toInstant, data);
  }

  @Override
  public void transitionPendingState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<byte[]> data, boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.getTimestamp().equals(toInstant.getTimestamp()));
    metaserverClient.transitionInstantState(databaseName, tableName, fromInstant, toInstant, data);
  }

  @Override
  public void createFileInMetaPath(String filename, Option<byte[]> content, boolean allowOverwrite) {
    HoodieFileStatus status = new HoodieFileStatus(new HoodieLocation(filename), 0, false, 0);
    HoodieInstant instant = new HoodieInstant(status);
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    metaserverClient.createNewInstant(databaseName, tableName, instant, Option.empty());
  }

  @Override
  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    throw new HoodieException("Unsupported now");
  }

  @Override
  protected Option<byte[]> readDataFromPath(HoodieLocation detailPath) {
    HoodieFileStatus status = new HoodieFileStatus(detailPath, 0, false, 0);
    HoodieInstant instant = new HoodieInstant(status);
    return metaserverClient.getInstantMetadata(databaseName, tableName, instant);
  }

  @Override
  public HoodieMetaserverBasedTimeline reload() {
    return new HoodieMetaserverBasedTimeline(metaClient, metaClient.getMetaserverConfig());
  }

  /**
   * Completion time is essential for {@link HoodieActiveTimeline},
   * TODO [HUDI-6883] We should change HoodieMetaserverBasedTimeline to store completion time as well.
   */
  @Override
  protected String getInstantFileName(HoodieInstant instant) {
    if (instant.isCompleted()) {
      // Set a fake completion time.
      return instant.getFileName("0").replace("_0", "");
    }

    return instant.getFileName();
  }
}
