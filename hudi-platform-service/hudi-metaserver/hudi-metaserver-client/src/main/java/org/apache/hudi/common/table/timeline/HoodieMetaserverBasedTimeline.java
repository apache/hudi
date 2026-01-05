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
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantFileNameGeneratorV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.client.HoodieMetaserverClient;
import org.apache.hudi.metaserver.client.HoodieMetaserverClientProxy;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.convertMetadataToByteArray;

/**
 * Active timeline for hoodie table whose metadata is stored in the hoodie meta server instead of file system.
 * Note. MetadataServer only works with 1.x table version and will be disabled when in prior table version.
 */
public class HoodieMetaserverBasedTimeline extends ActiveTimelineV2 {
  private final String databaseName;
  private final String tableName;
  private final HoodieMetaserverClient metaserverClient;
  private final InstantGeneratorV2 instantGenerator = new InstantGeneratorV2();
  private final CommitMetadataSerDeV2 metadataSerDeV2 = new CommitMetadataSerDeV2();
  private final InstantFileNameGeneratorV2 instantFileNameGenerator = new InstantFileNameGeneratorV2();
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
  protected <T> HoodieInstant transitionStateToComplete(boolean shouldLock, HoodieInstant fromInstant, HoodieInstant toInstant, Option<T> metadata) {
    ValidationUtils.checkArgument(fromInstant.requestedTime().equals(toInstant.requestedTime()));
    return metaserverClient.transitionInstantState(databaseName, tableName, fromInstant, toInstant,
        metadata.map(m -> convertMetadataToByteArray(m, metadataSerDeV2)));
  }

  @Override
  public <T> void transitionPendingState(HoodieInstant fromInstant, HoodieInstant toInstant, Option<T> metadata, boolean allowRedundantTransitions) {
    ValidationUtils.checkArgument(fromInstant.requestedTime().equals(toInstant.requestedTime()));
    metaserverClient.transitionInstantState(databaseName, tableName, fromInstant, toInstant,
        metadata.map(m -> convertMetadataToByteArray(m, metadataSerDeV2)));
  }

  @Override
  public <T> void createFileInMetaPath(String filename, Option<T> metadata, boolean allowOverwrite) {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(filename), 0, false, (short) 0, 0, 0);
    HoodieInstant instant = instantGenerator.createNewInstant(pathInfo);
    ValidationUtils.checkArgument(instant.getState().equals(HoodieInstant.State.REQUESTED));
    metaserverClient.createNewInstant(databaseName, tableName, instant,
        metadata.map(m -> convertMetadataToByteArray(m, metadataSerDeV2)));
  }

  @Override
  protected void revertCompleteToInflight(HoodieInstant completed, HoodieInstant inflight) {
    throw new HoodieException("Unsupported now");
  }

  @Override
  protected Option<byte[]> readDataFromPath(StoragePath detailPath) {
    StoragePathInfo pathInfo = new StoragePathInfo(detailPath, 0, false, (short) 0, 0, 0);
    HoodieInstant instant = instantGenerator.createNewInstant(pathInfo);
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
      return instantFileNameGenerator.getFileName("0", instant).replace("_0", "");
    }

    return instantFileNameGenerator.getFileName(instant);
  }
}
