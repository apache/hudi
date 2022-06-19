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

package org.apache.hudi.sync.common;

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ASSUME_DATE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA;

public abstract class HoodieSyncClient implements HoodieMetaSyncOperations, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(HoodieSyncClient.class);

  protected final HoodieSyncConfig config;
  protected final PartitionValueExtractor partitionValueExtractor;
  protected final HoodieTableMetaClient metaClient;

  public HoodieSyncClient(HoodieSyncConfig config) {
    this.config = config;
    this.partitionValueExtractor = ReflectionUtils.loadClass(config.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS));
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(config.getHadoopConf())
        .setBasePath(config.getString(META_SYNC_BASE_PATH))
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  public HoodieTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public HoodieTableType getTableType() {
    return metaClient.getTableType();
  }

  public String getBasePath() {
    return metaClient.getBasePath();
  }

  public boolean isBootstrap() {
    return metaClient.getTableConfig().getBootstrapBasePath().isPresent();
  }

  public boolean isDropPartition() {
    try {
      Option<HoodieCommitMetadata> hoodieCommitMetadata = HoodieTableMetadataUtil.getLatestCommitMetadata(metaClient);

      if (hoodieCommitMetadata.isPresent()
          && WriteOperationType.DELETE_PARTITION.equals(hoodieCommitMetadata.get().getOperationType())) {
        return true;
      }
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to get commit metadata", e);
    }
    return false;
  }

  public List<String> getPartitionsWrittenToSince(Option<String> lastCommitTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      LOG.info("Last commit time synced is not known, listing all partitions in "
          + config.getString(META_SYNC_BASE_PATH)
          + ",FS :" + config.getHadoopFileSystem());
      HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
      return FSUtils.getAllPartitionPaths(engineContext,
          config.getString(META_SYNC_BASE_PATH),
          config.getBoolean(META_SYNC_USE_FILE_LISTING_FROM_METADATA),
          config.getBoolean(META_SYNC_ASSUME_DATE_PARTITION));
    } else {
      LOG.info("Last commit time synced is " + lastCommitTimeSynced.get() + ", Getting commits since then");
      return TimelineUtils.getPartitionsWritten(metaClient.getActiveTimeline().getCommitsTimeline()
          .findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE));
    }
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  public List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions, boolean isDropPartition) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getStorageLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(config.getString(META_SYNC_BASE_PATH), storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);

      if (isDropPartition) {
        events.add(PartitionEvent.newPartitionDropEvent(storagePartition));
      } else {
        if (!storagePartitionValues.isEmpty()) {
          String storageValue = String.join(", ", storagePartitionValues);
          if (!paths.containsKey(storageValue)) {
            events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
          } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
            events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
          }
        }
      }
    }
    return events;
  }
}
