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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    return metaClient.getBasePathV2().toString();
  }

  public boolean isBootstrap() {
    return metaClient.getTableConfig().getBootstrapBasePath().isPresent();
  }

  /**
   * Get the set of dropped partitions since the last synced commit.
   * If last sync time is not known then consider only active timeline.
   * Going through archive timeline is a costly operation, and it should be avoided unless some start time is given.
   */
  public Set<String> getDroppedPartitionsSince(Option<String> lastCommitTimeSynced) {
    HoodieTimeline timeline = lastCommitTimeSynced.isPresent() ? metaClient.getArchivedTimeline(lastCommitTimeSynced.get())
        .mergeTimeline(metaClient.getActiveTimeline())
        .getCommitsTimeline()
        .findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE) : metaClient.getActiveTimeline();
    return new HashSet<>(TimelineUtils.getDroppedPartitions(timeline));
  }

  @Override
  public MessageType getStorageSchema() {
    try {
      return new TableSchemaResolver(metaClient).getTableParquetSchema();
    } catch (Exception e) {
      throw new HoodieSyncException("Failed to read schema from storage.", e);
    }
  }

  public List<String> getWrittenPartitionsSince(Option<String> lastCommitTimeSynced) {
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
      return TimelineUtils.getWrittenPartitions(
          metaClient.getArchivedTimeline(lastCommitTimeSynced.get())
              .mergeTimeline(metaClient.getActiveTimeline())
              .getCommitsTimeline()
              .findInstantsAfter(lastCommitTimeSynced.get(), Integer.MAX_VALUE));
    }
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  public List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions, Set<String> droppedPartitions) {
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

      if (droppedPartitions.contains(storagePartition)) {
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
