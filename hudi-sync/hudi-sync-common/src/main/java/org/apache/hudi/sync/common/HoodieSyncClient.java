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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.ParquetTableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.model.Partition;
import org.apache.hudi.sync.common.model.PartitionEvent;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA;

@Slf4j
public abstract class HoodieSyncClient implements HoodieMetaSyncOperations, AutoCloseable {

  protected final HoodieSyncConfig config;
  protected final PartitionValueExtractor partitionValueExtractor;
  @Getter
  protected final HoodieTableMetaClient metaClient;
  protected final ParquetTableSchemaResolver tableSchemaResolver;
  private static final String TEMP_SUFFIX = "_temp";

  public HoodieSyncClient(HoodieSyncConfig config, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.partitionValueExtractor = ReflectionUtils.loadClass(config.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS));
    this.metaClient = Objects.requireNonNull(metaClient, "metaClient is null");
    this.tableSchemaResolver = new ParquetTableSchemaResolver(metaClient);
  }

  public HoodieTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public HoodieTableType getTableType() {
    return metaClient.getTableType();
  }

  public String getBasePath() {
    return metaClient.getBasePath().toString();
  }

  public boolean isBootstrap() {
    return metaClient.getTableConfig().getBootstrapBasePath().isPresent();
  }

  public String getTableName() {
    return config.getString(META_SYNC_TABLE_NAME);
  }

  public String getDatabaseName() {
    return config.getString(META_SYNC_DATABASE_NAME);
  }

  /**
   * Get the set of dropped partitions since the last synced commit.
   * If last sync time is not known then consider only active timeline.
   * Going through archive timeline is a costly operation, and it should be avoided unless some start time is given.
   */
  public Set<String> getDroppedPartitionsSince(Option<String> lastCommitTimeSynced, Option<String> lastCommitCompletionTimeSynced) {
    return new HashSet<>(TimelineUtils.getDroppedPartitions(metaClient, lastCommitTimeSynced, lastCommitCompletionTimeSynced));
  }

  @Override
  public HoodieSchema getStorageSchema() {
    try {
      return tableSchemaResolver.getTableSchema();
    } catch (Exception e) {
      throw new HoodieSyncException(buildSchemaReadErrorMessage(e), e);
    }
  }

  @Override
  public HoodieSchema getStorageSchema(boolean includeMetadataField) {
    try {
      return tableSchemaResolver.getTableSchema(includeMetadataField);
    } catch (Exception e) {
      throw new HoodieSyncException(buildSchemaReadErrorMessage(e), e);
    }
  }

  private String buildSchemaReadErrorMessage(Exception e) {
    String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
    if (e instanceof java.io.FileNotFoundException) {
      return String.format(
          "Cannot read Hudi table schema.%n%n"
              + "Required data file missing .%n%n"
              + "This indicates:%n"
              + "  1. Aggressive cleaner retention compared to query run times\n"
              + "  2. Manual file deletions (timeline files or data files)\n"
              + "  3. Concurrent writers without proper locking or configurations set\n\n"
              + "Original error: %s", errorMessage);
    }
    return String.format("Failed to read schema from storage.%nError: %s", errorMessage);
  }

  /**
   * Gets all relative partitions paths in the Hudi table on storage.
   *
   * @return All relative partitions paths.
   */
  public List<String> getAllPartitionPathsOnStorage() {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    return FSUtils.getAllPartitionPaths(engineContext,
        metaClient,
        config.getBoolean(META_SYNC_USE_FILE_LISTING_FROM_METADATA));
  }

  public List<String> getWrittenPartitionsSince(Option<String> lastCommitTimeSynced, Option<String> lastCommitCompletionTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      log.info("Last commit time synced is not known, listing all partitions in {} , FS: {}",
          config.getString(META_SYNC_BASE_PATH), config.getHadoopFileSystem());
      return getAllPartitionPathsOnStorage();
    } else {
      log.info("Last commit time synced is {}, Getting commits since then", lastCommitTimeSynced.get());
      return TimelineUtils.getWrittenPartitions(
          TimelineUtils.getCommitsTimelineAfter(metaClient, lastCommitTimeSynced.get(), lastCommitCompletionTimeSynced));
    }
  }

  /**
   * Gets the partition events for changed partitions.
   * <p>
   * This compares the list of all partitions of a table stored in the metastore and
   * on the storage:
   * (1) Partitions exist in the metastore, but NOT the storage: drops them in the metastore;
   * (2) Partitions exist on the storage, but NOT the metastore: adds them to the metastore;
   * (3) Partitions exist in both, but the partition path is different: update them in the metastore.
   *
   * @param allPartitionsInMetastore All partitions of a table stored in the metastore.
   * @param allPartitionsOnStorage   All partitions of a table stored on the storage.
   * @return partition events for changed partitions.
   */
  public List<PartitionEvent> getPartitionEvents(List<Partition> allPartitionsInMetastore,
                                                 List<String> allPartitionsOnStorage) {
    Map<String, String> paths = getPartitionValuesToPathMapping(allPartitionsInMetastore);
    Set<String> partitionsToDrop = new HashSet<>(paths.keySet());

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : allPartitionsOnStorage) {
      Path storagePartitionPath =
          HadoopFSUtils.constructAbsolutePathInHadoopPath(config.getString(META_SYNC_BASE_PATH), storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);

      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        // Remove partitions that exist on storage from the `partitionsToDrop` set,
        // so the remaining partitions that exist in the metastore should be dropped
        partitionsToDrop.remove(storageValue);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }

    partitionsToDrop.forEach(storageValue -> {
      String storagePath = paths.get(storageValue);
      try {
        String relativePath = FSUtils.getRelativePartitionPath(
            metaClient.getBasePath(), new StoragePath(storagePath));
        events.add(PartitionEvent.newPartitionDropEvent(relativePath));
      } catch (IllegalArgumentException e) {
        log.error("Cannot parse the path stored in the metastore, ignoring it for generating DROP partition event: \"{}\".",
            storagePath, e);
      }
    });
    return events;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  public List<PartitionEvent> getPartitionEvents(List<Partition> partitionsInMetastore,
                                                 List<String> writtenPartitionsOnStorage,
                                                 Set<String> droppedPartitionsOnStorage) {
    Map<String, String> paths = getPartitionValuesToPathMapping(partitionsInMetastore);

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : writtenPartitionsOnStorage) {
      Path storagePartitionPath =
          HadoopFSUtils.constructAbsolutePathInHadoopPath(config.getString(META_SYNC_BASE_PATH), storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);

      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        if (droppedPartitionsOnStorage.contains(storagePartition)) {
          if (paths.containsKey(storageValue)) {
            // Add partition drop event only if it exists in the metastore
            events.add(PartitionEvent.newPartitionDropEvent(storagePartition));
          }
        } else {
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

  /**
   * Gets the partition values to the absolute path mapping based on the
   * partition information from the metastore.
   *
   * @param partitionsInMetastore Partitions in the metastore.
   * @return The partition values to the absolute path mapping.
   */
  private Map<String, String> getPartitionValuesToPathMapping(List<Partition> partitionsInMetastore) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : partitionsInMetastore) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getStorageLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }
    return paths;
  }

  protected String generateTempTableName(String tableName) {
    return tableName + TEMP_SUFFIX + ZonedDateTime.now().toEpochSecond();
  }
}
