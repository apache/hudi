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

package org.apache.hudi.table.action.savepoint;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SavepointActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieSavepointMetadata> {

  private static final Logger LOG = LogManager.getLogger(SavepointActionExecutor.class);

  private final String user;
  private final String comment;

  public SavepointActionExecutor(HoodieEngineContext context,
                                 HoodieWriteConfig config,
                                 HoodieTable<T, I, K, O> table,
                                 String instantTime,
                                 String user,
                                 String comment) {
    super(context, config, table, instantTime);
    this.user = user;
    this.comment = comment;
  }

  @Override
  public HoodieSavepointMetadata execute() {
    Option<HoodieInstant> cleanInstant = table.getCompletedCleanTimeline().lastInstant();
    if (!table.getCompletedCommitsTimeline().containsInstant(instantTime)) {
      throw new HoodieSavepointException("Could not savepoint non-existing commit " + instantTime);
    }

    try {
      // Check the last commit that was not cleaned and check if savepoint time is > that commit
      String lastCommitRetained;
      if (cleanInstant.isPresent()) {
        HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils
            .deserializeHoodieCleanMetadata(table.getActiveTimeline().getInstantDetails(cleanInstant.get()).get());
        lastCommitRetained = cleanMetadata.getEarliestCommitToRetain();
      } else {
        lastCommitRetained = table.getCompletedCommitsTimeline().firstInstant().get().getTimestamp();
      }

      // Cannot allow savepoint time on a commit that could have been cleaned
      ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.GREATER_THAN_OR_EQUALS, lastCommitRetained),
          "Could not savepoint commit " + instantTime + " as this is beyond the lookup window " + lastCommitRetained);

      context.setJobStatus(this.getClass().getSimpleName(), "Collecting latest files for savepoint " + instantTime + " " + table.getConfig().getTableName());
      TableFileSystemView.BaseFileOnlyView view = table.getBaseFileOnlyView();

      Map<String, List<String>> latestFilesMap;
      // NOTE: for performance, we have to use different logic here for listing the latest files
      // before or on the given instant:
      // (1) using metadata-table-based file listing: instead of parallelizing the partition
      // listing which incurs unnecessary metadata table reads, we directly read the metadata
      // table once in a batch manner through the timeline server;
      // (2) using direct file system listing:  we parallelize the partition listing so that
      // each partition can be listed on the file system concurrently through Spark.
      // Note that
      if (shouldUseBatchLookup(config)) {
        latestFilesMap = view.getAllLatestBaseFilesBeforeOrOn(instantTime).entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().map(HoodieBaseFile::getFileName).collect(Collectors.toList())));
      } else {
        List<String> partitions = FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), table.getMetaClient().getBasePath());
        latestFilesMap = context.mapToPair(partitions, partitionPath -> {
          // Scan all partitions files with this commit time
          LOG.info("Collecting latest files in partition path " + partitionPath);
          List<String> latestFiles = view.getLatestBaseFilesBeforeOrOn(partitionPath, instantTime)
              .map(HoodieBaseFile::getFileName).collect(Collectors.toList());
          return new ImmutablePair<>(partitionPath, latestFiles);
        }, null);
      }

      HoodieSavepointMetadata metadata = TimelineMetadataUtils.convertSavepointMetadata(user, comment, latestFilesMap);
      // Nothing to save in the savepoint
      table.getActiveTimeline().createNewInstant(
          new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, instantTime));
      table.getActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, instantTime),
              TimelineMetadataUtils.serializeSavepointMetadata(metadata));
      LOG.info("Savepoint " + instantTime + " created");
      return metadata;
    } catch (IOException e) {
      throw new HoodieSavepointException("Failed to savepoint " + instantTime, e);
    }
  }

  /**
   * Whether to use batch lookup for listing the latest base files in metadata table.
   * <p>
   * Note that metadata table has to be enabled, and the storage type of the file system view
   * cannot be EMBEDDED_KV_STORE or SPILLABLE_DISK (these two types are not integrated with
   * metadata table, see HUDI-5612).
   *
   * @param config Write configs.
   * @return {@code true} if using batch lookup; {@code false} otherwise.
   */
  private boolean shouldUseBatchLookup(HoodieWriteConfig config) {
    FileSystemViewStorageType storageType =
        config.getClientSpecifiedViewStorageConfig().getStorageType();
    return config.getMetadataConfig().enabled()
        && !FileSystemViewStorageType.EMBEDDED_KV_STORE.equals(storageType)
        && !FileSystemViewStorageType.SPILLABLE_DISK.equals(storageType);
  }
}
