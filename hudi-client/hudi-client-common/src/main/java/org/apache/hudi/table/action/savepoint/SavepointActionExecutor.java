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

import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.client.utils.MetadataTableUtils.shouldUseBatchLookup;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeCleanerPlan;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeHoodieCleanMetadata;

public class SavepointActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieSavepointMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(SavepointActionExecutor.class);

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
    if (!table.getCompletedCommitsTimeline().containsInstant(instantTime)) {
      throw new HoodieSavepointException("Could not savepoint non-existing commit " + instantTime);
    }

    try {
      // Check the last commit that was not cleaned and check if savepoint time is > that commit
      Option<HoodieInstant> cleanInstant = table.getCleanTimeline().lastInstant();
      String lastCommitRetained = cleanInstant.map(instant -> {
        try {
          if (instant.isCompleted()) {
            return deserializeHoodieCleanMetadata(
                table.getActiveTimeline().getInstantDetails(instant).get())
                .getEarliestCommitToRetain();
          } else {
            // clean is pending or inflight
            return deserializeCleanerPlan(
                table.getActiveTimeline().getInstantDetails(new HoodieInstant(REQUESTED, instant.getAction(), instant.getTimestamp())).get())
                .getEarliestInstantToRetain().getTimestamp();
          }
        } catch (IOException e) {
          throw new HoodieSavepointException("Failed to savepoint " + instantTime, e);
        }
      }).orElseGet(() -> table.getCompletedCommitsTimeline().firstInstant().get().getTimestamp());

      // Cannot allow savepoint time on a commit that could have been cleaned
      ValidationUtils.checkArgument(HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.GREATER_THAN_OR_EQUALS, lastCommitRetained),
          "Could not savepoint commit " + instantTime + " as this is beyond the lookup window " + lastCommitRetained);

      context.setJobStatus(this.getClass().getSimpleName(), "Collecting latest files for savepoint " + instantTime + " " + table.getConfig().getTableName());
      TableFileSystemView.SliceView view = table.getSliceView();

      Map<String, List<String>> latestFilesMap;
      // NOTE: for performance, we have to use different logic here for listing the latest files
      // before or on the given instant:
      // (1) using metadata-table-based file listing: instead of parallelizing the partition
      // listing which incurs unnecessary metadata table reads, we directly read the metadata
      // table once in a batch manner through the timeline server;
      // (2) using direct file system listing:  we parallelize the partition listing so that
      // each partition can be listed on the file system concurrently through Spark.
      // Note that
      if (shouldUseBatchLookup(table.getMetaClient().getTableConfig(), config)) {
        latestFilesMap = view.getAllLatestFileSlicesBeforeOrOn(instantTime).entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                  List<String> latestFiles = new ArrayList<>();
                  entry.getValue().forEach(fileSlice -> {
                    if (fileSlice.getBaseFile().isPresent()) {
                      latestFiles.add(fileSlice.getBaseFile().get().getFileName());
                    }
                    latestFiles.addAll(fileSlice.getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList()));
                  });
                  return latestFiles;
                }));
      } else {
        List<String> partitions = FSUtils.getAllPartitionPaths(
            context, table.getStorage(), config.getMetadataConfig(), table.getMetaClient().getBasePath());
        latestFilesMap = context.mapToPair(partitions, partitionPath -> {
          // Scan all partitions files with this commit time
          LOG.info("Collecting latest files in partition path " + partitionPath);
          List<String> latestFiles = new ArrayList<>();
          view.getLatestFileSlicesBeforeOrOn(partitionPath, instantTime, true).forEach(fileSlice -> {
            if (fileSlice.getBaseFile().isPresent()) {
              latestFiles.add(fileSlice.getBaseFile().get().getFileName());
            }
            latestFiles.addAll(fileSlice.getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList()));
          });
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

}
