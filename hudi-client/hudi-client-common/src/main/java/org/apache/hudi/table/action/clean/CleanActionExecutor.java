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

package org.apache.hudi.table.action.clean;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CleanActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieCleanMetadata> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(CleanActionExecutor.class);
  private final TransactionManager txnManager;
  private final boolean skipLocking;

  public CleanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    this(context, config, table, instantTime, false);
  }

  public CleanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime, boolean skipLocking) {
    super(context, config, table, instantTime);
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
    this.skipLocking = skipLocking;
  }

  private static Boolean deleteFileAndGetResult(FileSystem fs, String deletePathStr) throws IOException {
    Path deletePath = new Path(deletePathStr);
    LOG.debug("Working on delete path :" + deletePath);
    try {
      boolean isDirectory = fs.isDirectory(deletePath);
      boolean deleteResult = fs.delete(deletePath, isDirectory);
      if (deleteResult) {
        LOG.debug("Cleaned file at path :" + deletePath);
      }
      return deleteResult;
    } catch (FileNotFoundException fio) {
      // With cleanPlan being used for retried cleaning operations, its possible to clean a file twice
      return false;
    }
  }

  private static Stream<Pair<String, PartitionCleanStat>> deleteFilesFunc(Iterator<Pair<String, CleanFileInfo>> cleanFileInfo, HoodieTable table) {
    Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();
    FileSystem fs = table.getMetaClient().getFs();

    cleanFileInfo.forEachRemaining(partitionDelFileTuple -> {
      String partitionPath = partitionDelFileTuple.getLeft();
      Path deletePath = new Path(partitionDelFileTuple.getRight().getFilePath());
      String deletePathStr = deletePath.toString();
      Boolean deletedFileResult = null;
      try {
        deletedFileResult = deleteFileAndGetResult(fs, deletePathStr);

      } catch (IOException e) {
        LOG.error("Delete file failed: " + deletePathStr);
      }
      final PartitionCleanStat partitionCleanStat =
          partitionCleanStatMap.computeIfAbsent(partitionPath, k -> new PartitionCleanStat(partitionPath));
      boolean isBootstrapBasePathFile = partitionDelFileTuple.getRight().isBootstrapBaseFile();

      if (isBootstrapBasePathFile) {
        // For Bootstrap Base file deletions, store the full file path.
        partitionCleanStat.addDeleteFilePatterns(deletePath.toString(), true);
        partitionCleanStat.addDeletedFileResult(deletePath.toString(), deletedFileResult, true);
      } else {
        partitionCleanStat.addDeleteFilePatterns(deletePath.getName(), false);
        partitionCleanStat.addDeletedFileResult(deletePath.getName(), deletedFileResult, false);
      }
    });
    return partitionCleanStatMap.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue()));
  }

  /**
   * Performs cleaning of partition paths according to cleaning policy and returns the number of files cleaned. Handles
   * skews in partitions to clean by making files to clean as the unit of task distribution.
   *
   * @throws IllegalArgumentException if unknown cleaning policy is provided
   */
  List<HoodieCleanStat> clean(HoodieEngineContext context, HoodieCleanerPlan cleanerPlan) {
    int cleanerParallelism = Math.min(
        (int) (cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().mapToInt(List::size).count()),
        config.getCleanerParallelism());
    LOG.info("Using cleanerParallelism: " + cleanerParallelism);

    context.setJobStatus(this.getClass().getSimpleName(), "Perform cleaning of partitions: " + config.getTableName());

    Stream<Pair<String, CleanFileInfo>> filesToBeDeletedPerPartition =
        cleanerPlan.getFilePathsToBeDeletedPerPartition().entrySet().stream()
            .flatMap(x -> x.getValue().stream().map(y -> new ImmutablePair<>(x.getKey(),
                new CleanFileInfo(y.getFilePath(), y.getIsBootstrapBaseFile()))));

    Stream<ImmutablePair<String, PartitionCleanStat>> partitionCleanStats =
        context.mapPartitionsToPairAndReduceByKey(filesToBeDeletedPerPartition,
            iterator -> deleteFilesFunc(iterator, table), PartitionCleanStat::merge, cleanerParallelism);

    Map<String, PartitionCleanStat> partitionCleanStatsMap = partitionCleanStats
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    List<String> partitionsToBeDeleted = cleanerPlan.getPartitionsToBeDeleted() != null ? cleanerPlan.getPartitionsToBeDeleted() : new ArrayList<>();
    partitionsToBeDeleted.forEach(entry -> {
      try {
        deleteFileAndGetResult(table.getMetaClient().getFs(), table.getMetaClient().getBasePath() + "/" + entry);
      } catch (IOException e) {
        LOG.warn("Partition deletion failed " + entry);
      }
    });

    // Return PartitionCleanStat for each partition passed.
    return cleanerPlan.getFilePathsToBeDeletedPerPartition().keySet().stream().map(partitionPath -> {
      PartitionCleanStat partitionCleanStat = partitionCleanStatsMap.containsKey(partitionPath)
          ? partitionCleanStatsMap.get(partitionPath)
          : new PartitionCleanStat(partitionPath);
      HoodieActionInstant actionInstant = cleanerPlan.getEarliestInstantToRetain();
      return HoodieCleanStat.newBuilder().withPolicy(config.getCleanerPolicy()).withPartitionPath(partitionPath)
          .withEarliestCommitRetained(Option.ofNullable(
              actionInstant != null
                  ? new HoodieInstant(HoodieInstant.State.valueOf(actionInstant.getState()),
                  actionInstant.getAction(), actionInstant.getTimestamp())
                  : null))
          .withDeletePathPattern(partitionCleanStat.deletePathPatterns())
          .withSuccessfulDeletes(partitionCleanStat.successDeleteFiles())
          .withFailedDeletes(partitionCleanStat.failedDeleteFiles())
          .withDeleteBootstrapBasePathPatterns(partitionCleanStat.getDeleteBootstrapBasePathPatterns())
          .withSuccessfulDeleteBootstrapBaseFiles(partitionCleanStat.getSuccessfulDeleteBootstrapBaseFiles())
          .withFailedDeleteBootstrapBaseFiles(partitionCleanStat.getFailedDeleteBootstrapBaseFiles())
          .isPartitionDeleted(partitionsToBeDeleted.contains(partitionPath))
          .build();
    }).collect(Collectors.toList());
  }


  /**
   * Executes the Cleaner plan stored in the instant metadata.
   */
  HoodieCleanMetadata runPendingClean(HoodieTable<T, I, K, O> table, HoodieInstant cleanInstant) {
    try {
      HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(table.getMetaClient(), cleanInstant);
      return runClean(table, cleanInstant, cleanerPlan);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  private HoodieCleanMetadata runClean(HoodieTable<T, I, K, O> table, HoodieInstant cleanInstant, HoodieCleanerPlan cleanerPlan) {
    ValidationUtils.checkArgument(cleanInstant.getState().equals(HoodieInstant.State.REQUESTED)
        || cleanInstant.getState().equals(HoodieInstant.State.INFLIGHT));

    try {
      final HoodieInstant inflightInstant;
      final HoodieTimer timer = new HoodieTimer();
      timer.startTimer();
      if (cleanInstant.isRequested()) {
        inflightInstant = table.getActiveTimeline().transitionCleanRequestedToInflight(cleanInstant,
            TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
      } else {
        inflightInstant = cleanInstant;
      }

      List<HoodieCleanStat> cleanStats = clean(context, cleanerPlan);
      if (cleanStats.isEmpty()) {
        return HoodieCleanMetadata.newBuilder().build();
      }

      table.getMetaClient().reloadActiveTimeline();
      HoodieCleanMetadata metadata = CleanerUtils.convertCleanMetadata(
          inflightInstant.getTimestamp(),
          Option.of(timer.endTimer()),
          cleanStats
      );
      if (!skipLocking) {
        this.txnManager.beginTransaction(Option.empty(), Option.empty());
      }
      writeTableMetadata(metadata, inflightInstant.getTimestamp());
      table.getActiveTimeline().transitionCleanInflightToComplete(inflightInstant,
          TimelineMetadataUtils.serializeCleanMetadata(metadata));
      LOG.info("Marked clean started on " + inflightInstant.getTimestamp() + " as complete");
      return metadata;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    } finally {
      if (!skipLocking) {
        this.txnManager.endTransaction(Option.empty());
      }
    }
  }

  @Override
  public HoodieCleanMetadata execute() {
    List<HoodieCleanMetadata> cleanMetadataList = new ArrayList<>();
    // If there are inflight(failed) or previously requested clean operation, first perform them
    List<HoodieInstant> pendingCleanInstants = table.getCleanTimeline()
        .filterInflightsAndRequested().getInstants().collect(Collectors.toList());
    if (pendingCleanInstants.size() > 0) {
      // try to clean old history schema.
      try {
        FileBasedInternalSchemaStorageManager fss = new FileBasedInternalSchemaStorageManager(table.getMetaClient());
        fss.cleanOldFiles(pendingCleanInstants.stream().map(is -> is.getTimestamp()).collect(Collectors.toList()));
      } catch (Exception e) {
        // we should not affect original clean logic. Swallow exception and log warn.
        LOG.warn("failed to clean old history schema");
      }
      pendingCleanInstants.forEach(hoodieInstant -> {
        if (table.getCleanTimeline().isEmpty(hoodieInstant)) {
          table.getActiveTimeline().deleteEmptyInstantIfExists(hoodieInstant);
        } else {
          LOG.info("Finishing previously unfinished cleaner instant=" + hoodieInstant);
          try {
            cleanMetadataList.add(runPendingClean(table, hoodieInstant));
          } catch (Exception e) {
            LOG.warn("Failed to perform previous clean operation, instant: " + hoodieInstant, e);
          }
        }
        table.getMetaClient().reloadActiveTimeline();
        if (config.isMetadataTableEnabled()) {
          table.getHoodieView().sync();
        }
      });
    }

    // return the last clean metadata for now
    // TODO (NA) : Clean only the earliest pending clean just like how we do for other table services
    // This requires the CleanActionExecutor to be refactored as BaseCommitActionExecutor
    return cleanMetadataList.size() > 0 ? cleanMetadataList.get(cleanMetadataList.size() - 1) : null;
  }
}
