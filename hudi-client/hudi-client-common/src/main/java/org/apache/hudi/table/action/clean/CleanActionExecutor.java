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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.blob.BlobExtractor;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;

@Slf4j
public class CleanActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, HoodieCleanMetadata> {

  private static final long serialVersionUID = 1L;
  private final TransactionManager txnManager;

  public CleanActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime);
    this.txnManager = new TransactionManager(config, table.getStorage());
  }

  private static boolean deleteFileAndGetResult(HoodieStorage storage, String deletePathStr) {
    StoragePath deletePath = new StoragePath(deletePathStr);
    log.debug("Working on delete path: {}", deletePath);
    try {
      boolean deleteResult = storage.getPathInfo(deletePath).isDirectory()
          ? storage.deleteDirectory(deletePath)
          : storage.deleteFile(deletePath);
      if (deleteResult) {
        log.debug("Cleaned file at path: {}", deletePath);
      } else {
        if (storage.exists(deletePath)) {
          throw new HoodieIOException("Failed to delete path during clean execution " + deletePath);
        } else {
          log.debug("Already cleaned up file at path: {}", deletePath);
        }
      }
      return deleteResult;
    } catch (FileNotFoundException fio) {
      // With cleanPlan being used for retried cleaning operations, its possible to clean a file twice if a file to be
      // deleted is not found, treat it as a success.  In other words, there is nothing else to be cleaned up on the
      // FileSystem, except for updating the MDT.  By returning success, we would remove the entry from MDT.
      return true;
    } catch (IOException e) {
      try {
        if (storage.exists(deletePath)) {
          log.error("Delete file failed: {} and file still exists", deletePath, e);
          throw new HoodieIOException(e.getMessage(), e);
        }
        log.warn("Delete file failed: {} but file does not exist", deletePath, e);
        return false;
      } catch (IOException ex) {
        log.error("Delete file failed: {} with exception: {} and existence check also failed", deletePath, e, ex);
        throw new HoodieIOException(ex.getMessage(), ex);
      }
    }
  }

  private static Stream<Pair<String, PartitionCleanStat>> deleteFilesFunc(Iterator<Pair<String, CleanFileInfo>> cleanFileInfo, HoodieTable table) {
    Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();
    HoodieStorage storage = table.getStorage();

    cleanFileInfo.forEachRemaining(partitionDelFileTuple -> {
      String partitionPath = partitionDelFileTuple.getLeft();
      StoragePath deletePath = new StoragePath(partitionDelFileTuple.getRight().getFilePath());
      String deletePathStr = deletePath.toString();
      boolean deletedFileResult = deleteFileAndGetResult(storage, deletePathStr);
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
        cleanerPlan.getFilePathsToBeDeletedPerPartition().values().stream().mapToInt(List::size).sum(),
        config.getCleanerParallelism());
    log.info("Using cleanerParallelism: {}", cleanerParallelism);

    context.setJobStatus(this.getClass().getSimpleName(), "Perform cleaning of table: " + config.getTableName());

    deleteBlobReferencesIfRequired(context, cleanerPlan);

    Stream<Pair<String, CleanFileInfo>> filesToBeDeletedPerPartition =
        cleanerPlan.getFilePathsToBeDeletedPerPartition().entrySet().stream()
            .flatMap(x -> x.getValue().stream().map(y -> new ImmutablePair<>(x.getKey(),
                new CleanFileInfo(y.getFilePath(), y.getIsBootstrapBaseFile()))));

    Stream<ImmutablePair<String, PartitionCleanStat>> partitionCleanStats =
        context.mapPartitionsToPairAndReduceByKey(filesToBeDeletedPerPartition,
            iterator -> deleteFilesFunc(iterator, table), PartitionCleanStat::merge, cleanerParallelism);

    Map<String, PartitionCleanStat> partitionCleanStatsMap = partitionCleanStats
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    List<String> partitionsToBeDeleted = table.getMetaClient().getTableConfig().isTablePartitioned() && cleanerPlan.getPartitionsToBeDeleted() != null
        ? cleanerPlan.getPartitionsToBeDeleted()
        : new ArrayList<>();
    partitionsToBeDeleted.forEach(entry -> {
      if (!isNullOrEmpty(entry)) {
        deleteFileAndGetResult(table.getStorage(), table.getMetaClient().getBasePath() + "/" + entry);
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
                  ? instantGenerator.createNewInstant(HoodieInstant.State.valueOf(actionInstant.getState()),
                  actionInstant.getAction(), actionInstant.getTimestamp())
                  : null))
          .withLastCompletedCommitTimestamp(cleanerPlan.getLastCompletedCommitTimestamp())
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

    HoodieInstant inflightInstant = null;
    try {
      final HoodieTimer timer = HoodieTimer.start();
      if (cleanInstant.isRequested()) {
        inflightInstant = table.getActiveTimeline().transitionCleanRequestedToInflight(cleanInstant);
      } else {
        inflightInstant = cleanInstant;
      }

      List<HoodieCleanStat> cleanStats = clean(context, cleanerPlan);
      if (cleanStats.isEmpty()) {
        return HoodieCleanMetadata.newBuilder().build();
      }

      table.getMetaClient().reloadActiveTimeline();
      HoodieCleanMetadata metadata = CleanerUtils.convertCleanMetadata(
          inflightInstant.requestedTime(),
          Option.of(timer.endTimer()),
          cleanStats,
          cleanerPlan.getExtraMetadata()
      );
      this.txnManager.beginStateChange(Option.of(inflightInstant), Option.empty());
      writeTableMetadata(metadata, inflightInstant.requestedTime());
      table.getActiveTimeline().transitionCleanInflightToComplete(
          false,
          inflightInstant,
          Option.of(metadata),
          completedInstant -> table.getMetaClient().getTableFormat().clean(metadata, completedInstant, table.getContext(), table.getMetaClient(), table.getViewManager()));
      log.info("Marked clean started on {} as complete", inflightInstant.requestedTime());
      return metadata;
    } finally {
      this.txnManager.endStateChange(Option.ofNullable(inflightInstant));
    }
  }

  @Override
  public HoodieCleanMetadata execute() {
    List<HoodieCleanMetadata> cleanMetadataList = new ArrayList<>();
    // If there are inflight(failed) or previously requested clean operation, first perform them
    List<HoodieInstant> pendingCleanInstants = table.getCleanTimeline()
        .filterInflightsAndRequested().getInstants();
    if (pendingCleanInstants.size() > 0) {
      // try to clean old history schema.
      try {
        FileBasedInternalSchemaStorageManager fss = new FileBasedInternalSchemaStorageManager(table.getMetaClient());
        fss.cleanOldFiles(pendingCleanInstants.stream().map(is -> is.requestedTime()).collect(Collectors.toList()));
      } catch (Exception e) {
        // we should not affect original clean logic. Swallow exception and log.
        log.warn("failed to clean old history schema");
      }

      for (HoodieInstant hoodieInstant : pendingCleanInstants) {
        log.info("Finishing previously unfinished cleaner instant={}", hoodieInstant);
        try {
          cleanMetadataList.add(runPendingClean(table, hoodieInstant));
        } catch (HoodieIOException e) {
          checkIfOtherWriterCommitted(hoodieInstant, e);
        } catch (Exception e) {
          log.error("Failed to perform previous clean operation, instant: {}", hoodieInstant, e);
          throw e;
        }
        if (!pendingCleanInstants.get(pendingCleanInstants.size() - 1).equals(hoodieInstant)) {
          // refresh the view of the table if there are more instants to clean
          table.getMetaClient().reloadActiveTimeline();
          if (table.getMetaClient().getTableConfig().isMetadataTableAvailable()) {
            table.getHoodieView().sync();
          }
        }
      }
    }

    // return the last clean metadata for now
    // TODO (NA) : Clean only the earliest pending clean just like how we do for other table services
    // This requires the CleanActionExecutor to be refactored as BaseCommitActionExecutor
    return cleanMetadataList.size() > 0 ? cleanMetadataList.get(cleanMetadataList.size() - 1) : null;
  }

  private void checkIfOtherWriterCommitted(HoodieInstant hoodieInstant, HoodieIOException e) {
    table.getMetaClient().reloadActiveTimeline();
    if (table.getCleanTimeline().filterCompletedInstants().containsInstant(hoodieInstant.requestedTime())) {
      log.info("Clean operation was completed by another writer for instant: {}", hoodieInstant);
    } else {
      log.error("Failed to perform previous clean operation, instant: {}", hoodieInstant, e);
      throw e;
    }
  }

  private <R> void deleteBlobReferencesIfRequired(HoodieEngineContext context, HoodieCleanerPlan cleanerPlan) {
    // check if the table schema has blob fields, if not exit early
    TableSchemaResolver schemaResolver = new TableSchemaResolver(table.getMetaClient());
    HoodieSchema tableSchema;
    try {
      tableSchema = schemaResolver.getTableSchema(false);
    } catch (Exception e) {
      log.warn("Skipping blob cleanup, failed to read schema for table: {}", config.getTableName(), e);
      return;
    }
    BlobExtractor blobExtractor = BlobExtractor.getInstance();

    // Only inspect the files for blob references if there are blob fields in the schema
    blobExtractor.getReducedBlobSchema(tableSchema).ifPresent(blobSchema -> {
      ReaderContextFactory<R> readerContextFactory = context.getReaderContextFactory(table.getMetaClient());
      // Limit the search scope for blob references to the relevant files for the given file slice that is being removed
      List<FileComparisonGroup> removedAndRetainedFiles = getBlobInspectionGroups(cleanerPlan);
      long totalRemoved = removedAndRetainedFiles.stream().flatMap(fileComparisonGroup -> {
        Set<String> removedFiles = fileComparisonGroup.getRemovedFilePaths();
        Set<String> retainedFiles = fileComparisonGroup.getRetainedFilePaths();
        HoodieTableMetaClient hoodieTableMetaClient = table.getMetaClient();
        HoodiePairData<String, Boolean> removedReferences = getBlobReferencesFromFiles(removedFiles, readerContextFactory, hoodieTableMetaClient, tableSchema, blobSchema);
        HoodiePairData<String, Boolean> retainedReferences = getBlobReferencesFromFiles(retainedFiles, readerContextFactory, hoodieTableMetaClient, tableSchema, blobSchema);
        return removedReferences.leftOuterJoin(retainedReferences)
            .filter((path, result) -> {
              boolean hasRetainedReference = result.getRight().orElse(false);
              return !hasRetainedReference;
            })
            .map(Pair::getLeft)
            .mapPartitions(pathsToDelete -> {
              HoodieStorage storage = hoodieTableMetaClient.getStorage();
              long count = 0;
              while (pathsToDelete.hasNext()) {
                String path = pathsToDelete.next();
                log.debug("Identified blob reference for deletion: {}", path);
                try {
                  storage.deleteFile(new StoragePath(path));
                  count++;
                } catch (IOException e) {
                  log.warn("Failed to delete blob reference for path: {}", path, e);
                }
              }
              return Collections.singletonList(count).iterator();
            }, true)
            .collectAsList().stream();
      }).reduce(0L, Long::sum);
      log.info("Deleted total {} blob references during clean operation", totalRemoved);
    });

  }

  private <R> HoodiePairData<String, Boolean> getBlobReferencesFromFiles(Set<String> files,
                                                                         ReaderContextFactory<R> readerContextFactory,
                                                                         HoodieTableMetaClient hoodieTableMetaClient,
                                                                         HoodieSchema tableSchema,
                                                                         HoodieSchema blobSchema) {
    return context.parallelize(new ArrayList<>(files), files.size()).flatMap(file -> {
      HoodieReaderContext<R> readerContext = readerContextFactory.getContext();
      RecordContext<R> recordContext = readerContext.getRecordContext();
      StoragePath path = new StoragePath(file);
      HoodieStorage storage = hoodieTableMetaClient.getStorage().newInstance(path, readerContext.getStorageConfiguration());
      try {
        StoragePathInfo storagePathInfo = storage.getPathInfo(path);
        BlobExtractor blobExtractor = BlobExtractor.getInstance();
        return new CloseableMappingIterator<>(readerContext.getFileRecordIterator(storagePathInfo, 0, storagePathInfo.getLength(), tableSchema, blobSchema, storage),
            record -> blobExtractor.getManagedBlobPaths(blobSchema, record, recordContext));
      } catch (IOException e) {
        log.warn("Failed to access file at path: {}, skipping blob reference extraction for this file", path, e);
        return Collections.emptyIterator();
      }
    }).flatMapToPair(paths -> paths.stream().map(path -> Pair.of(path, true)).iterator());
  }

  // TODO: How does this work for tables with global index?
  /**
   * Cleaning blob files requires that we inspect the contents of the retained and removed file slices to find the blob file references that can be removed.
   * To optimize the process, we limit the comparisons required with the following grouping logic:
   * 1) If a removed file slice has a corresponding retained file slice in the same file group, we will compare the removed file slice with the retained file slice(s) in the same file group.
   * 2) If a removed file slice does not have a corresponding retained file slice in the same file group, this implies that there is a replace or clustering commit. This requires comparing the
   *    removed file slice with all retained file slices that were created after the removed file slice's latest instant time, as these are the only retained file slices
   *    that could be part of a replace or clustering commit that removes these files from the active view of the table. All slices that fall into this case are put into the same comparison group to
   *    avoid reading the same file multiple times.
   * @param cleanerPlan the cleaner plan containing the files to be removed
   * @return the list of FileSliceComparisonGroup which contains the groups of file slices to compare for finding dereferenced blobs
   */
  List<FileComparisonGroup> getBlobInspectionGroups(HoodieCleanerPlan cleanerPlan) {
    return cleanerPlan.getFilePathsToBeDeletedPerPartition().entrySet().stream().flatMap(entry -> {
      String partitionPath = entry.getKey();
      List<String> removedFiles = entry.getValue().stream().map(HoodieCleanFileInfo::getFilePath).collect(Collectors.toList());
      Map<String, Set<String>> removedFilesGroupedByFileGroup = removedFiles.stream().collect(Collectors.groupingBy(path -> FSUtils.getFileId(new StoragePath(path).getName()), Collectors.toSet()));
      Map<String, Set<String>> activeFilesGroupedByFileGroup = new HashMap<>();
      List<HoodieFileGroup> unmatchedFileGroups = new ArrayList<>();
      List<String> deletedFilePathsWithoutNewFileSlice = new ArrayList<>();
      AtomicReference<String> earliestBaseInstantTime = new AtomicReference<>(null);
      table.getHoodieView().getAllFileGroups(partitionPath).forEach(fileGroup -> {
        if (removedFilesGroupedByFileGroup.containsKey(fileGroup.getFileGroupId().getFileId())) {
          Set<String> removedPaths = removedFilesGroupedByFileGroup.get(fileGroup.getFileGroupId().getFileId());
          Set<String> retainedPaths = new HashSet<>();
          // Only consider the slices present at the time of clean planning
          fileGroup.getAllFileSlices().filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, instantTime)).forEach(fileSlice -> {
            fileSlice.getBaseFile().ifPresent(baseFile -> {
              String baseFilePath = baseFile.getPath();
              if (!removedPaths.contains(baseFilePath)) {
                retainedPaths.add(baseFilePath);
              }
              fileSlice.getLogFiles().forEach(logFile -> {
                String logFilePath = logFile.getPath().toString();
                if (!removedPaths.contains(logFilePath)) {
                  retainedPaths.add(logFilePath);
                }
              });
            });
          });
          if (!retainedPaths.isEmpty()) {
            activeFilesGroupedByFileGroup.put(fileGroup.getFileGroupId().getFileId(), retainedPaths);
          } else {
            deletedFilePathsWithoutNewFileSlice.addAll(removedPaths);
            fileGroup.getLatestFileSlice().ifPresent(latestSlice -> {
              String latestInstantTime = latestSlice.getLatestInstantTime();
              if (earliestBaseInstantTime.get() == null || compareTimestamps(latestInstantTime, LESSER_THAN_OR_EQUALS, earliestBaseInstantTime.get())) {
                earliestBaseInstantTime.set(latestInstantTime);
              }
            });
          }
        } else {
          unmatchedFileGroups.add(fileGroup);
        }
      });
      // Cover case of file groups being removed
      Stream<FileComparisonGroup> remainingFileComparisonGroupStream = Stream.empty();
      if (!deletedFilePathsWithoutNewFileSlice.isEmpty()) {
        List<String> pathsFromPotentialReplaceOrClusteringFileSlices = unmatchedFileGroups.stream()
            .flatMap(fileGroup -> fileGroup.getAllFileSlices()
                .filter(slice -> compareTimestamps(slice.getBaseInstantTime(), LESSER_THAN_OR_EQUALS, instantTime)
                    && compareTimestamps(slice.getBaseInstantTime(), GREATER_THAN_OR_EQUALS, earliestBaseInstantTime.get()))
                .flatMap(fileSlice -> Stream.concat(
                    fileSlice.getBaseFile().map(HoodieBaseFile::getPath).map(Stream::of).orElseGet(Stream::empty),
                    fileSlice.getLogFiles().map(logFile -> logFile.getPath().toString())
                )))
            .collect(Collectors.toList());
        remainingFileComparisonGroupStream = Stream.of(FileComparisonGroup.builder()
            .retainedFilePaths(new HashSet<>(pathsFromPotentialReplaceOrClusteringFileSlices))
            .removedFilePaths(new HashSet<>(deletedFilePathsWithoutNewFileSlice))
            .build());
      }



      return Stream.concat(remainingFileComparisonGroupStream, activeFilesGroupedByFileGroup.entrySet().stream().map(kv -> {
        String fileId = kv.getKey();
        Set<String> removedPaths = removedFilesGroupedByFileGroup.get(fileId);
        Set<String> retainedPaths = kv.getValue();
        return FileComparisonGroup.builder()
            .retainedFilePaths(retainedPaths)
            .removedFilePaths(removedPaths)
            .build();
      }));
    }).collect(Collectors.toList());
  }

  @Value
  @Builder
  static class FileComparisonGroup {
    Set<String> retainedFilePaths;
    Set<String> removedFilePaths;
  }
}
