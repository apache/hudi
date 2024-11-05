/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieMergeArchiveFilePlan;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.utils.ArchivalUtils.getMinAndMaxInstantsToKeep;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * Archiver to bound the growth of files under .hoodie meta path.
 */
public class HoodieTimelineArchiver<T extends HoodieAvroPayload, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTimelineArchiver.class);

  private final StoragePath archiveFilePath;
  private final HoodieWriteConfig config;
  private Writer writer;
  private final int maxInstantsToKeep;
  private final int minInstantsToKeep;
  private final HoodieTable<T, I, K, O> table;
  private final HoodieTableMetaClient metaClient;
  private final TransactionManager txnManager;

  public HoodieTimelineArchiver(HoodieWriteConfig config, HoodieTable<T, I, K, O> table) {
    this.config = config;
    this.table = table;
    this.metaClient = table.getMetaClient();
    this.archiveFilePath = HoodieArchivedTimeline.getArchiveLogPath(metaClient.getArchivePath());
    this.txnManager = new TransactionManager(config, table.getMetaClient().getStorage());
    Pair<Integer, Integer> minAndMaxInstants = getMinAndMaxInstantsToKeep(table, metaClient);
    this.minInstantsToKeep = minAndMaxInstants.getLeft();
    this.maxInstantsToKeep = minAndMaxInstants.getRight();
  }

  private Writer openWriter() {
    try {
      if (this.writer == null) {
        return HoodieLogFormat.newWriterBuilder().onParentPath(archiveFilePath.getParent())
            .withFileId(archiveFilePath.getName()).withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
            .withStorage(metaClient.getStorage()).overBaseCommit("").build();
      } else {
        return this.writer;
      }
    } catch (IOException e) {
      throw new HoodieException("Unable to initialize HoodieLogFormat writer", e);
    }
  }

  public Writer reOpenWriter() {
    try {
      if (this.writer != null) {
        this.writer.close();
        this.writer = null;
      }
      this.writer = openWriter();
      return writer;
    } catch (IOException e) {
      throw new HoodieException("Unable to initialize HoodieLogFormat writer", e);
    }
  }

  private void close() {
    try {
      if (this.writer != null) {
        this.writer.close();
      }
    } catch (IOException e) {
      throw new HoodieException("Unable to close HoodieLogFormat writer", e);
    }
  }

  public boolean archiveIfRequired(HoodieEngineContext context) throws IOException {
    return archiveIfRequired(context, false);
  }

  /**
   * Check if commits need to be archived. If yes, archive commits.
   */
  public boolean archiveIfRequired(HoodieEngineContext context, boolean acquireLock) throws IOException {
    try {
      if (acquireLock) {
        // there is no owner or instant time per se for archival.
        txnManager.beginTransaction(Option.empty(), Option.empty());
      }
      List<HoodieInstant> instantsToArchive = getInstantsToArchive().collect(Collectors.toList());
      verifyLastMergeArchiveFilesIfNecessary(context);
      boolean success = true;
      if (!instantsToArchive.isEmpty()) {
        this.writer = openWriter();
        LOG.info("Archiving instants " + instantsToArchive);
        archive(context, instantsToArchive);
        LOG.info("Deleting archived instants " + instantsToArchive);
        success = deleteArchivedInstants(instantsToArchive, context);
      } else {
        LOG.info("No Instants to archive");
      }

      if (shouldMergeSmallArchiveFiles()) {
        mergeArchiveFilesIfNecessary(context);
      }
      return success;
    } finally {
      close();
      if (acquireLock) {
        txnManager.endTransaction(Option.empty());
      }
    }
  }

  public boolean shouldMergeSmallArchiveFiles() {
    return config.getArchiveMergeEnable() && !StorageSchemes.isAppendSupported(metaClient.getStorage().getScheme());
  }

  /**
   * Here Hoodie can merge the small archive files into a new larger one.
   * Only used for filesystem which does not support append operation.
   * The whole merge small archive files operation has four stages:
   * 1. Build merge plan with merge candidates/merged file name infos.
   * 2. Do merge.
   * 3. Delete all the candidates.
   * 4. Delete the merge plan.
   *
   * @param context HoodieEngineContext
   * @throws IOException
   */
  private void mergeArchiveFilesIfNecessary(HoodieEngineContext context) throws IOException {
    StoragePath planPath = new StoragePath(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME);
    // Flush remained content if existed and open a new write
    reOpenWriter();
    // List all archive files
    List<StoragePathInfo> entryList = metaClient.getStorage().globEntries(
        new StoragePath(metaClient.getArchivePath() + "/.commits_.archive*"));
    // Sort files by version suffix in reverse (implies reverse chronological order)
    entryList.sort(new HoodieArchivedTimeline.ArchiveFileVersionComparator());

    int archiveMergeFilesBatchSize = config.getArchiveMergeFilesBatchSize();
    long smallFileLimitBytes = config.getArchiveMergeSmallFileLimitBytes();

    List<StoragePathInfo> mergeCandidate = getMergeCandidates(smallFileLimitBytes, entryList);

    if (mergeCandidate.size() >= archiveMergeFilesBatchSize) {
      List<String> candidateFiles = mergeCandidate.stream().map(fs -> fs.getPath().toString()).collect(Collectors.toList());
      // before merge archive files build merge plan
      String logFileName = computeLogFileName();
      buildArchiveMergePlan(candidateFiles, planPath, logFileName);
      // merge archive files
      mergeArchiveFiles(mergeCandidate);
      // after merge, delete the small archive files.
      deleteFilesParallelize(metaClient, candidateFiles, context, true);
      LOG.info("Success to delete replaced small archive files.");
      // finally, delete archiveMergePlan which means merging small archive files operation is successful.
      metaClient.getStorage().deleteFile(planPath);
      LOG.info("Success to merge small archive files.");
    }
  }

  /**
   * Find the latest 'huge archive file' index as a break point and only check/merge newer archive files.
   * Because we need to keep the original order of archive files which is important when loading archived instants with time filter.
   * {@link HoodieArchivedTimeline} loadInstants(TimeRangeFilter filter, boolean loadInstantDetails, Function<GenericRecord, Boolean> commitsFilter)
   *
   * @param smallFileLimitBytes small File Limit Bytes
   * @param entryList          Sort by version suffix in reverse
   * @return merge candidates
   */
  private List<StoragePathInfo> getMergeCandidates(long smallFileLimitBytes, List<StoragePathInfo> entryList) {
    int index = 0;
    for (; index < entryList.size(); index++) {
      if (entryList.get(index).getLength() > smallFileLimitBytes) {
        break;
      }
    }
    return entryList.stream().limit(index).collect(Collectors.toList());
  }

  /**
   * Get final written archive file name based on storageSchemes which does not support append.
   */
  private String computeLogFileName() throws IOException {
    String logWriteToken = writer.getLogFile().getLogWriteToken();
    HoodieLogFile hoodieLogFile = writer.getLogFile().rollOver(metaClient.getStorage(), logWriteToken);
    return hoodieLogFile.getFileName();
  }

  /**
   * Check/Solve if there is any failed and unfinished merge small archive files operation
   *
   * @param context HoodieEngineContext used for parallelize to delete small archive files if necessary.
   * @throws IOException
   */
  private void verifyLastMergeArchiveFilesIfNecessary(HoodieEngineContext context) throws IOException {
    if (shouldMergeSmallArchiveFiles()) {
      StoragePath planPath = new StoragePath(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME);
      HoodieStorage storage = metaClient.getStorage();
      // If plan exist, last merge small archive files was failed.
      // we need to revert or complete last action.
      if (storage.exists(planPath)) {
        HoodieMergeArchiveFilePlan plan = null;
        try {
          plan = TimelineMetadataUtils.deserializeAvroMetadata(FileIOUtils.readDataFromPath(storage, planPath).get(), HoodieMergeArchiveFilePlan.class);
        } catch (IOException e) {
          LOG.warn("Parsing merge archive plan failed.", e);
          // Reading partial plan file which means last merge action is failed during writing plan file.
          storage.deleteFile(planPath);
          return;
        }
        StoragePath mergedArchiveFile = new StoragePath(metaClient.getArchivePath(), plan.getMergedArchiveFileName());
        List<StoragePath> candidates = plan.getCandidate().stream().map(StoragePath::new).collect(Collectors.toList());
        if (candidateAllExists(candidates)) {
          // Last merge action is failed during writing merged archive file.
          // But all the small archive files are not deleted.
          // Revert last action by deleting mergedArchiveFile if existed.
          if (storage.exists(mergedArchiveFile)) {
            storage.deleteFile(mergedArchiveFile);
          }
        } else {
          // Last merge action is failed during deleting small archive files.
          // But the merged files is completed.
          // Try to complete last action
          if (storage.exists(mergedArchiveFile)) {
            deleteFilesParallelize(metaClient, plan.getCandidate(), context, true);
          }
        }

        storage.deleteFile(planPath);
      }
    }
  }

  /**
   * If all the candidate small archive files existed, last merge operation was failed during writing the merged archive file.
   * If at least one of candidate small archive files existed, the merged archive file was created and last operation was failed during deleting the small archive files.
   */
  private boolean candidateAllExists(List<StoragePath> candidates) throws IOException {
    for (StoragePath archiveFile : candidates) {
      if (!metaClient.getStorage().exists(archiveFile)) {
        // candidate is deleted
        return false;
      }
    }
    return true;
  }

  public void buildArchiveMergePlan(List<String> compactCandidate, StoragePath planPath, String compactedArchiveFileName) throws IOException {
    LOG.info("Start to build archive merge plan.");
    HoodieMergeArchiveFilePlan plan = HoodieMergeArchiveFilePlan.newBuilder()
        .setCandidate(compactCandidate)
        .setMergedArchiveFileName(compactedArchiveFileName)
        .build();
    Option<byte[]> content = TimelineMetadataUtils.serializeAvroMetadata(plan, HoodieMergeArchiveFilePlan.class);
    // building merge archive files plan.
    FileIOUtils.createFileInPath(metaClient.getStorage(), planPath, content);
    LOG.info("Success to build archive merge plan");
  }

  public void mergeArchiveFiles(List<StoragePathInfo> compactCandidate) throws IOException {
    LOG.info("Starting to merge small archive files.");
    Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
    try {
      List<IndexedRecord> records = new ArrayList<>();
      for (StoragePathInfo fs : compactCandidate) {
        // Read the archived file
        try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient.getStorage(),
            new HoodieLogFile(fs.getPath()), HoodieArchivedMetaEntry.getClassSchema())) {
          // Read the avro blocks
          while (reader.hasNext()) {
            HoodieAvroDataBlock blk = (HoodieAvroDataBlock) reader.next();
            blk.getRecordIterator(HoodieRecordType.AVRO).forEachRemaining(r -> records.add((IndexedRecord) r.getData()));
            if (records.size() >= this.config.getCommitArchivalBatchSize()) {
              writeToFile(wrapperSchema, records);
            }
          }
        }
      }
      writeToFile(wrapperSchema, records);
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to merge small archive files", e);
    } finally {
      writer.close();
    }
    LOG.info("Success to merge small archive files.");
  }

  private Map<String, Boolean> deleteFilesParallelize(HoodieTableMetaClient metaClient, List<String> paths, HoodieEngineContext context, boolean ignoreFailed) {

    return FSUtils.parallelizeFilesProcess(context,
        metaClient.getStorage(),
        config.getArchiveDeleteParallelism(),
        pairOfSubPathAndConf -> {
          StoragePath file = new StoragePath(pairOfSubPathAndConf.getKey());
          try {
            HoodieStorage storage = metaClient.getStorage();
            if (storage.exists(file)) {
              return storage.deleteFile(file);
            }
            return true;
          } catch (IOException e) {
            if (!ignoreFailed) {
              throw new HoodieIOException("Failed to delete : " + file, e);
            } else {
              LOG.warn("Ignore failed deleting : " + file);
              return true;
            }
          }
        },
        paths);
  }

  private Stream<HoodieInstant> getCleanInstantsToArchive() {
    HoodieTimeline cleanAndRollbackTimeline = table.getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION, HoodieTimeline.ROLLBACK_ACTION)).filterCompletedInstants();
    return cleanAndRollbackTimeline.getInstantsAsStream()
        .collect(Collectors.groupingBy(HoodieInstant::getAction)).values().stream()
        .map(hoodieInstants -> {
          if (hoodieInstants.size() > this.maxInstantsToKeep) {
            return hoodieInstants.subList(0, hoodieInstants.size() - this.minInstantsToKeep);
          } else {
            return new ArrayList<HoodieInstant>();
          }
        }).flatMap(Collection::stream);
  }

  private Stream<HoodieInstant> getCommitInstantsToArchive() throws IOException {
    // TODO (na) : Add a way to return actions associated with a timeline and then merge/unify
    // with logic above to avoid Stream.concat
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();

    // Get the oldest inflight instant and a completed commit before this inflight instant.
    Option<HoodieInstant> oldestPendingInstant = table.getActiveTimeline()
        .getWriteTimeline()
        .filter(instant -> !instant.isCompleted())
        .firstInstant();

    // Oldest commit to retain is the greatest completed commit, that is less than the oldest pending instant.
    // In some cases when inflight is the lowest commit then oldest commit to retain will be equal to oldest
    // inflight commit.
    Option<HoodieInstant> oldestCommitToRetain;
    if (oldestPendingInstant.isPresent()) {
      Option<HoodieInstant> completedCommitBeforeOldestPendingInstant =
          Option.fromJavaOptional(commitTimeline.getReverseOrderedInstants()
              .filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(),
                  LESSER_THAN, oldestPendingInstant.get().getTimestamp())).findFirst());
      // Check if the completed instant is higher than the oldest inflight instant
      // in that case update the oldestCommitToRetain to oldestInflight commit time.
      if (!completedCommitBeforeOldestPendingInstant.isPresent()
          || HoodieTimeline.compareTimestamps(oldestPendingInstant.get().getTimestamp(),
          LESSER_THAN, completedCommitBeforeOldestPendingInstant.get().getTimestamp())) {
        oldestCommitToRetain = oldestPendingInstant;
      } else {
        oldestCommitToRetain = completedCommitBeforeOldestPendingInstant;
      }
    } else {
      oldestCommitToRetain = Option.empty();
    }

    // NOTE: We cannot have any holes in the commit timeline.
    // We cannot archive any commits which are made after the first savepoint present,
    // unless HoodieArchivalConfig#ARCHIVE_BEYOND_SAVEPOINT is enabled.
    Option<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
    Set<String> savepointTimestamps = table.getSavepointTimestamps();
    if (!commitTimeline.empty() && commitTimeline.countInstants() > maxInstantsToKeep) {
      // For Merge-On-Read table, inline or async compaction is enabled
      // We need to make sure that there are enough delta commits in the active timeline
      // to trigger compaction scheduling, when the trigger strategy of compaction is
      // NUM_COMMITS or NUM_AND_TIME.
      Option<HoodieInstant> oldestInstantToRetainForCompaction =
          (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ
              && (config.getInlineCompactTriggerStrategy() == CompactionTriggerStrategy.NUM_COMMITS
              || config.getInlineCompactTriggerStrategy() == CompactionTriggerStrategy.NUM_AND_TIME))
              ? CompactionUtils.getOldestInstantToRetainForCompaction(
              table.getActiveTimeline(), config.getInlineCompactDeltaCommitMax())
              : Option.empty();

      // The clustering commit instant can not be archived unless we ensure that the replaced files have been cleaned,
      // without the replaced files metadata on the timeline, the fs view would expose duplicates for readers.
      // Meanwhile, when inline or async clustering is enabled, we need to ensure that there is a commit in the active timeline
      // to check whether the file slice generated in pending clustering after archive isn't committed.
      Option<HoodieInstant> oldestInstantToRetainForClustering =
          ClusteringUtils.getOldestInstantToRetainForClustering(table.getActiveTimeline(), table.getMetaClient());

      // Actually do the commits
      Stream<HoodieInstant> instantToArchiveStream = commitTimeline.getInstantsAsStream()
          .filter(s -> {
            if (config.shouldArchiveBeyondSavepoint()) {
              // skip savepoint commits and proceed further
              return !savepointTimestamps.contains(s.getTimestamp());
            } else {
              // if no savepoint present, then don't filter
              // stop at first savepoint commit
              return !(firstSavepoint.isPresent() && compareTimestamps(firstSavepoint.get().getTimestamp(), LESSER_THAN_OR_EQUALS, s.getTimestamp()));
            }
          }).filter(s -> {
            // oldestCommitToRetain is the highest completed commit instant that is less than the oldest inflight instant.
            // By filtering out any commit >= oldestCommitToRetain, we can ensure there are no gaps in the timeline
            // when inflight commits are present.
            return oldestCommitToRetain
                .map(instant -> compareTimestamps(instant.getTimestamp(), GREATER_THAN, s.getTimestamp()))
                .orElse(true);
          }).filter(s ->
              oldestInstantToRetainForCompaction.map(instantToRetain ->
                      compareTimestamps(s.getTimestamp(), LESSER_THAN, instantToRetain.getTimestamp()))
                  .orElse(true)
          ).filter(s ->
              oldestInstantToRetainForClustering.map(instantToRetain ->
                      HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN, instantToRetain.getTimestamp()))
                  .orElse(true)
          );
      return instantToArchiveStream.limit(commitTimeline.countInstants() - minInstantsToKeep);
    } else {
      return Stream.empty();
    }
  }

  private Stream<HoodieInstant> getInstantsToArchive() throws IOException {
    Stream<HoodieInstant> instants = Stream.concat(getCleanInstantsToArchive(), getCommitInstantsToArchive());
    if (config.isMetaserverEnabled()) {
      return Stream.empty();
    }

    // For archiving and cleaning instants, we need to include intermediate state files if they exist
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    Map<Pair<String, String>, List<HoodieInstant>> groupByTsAction = rawActiveTimeline.getInstantsAsStream()
        .collect(Collectors.groupingBy(i -> Pair.of(i.getTimestamp(),
            HoodieInstant.getComparableAction(i.getAction()))));

    // If metadata table is enabled, do not archive instants which are more recent than the last compaction on the
    // metadata table.
    if (table.getMetaClient().getTableConfig().isMetadataTableAvailable()) {
      try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(table.getContext(), table.getStorage(), config.getMetadataConfig(), config.getBasePath())) {
        Option<String> latestCompactionTime = tableMetadata.getLatestCompactionTime();
        if (!latestCompactionTime.isPresent()) {
          LOG.info("Not archiving as there is no compaction yet on the metadata table");
          instants = Stream.empty();
        } else {
          LOG.info("Limiting archiving of instants to latest compaction on metadata table at " + latestCompactionTime.get());
          instants = instants.filter(instant -> compareTimestamps(instant.getTimestamp(), LESSER_THAN,
              latestCompactionTime.get()));
        }
      } catch (Exception e) {
        throw new HoodieException("Error limiting instant archival based on metadata table", e);
      }
    }

    if (table.isMetadataTable()) {
      HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(HoodieTableMetadata.getDatasetBasePath(config.getBasePath()))
          .setConf(metaClient.getStorageConf())
          .build();
      Option<HoodieInstant> qualifiedEarliestInstant =
          TimelineUtils.getEarliestInstantForMetadataArchival(
              dataMetaClient.getActiveTimeline(), config.shouldArchiveBeyondSavepoint());

      // Do not archive the instants after the earliest commit (COMMIT, DELTA_COMMIT, and
      // REPLACE_COMMIT only, considering non-savepoint commit only if enabling archive
      // beyond savepoint) and the earliest inflight instant (all actions).
      // This is required by metadata table, see HoodieTableMetadataUtil#processRollbackMetadata
      // for details.
      // Note that we cannot blindly use the earliest instant of all actions, because CLEAN and
      // ROLLBACK instants are archived separately apart from commits (check
      // HoodieTimelineArchiver#getCleanInstantsToArchive).  If we do so, a very old completed
      // CLEAN or ROLLBACK instant can block the archive of metadata table timeline and causes
      // the active timeline of metadata table to be extremely long, leading to performance issues
      // for loading the timeline.
      if (qualifiedEarliestInstant.isPresent()) {
        instants = instants.filter(instant ->
            compareTimestamps(
                instant.getTimestamp(),
                HoodieTimeline.LESSER_THAN,
                qualifiedEarliestInstant.get().getTimestamp()));
      }
    }

    return instants.flatMap(hoodieInstant -> {
      List<HoodieInstant> instantsToStream = groupByTsAction.get(Pair.of(hoodieInstant.getTimestamp(),
          HoodieInstant.getComparableAction(hoodieInstant.getAction())));
      if (instantsToStream != null) {
        return instantsToStream.stream();
      } else {
        // if a concurrent writer archived the instant
        return Stream.empty();
      }
    });
  }

  private boolean deleteArchivedInstants(List<HoodieInstant> archivedInstants, HoodieEngineContext context) throws IOException {
    LOG.info("Deleting instants " + archivedInstants);

    List<HoodieInstant> pendingInstants = new ArrayList<>();
    List<HoodieInstant> completedInstants = new ArrayList<>();

    for (HoodieInstant instant : archivedInstants) {
      if (instant.isCompleted()) {
        completedInstants.add(instant);
      } else {
        pendingInstants.add(instant);
      }
    }

    context.setJobStatus(this.getClass().getSimpleName(), "Delete archived instants: " + config.getTableName());
    // Delete the metadata files
    // in HoodieInstant.State sequence: requested -> inflight -> completed,
    // this is important because when a COMPLETED metadata file is removed first,
    // other monitors on the timeline(such as the compaction or clustering services) would
    // mistakenly recognize the pending file as a pending operation,
    // then all kinds of weird bugs occur.
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    if (!pendingInstants.isEmpty()) {
      context.foreach(
          pendingInstants,
          instant -> activeTimeline.deleteInstantFileIfExists(instant),
          Math.min(pendingInstants.size(), config.getArchiveDeleteParallelism())
      );
    }
    if (!completedInstants.isEmpty()) {
      // Due to the concurrency between deleting completed instants and reading data,
      // there may be hole in the timeline, which can lead to errors when reading data.
      // Therefore, the concurrency of deleting completed instants is temporarily disabled,
      // and instants are deleted in ascending order to prevent the occurrence of such holes.
      // See HUDI-7207 and #10325.
      completedInstants.stream()
          .forEach(instant -> activeTimeline.deleteInstantFileIfExists(instant));
    }

    return true;
  }

  public void archive(HoodieEngineContext context, List<HoodieInstant> instants) throws HoodieCommitException {
    try {
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      LOG.info("Wrapper schema " + wrapperSchema.toString());
      List<IndexedRecord> records = new ArrayList<>();
      for (HoodieInstant hoodieInstant : instants) {
        try {
          deleteAnyLeftOverMarkers(context, hoodieInstant);
          records.add(convertToAvroRecord(hoodieInstant));
          if (records.size() >= this.config.getCommitArchivalBatchSize()) {
            writeToFile(wrapperSchema, records);
          }
        } catch (Exception e) {
          LOG.error("Failed to archive commits, .commit file: " + hoodieInstant.getFileName(), e);
          if (this.config.isFailOnTimelineArchivingEnabled()) {
            throw e;
          }
        }
      }
      writeToFile(wrapperSchema, records);
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to archive commits", e);
    }
  }

  private void deleteAnyLeftOverMarkers(HoodieEngineContext context, HoodieInstant instant) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instant.getTimestamp());
    if (writeMarkers.deleteMarkerDir(context, config.getMarkersDeleteParallelism())) {
      LOG.info("Cleaned up left over marker directory for instant :" + instant);
    }
  }

  private void writeToFile(Schema wrapperSchema, List<IndexedRecord> records) throws Exception {
    if (records.size() > 0) {
      Map<HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, wrapperSchema.toString());
      final String keyField = table.getMetaClient().getTableConfig().getRecordKeyFieldProp();
      List<HoodieRecord> indexRecords = records.stream().map(HoodieAvroIndexedRecord::new).collect(Collectors.toList());
      HoodieAvroDataBlock block = new HoodieAvroDataBlock(indexRecords, header, keyField);
      writer.appendBlock(block);
      records.clear();
    }
  }

  private IndexedRecord convertToAvroRecord(HoodieInstant hoodieInstant)
      throws IOException {
    return MetadataConversionUtils.createMetaWrapper(hoodieInstant, metaClient);
  }
}
