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
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
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
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
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
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * Archiver to bound the growth of files under .hoodie meta path.
 */
public class HoodieTimelineArchiver<T extends HoodieAvroPayload, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieTimelineArchiver.class);

  private final Path archiveFilePath;
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
    this.maxInstantsToKeep = config.getMaxCommitsToKeep();
    this.minInstantsToKeep = config.getMinCommitsToKeep();
    this.txnManager = new TransactionManager(config, table.getMetaClient().getFs());
  }

  private Writer openWriter() {
    try {
      if (this.writer == null) {
        return HoodieLogFormat.newWriterBuilder().onParentPath(archiveFilePath.getParent())
            .withFileId(archiveFilePath.getName()).withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
            .withFs(metaClient.getFs()).overBaseCommit("").build();
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
    return config.getArchiveMergeEnable() && !StorageSchemes.isAppendSupported(metaClient.getFs().getScheme());
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
    Path planPath = new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME);
    // Flush remained content if existed and open a new write
    reOpenWriter();
    // List all archive files
    FileStatus[] fsStatuses = metaClient.getFs().globStatus(
        new Path(metaClient.getArchivePath() + "/.commits_.archive*"));
    // Sort files by version suffix in reverse (implies reverse chronological order)
    Arrays.sort(fsStatuses, new HoodieArchivedTimeline.ArchiveFileVersionComparator());

    int archiveMergeFilesBatchSize = config.getArchiveMergeFilesBatchSize();
    long smallFileLimitBytes = config.getArchiveMergeSmallFileLimitBytes();

    List<FileStatus> mergeCandidate = getMergeCandidates(smallFileLimitBytes, fsStatuses);

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
      metaClient.getFs().delete(planPath, false);
      LOG.info("Success to merge small archive files.");
    }
  }

  /**
   * Find the latest 'huge archive file' index as a break point and only check/merge newer archive files.
   * Because we need to keep the original order of archive files which is important when loading archived instants with time filter.
   * {@link HoodieArchivedTimeline} loadInstants(TimeRangeFilter filter, boolean loadInstantDetails, Function<GenericRecord, Boolean> commitsFilter)
   *
   * @param smallFileLimitBytes small File Limit Bytes
   * @param fsStatuses          Sort by version suffix in reverse
   * @return merge candidates
   */
  private List<FileStatus> getMergeCandidates(long smallFileLimitBytes, FileStatus[] fsStatuses) {
    int index = 0;
    for (; index < fsStatuses.length; index++) {
      if (fsStatuses[index].getLen() > smallFileLimitBytes) {
        break;
      }
    }
    return Arrays.stream(fsStatuses).limit(index).collect(Collectors.toList());
  }

  /**
   * Get final written archive file name based on storageSchemes which does not support append.
   */
  private String computeLogFileName() throws IOException {
    String logWriteToken = writer.getLogFile().getLogWriteToken();
    HoodieLogFile hoodieLogFile = writer.getLogFile().rollOver(metaClient.getFs(), logWriteToken);
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
      Path planPath = new Path(metaClient.getArchivePath(), HoodieArchivedTimeline.MERGE_ARCHIVE_PLAN_NAME);
      HoodieWrapperFileSystem fs = metaClient.getFs();
      // If plan exist, last merge small archive files was failed.
      // we need to revert or complete last action.
      if (fs.exists(planPath)) {
        HoodieMergeArchiveFilePlan plan = null;
        try {
          plan = TimelineMetadataUtils.deserializeAvroMetadata(FileIOUtils.readDataFromPath(fs, planPath).get(), HoodieMergeArchiveFilePlan.class);
        } catch (IOException e) {
          LOG.warn("Parsing merge archive plan failed.", e);
          // Reading partial plan file which means last merge action is failed during writing plan file.
          fs.delete(planPath);
          return;
        }
        Path mergedArchiveFile = new Path(metaClient.getArchivePath(), plan.getMergedArchiveFileName());
        List<Path> candidates = plan.getCandidate().stream().map(Path::new).collect(Collectors.toList());
        if (candidateAllExists(candidates)) {
          // Last merge action is failed during writing merged archive file.
          // But all the small archive files are not deleted.
          // Revert last action by deleting mergedArchiveFile if existed.
          if (fs.exists(mergedArchiveFile)) {
            fs.delete(mergedArchiveFile, false);
          }
        } else {
          // Last merge action is failed during deleting small archive files.
          // But the merged files is completed.
          // Try to complete last action
          if (fs.exists(mergedArchiveFile)) {
            deleteFilesParallelize(metaClient, plan.getCandidate(), context, true);
          }
        }

        fs.delete(planPath);
      }
    }
  }

  /**
   * If all the candidate small archive files existed, last merge operation was failed during writing the merged archive file.
   * If at least one of candidate small archive files existed, the merged archive file was created and last operation was failed during deleting the small archive files.
   */
  private boolean candidateAllExists(List<Path> candidates) throws IOException {
    for (Path archiveFile : candidates) {
      if (!metaClient.getFs().exists(archiveFile)) {
        // candidate is deleted
        return false;
      }
    }
    return true;
  }

  public void buildArchiveMergePlan(List<String> compactCandidate, Path planPath, String compactedArchiveFileName) throws IOException {
    LOG.info("Start to build archive merge plan.");
    HoodieMergeArchiveFilePlan plan = HoodieMergeArchiveFilePlan.newBuilder()
        .setCandidate(compactCandidate)
        .setMergedArchiveFileName(compactedArchiveFileName)
        .build();
    Option<byte[]> content = TimelineMetadataUtils.serializeAvroMetadata(plan, HoodieMergeArchiveFilePlan.class);
    // building merge archive files plan.
    FileIOUtils.createFileInPath(metaClient.getFs(), planPath, content);
    LOG.info("Success to build archive merge plan");
  }

  public void mergeArchiveFiles(List<FileStatus> compactCandidate) throws IOException {
    LOG.info("Starting to merge small archive files.");
    Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
    try {
      List<IndexedRecord> records = new ArrayList<>();
      for (FileStatus fs : compactCandidate) {
        // Read the archived file
        try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient.getFs(),
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
        metaClient.getFs(),
        config.getArchiveDeleteParallelism(),
        pairOfSubPathAndConf -> {
          Path file = new Path(pairOfSubPathAndConf.getKey());
          try {
            FileSystem fs = metaClient.getFs();
            if (fs.exists(file)) {
              return fs.delete(file, false);
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

    Option<HoodieInstant> oldestPendingCompactionAndReplaceInstant = table.getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION))
        .filter(s -> !s.isCompleted())
        .firstInstant();

    Option<HoodieInstant> oldestInflightCommitInstant =
        table.getActiveTimeline()
            .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
            .filterInflights().firstInstant();

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
            // Ensure commits >= the oldest pending compaction/replace commit is retained
            return oldestPendingCompactionAndReplaceInstant
                .map(instant -> compareTimestamps(instant.getTimestamp(), GREATER_THAN, s.getTimestamp()))
                .orElse(true);
          }).filter(s -> {
            // We need this to ensure that when multiple writers are performing conflict resolution, eligible instants don't
            // get archived, i.e, instants after the oldestInflight are retained on the timeline
            if (config.getFailedWritesCleanPolicy() == HoodieFailedWritesCleaningPolicy.LAZY) {
              return oldestInflightCommitInstant.map(instant ->
                      compareTimestamps(instant.getTimestamp(), GREATER_THAN, s.getTimestamp()))
                  .orElse(true);
            }
            return true;
          }).filter(s ->
              oldestInstantToRetainForCompaction.map(instantToRetain ->
                      compareTimestamps(s.getTimestamp(), LESSER_THAN, instantToRetain.getTimestamp()))
                  .orElse(true)
          ).filter(s ->
              oldestInstantToRetainForClustering.map(instantToRetain ->
                      HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN, instantToRetain.getTimestamp()))
                  .orElse(true)
          );
      List<HoodieInstant> instantsToArchive = instantToArchiveStream.limit(commitTimeline.countInstants() - minInstantsToKeep).collect(Collectors.toList());
      // If cleaner is based on hours, lets ensure hudi does not archive commits yet to cleaned by the cleaner.
      if (config.getCleanerPolicy() == HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS && !instantsToArchive.isEmpty()) {
        String latestCommitToArchive = instantsToArchive.get(instantsToArchive.size() - 1).getTimestamp();
        try {
          Instant latestCommitInstant = HoodieActiveTimeline.parseDateFromInstantTime(commitTimeline.lastInstant().get().getTimestamp()).toInstant();
          ZonedDateTime currentDateTime = ZonedDateTime.ofInstant(latestCommitInstant, metaClient.getTableConfig().getTimelineTimezone().getZoneId());
          String earliestTimeToRetain = HoodieActiveTimeline.formatDate(Date.from(currentDateTime.minusHours(config.getCleanerHoursRetained()).toInstant()));
          if (HoodieTimeline.compareTimestamps(latestCommitToArchive, GREATER_THAN_OR_EQUALS, earliestTimeToRetain)) {
            throw new HoodieIOException("Please align your archival configs based on cleaner configs. 'hoodie.keep.min.commits' : "
                + config.getMinCommitsToKeep() + " + should be greater than "
                + " 'hoodie.cleaner.hours.retained' : " + config.getCleanerHoursRetained());
          }
        } catch (ParseException e) {
          throw new HoodieIOException("Failed to parse latest commit instant time " + commitTimeline.lastInstant().get().getTimestamp() + e.getMessage());
        }
      }
      return instantsToArchive.stream();
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
    if (table.getMetaClient().getTableConfig().isMetadataTableEnabled()) {
      try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(table.getContext(), config.getMetadataConfig(),
          config.getBasePath(), FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue())) {
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
          .setConf(metaClient.getHadoopConf())
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
      context.foreach(
          completedInstants,
          instant -> activeTimeline.deleteInstantFileIfExists(instant),
          Math.min(completedInstants.size(), config.getArchiveDeleteParallelism())
      );
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
          // in local FS and HDFS, there could be empty completed instants due to crash.
          if (table.getActiveTimeline().isEmpty(hoodieInstant) && hoodieInstant.isCompleted()) {
            // lets add an entry to the archival, even if not for the plan.
            records.add(createAvroRecordFromEmptyInstant(hoodieInstant));
          } else {
            records.add(convertToAvroRecord(hoodieInstant));
          }
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

  private IndexedRecord createAvroRecordFromEmptyInstant(HoodieInstant hoodieInstant) throws IOException {
    return MetadataConversionUtils.createMetaWrapperForEmptyInstant(hoodieInstant);
  }
}
