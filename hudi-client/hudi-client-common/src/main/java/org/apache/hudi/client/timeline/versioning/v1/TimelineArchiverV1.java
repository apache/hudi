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

package org.apache.hudi.client.timeline.versioning.v1;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.ArchivedTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantFileNameGeneratorV1;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.utils.ArchivalUtils.getMinAndMaxInstantsToKeep;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Archiver to bound the growth of files under .hoodie meta path.
 */
@Slf4j
public class TimelineArchiverV1<T extends HoodieAvroPayload, I, K, O> implements HoodieTimelineArchiver<T, I, K, O> {

  public static final String ARCHIVE_LIMIT_INSTANTS = "hoodie.archive.limit.instants";

  private final StoragePath archiveFilePath;
  private final HoodieWriteConfig config;
  private Writer writer;
  private final int maxInstantsToKeep;
  private final int minInstantsToKeep;
  private final HoodieTable<T, I, K, O> table;
  private final HoodieTableMetaClient metaClient;
  private final TransactionManager txnManager;

  public TimelineArchiverV1(HoodieWriteConfig config, HoodieTable<T, I, K, O> table) {
    this.config = config;
    this.table = table;
    this.metaClient = table.getMetaClient();
    this.archiveFilePath = ArchivedTimelineV1.getArchiveLogPath(metaClient.getArchivePath());
    this.txnManager = new TransactionManager(config, table.getMetaClient().getStorage());
    Pair<Integer, Integer> minAndMaxInstants = getMinAndMaxInstantsToKeep(table, metaClient);
    this.minInstantsToKeep = minAndMaxInstants.getLeft();
    this.maxInstantsToKeep = minAndMaxInstants.getRight();
  }

  private Writer openWriter(StoragePath archivePath) {
    try {
      if (this.writer == null) {
        return HoodieLogFormat.newWriterBuilder().onParentPath(archivePath).withInstantTime("")
            .withFileId(archiveFilePath.getName()).withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
            .withStorage(metaClient.getStorage()).build();
      } else {
        return this.writer;
      }
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

  @Override
  public int archiveIfRequired(HoodieEngineContext context, boolean acquireLock) throws IOException {
    //NOTE:  We permanently disable merging archive files. This is different from 0.15 behavior.
    try {
      if (acquireLock) {
        // there is no owner or instant time per se for archival.
        txnManager.beginStateChange(Option.empty(), Option.empty());
      }
      List<HoodieInstant> instantsToArchive = getInstantsToArchive();
      if (!instantsToArchive.isEmpty()) {
        this.writer = openWriter(archiveFilePath.getParent());
        log.info("Archiving instants {} for table {}", instantsToArchive, config.getBasePath());
        archive(context, instantsToArchive);
        log.info("Deleting archived instants {} for table {}", instantsToArchive, config.getBasePath());
        deleteArchivedInstants(instantsToArchive, context);
      } else {
        log.info("No Instants to archive for table {}", config.getBasePath());
      }

      return instantsToArchive.size();
    } finally {
      close();
      if (acquireLock) {
        txnManager.endStateChange(Option.empty());
      }
    }
  }

  /**
   * Keeping for downgrade from 1.x LSM archived timeline.
   */
  public void flushArchiveEntries(List<IndexedRecord> archiveRecords, StoragePath archivePath) throws HoodieCommitException {
    try {
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      this.writer = openWriter(archivePath);
      writeToFile(wrapperSchema, archiveRecords);
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to archive commits", e);
    } finally {
      close();
    }
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
              .filter(instant -> compareTimestamps(instant.requestedTime(),
                  LESSER_THAN, oldestPendingInstant.get().requestedTime())).findFirst());
      // Check if the completed instant is higher than the oldest inflight instant
      // in that case update the oldestCommitToRetain to oldestInflight commit time.
      if (!completedCommitBeforeOldestPendingInstant.isPresent()
          || compareTimestamps(oldestPendingInstant.get().requestedTime(),
          LESSER_THAN, completedCommitBeforeOldestPendingInstant.get().requestedTime())) {
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
              ? CompactionUtils.getEarliestInstantToRetainForCompaction(
              table.getActiveTimeline(), config.getInlineCompactDeltaCommitMax())
              : Option.empty();

      // The clustering commit instant can not be archived unless we ensure that the replaced files have been cleaned,
      // without the replaced files metadata on the timeline, the fs view would expose duplicates for readers.
      // Meanwhile, when inline or async clustering is enabled, we need to ensure that there is a commit in the active timeline
      // to check whether the file slice generated in pending clustering after archive isn't committed.
      Option<HoodieInstant> oldestInstantToRetainForClustering =
          ClusteringUtils.getEarliestInstantToRetainForClustering(table.getActiveTimeline(), table.getMetaClient(), config.getCleanerPolicy());

      // Actually do the commits
      Stream<HoodieInstant> instantToArchiveStream = commitTimeline.getInstantsAsStream()
          .filter(s -> {
            if (config.shouldArchiveBeyondSavepoint()) {
              // skip savepoint commits and proceed further
              return !savepointTimestamps.contains(s.requestedTime());
            } else {
              // if no savepoint present, then don't filter
              // stop at first savepoint commit
              return !(firstSavepoint.isPresent() && compareTimestamps(firstSavepoint.get().requestedTime(), LESSER_THAN_OR_EQUALS, s.requestedTime()));
            }
          }).filter(s -> {
            // oldestCommitToRetain is the highest completed commit instant that is less than the oldest inflight instant.
            // By filtering out any commit >= oldestCommitToRetain, we can ensure there are no gaps in the timeline
            // when inflight commits are present.
            return oldestCommitToRetain
                .map(instant -> compareTimestamps(instant.requestedTime(), GREATER_THAN, s.requestedTime()))
                .orElse(true);
          }).filter(s ->
              oldestInstantToRetainForCompaction.map(instantToRetain ->
                      compareTimestamps(s.requestedTime(), LESSER_THAN, instantToRetain.requestedTime()))
                  .orElse(true)
          ).filter(s ->
              oldestInstantToRetainForClustering.map(instantToRetain ->
                      compareTimestamps(s.requestedTime(), LESSER_THAN, instantToRetain.requestedTime()))
                  .orElse(true)
          );
      return instantToArchiveStream.limit(commitTimeline.countInstants() - minInstantsToKeep);
    } else {
      return Stream.empty();
    }
  }

  private List<HoodieInstant> getInstantsToArchive() throws IOException {
    if (config.isMetaserverEnabled()) {
      return Collections.emptyList();
    }

    List<HoodieInstant> candidates = Stream.concat(getCleanInstantsToArchive(), getCommitInstantsToArchive()).collect(Collectors.toList());
    if (candidates.isEmpty()) {
      // exit early to avoid loading meta client for metadata table
      return Collections.emptyList();
    }

    Stream<HoodieInstant> instants = candidates.stream();

    // If metadata table is enabled, do not archive instants which are more recent than the last compaction on the
    // metadata table.
    if (config.isMetadataTableEnabled() && table.getMetaClient().getTableConfig().isMetadataTableAvailable()) {
      try (HoodieTableMetadata tableMetadata = table.refreshAndGetTableMetadata()) {
        Option<String> latestCompactionTime = tableMetadata.getLatestCompactionTime();
        if (!latestCompactionTime.isPresent()) {
          log.info("Not archiving as there is no compaction yet on the metadata table");
          instants = Stream.empty();
        } else {
          log.info("Limiting archiving of instants to latest compaction on metadata table at " + latestCompactionTime.get());
          instants = instants.filter(instant -> compareTimestamps(instant.requestedTime(), LESSER_THAN,
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
      // TimelineArchiverV1#getCleanInstantsToArchive).  If we do so, a very old completed
      // CLEAN or ROLLBACK instant can block the archive of metadata table timeline and causes
      // the active timeline of metadata table to be extremely long, leading to performance issues
      // for loading the timeline.
      if (qualifiedEarliestInstant.isPresent()) {
        instants = instants.filter(instant ->
            compareTimestamps(
                instant.requestedTime(),
                LESSER_THAN,
                qualifiedEarliestInstant.get().requestedTime()));
      }
    }

    List<HoodieInstant> instantsToArchive = instants.collect(Collectors.toList());
    if (instantsToArchive.isEmpty()) {
      // Exit early to avoid loading raw timeline
      return Collections.emptyList();
    }

    // For archiving and cleaning instants, we need to include intermediate state files if they exist
    HoodieActiveTimeline rawActiveTimeline = new ActiveTimelineV1(metaClient, false);
    Map<Pair<String, String>, List<HoodieInstant>> groupByTsAction = rawActiveTimeline.getInstantsAsStream()
        .collect(Collectors.groupingBy(i -> Pair.of(i.requestedTime(),
            InstantComparatorV1.getComparableAction(i.getAction()))));

    return instantsToArchive.stream()
        .sorted()
        .limit(config.getProps().getLong(ARCHIVE_LIMIT_INSTANTS, Long.MAX_VALUE))
        .flatMap(hoodieInstant ->
            groupByTsAction.getOrDefault(Pair.of(hoodieInstant.requestedTime(),
                InstantComparatorV1.getComparableAction(hoodieInstant.getAction())), Collections.emptyList()).stream())
        .collect(Collectors.toList());
  }

  private boolean deleteArchivedInstants(List<HoodieInstant> archivedInstants, HoodieEngineContext context) throws IOException {
    log.info("Deleting instants " + archivedInstants);

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
      completedInstants.stream().forEach(activeTimeline::deleteInstantFileIfExists);
    }
    // Call Table Format archive to allow archiving in table format.
    table.getMetaClient().getTableFormat().archive(() -> archivedInstants, table.getContext(), table.getMetaClient(), table.getViewManager());
    return true;
  }

  public void archive(HoodieEngineContext context, List<HoodieInstant> instants) throws HoodieCommitException {
    try {
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      log.info("Wrapper schema " + wrapperSchema.toString());
      List<IndexedRecord> records = new ArrayList<>();
      for (HoodieInstant hoodieInstant : instants) {
        try {
          deleteAnyLeftOverMarkers(context, hoodieInstant);
          records.add(convertToAvroRecord(hoodieInstant));
          if (records.size() >= this.config.getCommitArchivalBatchSize()) {
            writeToFile(wrapperSchema, records);
          }
        } catch (Exception e) {
          InstantFileNameGenerator fileNameFactory = new InstantFileNameGeneratorV1();
          log.error("Failed to archive commits, .commit file: " + fileNameFactory.getFileName(hoodieInstant), e);
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
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instant.requestedTime());
    if (writeMarkers.deleteMarkerDir(context, config.getMarkersDeleteParallelism())) {
      log.info("Cleaned up left over marker directory for instant :" + instant);
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
