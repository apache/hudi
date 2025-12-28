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

package org.apache.hudi.client.timeline.versioning.v2;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.NativeTableFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.utils.ArchivalUtils.getMinAndMaxInstantsToKeep;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Archiver to bound the growth of files under .hoodie meta path.
 */
@Slf4j
public class TimelineArchiverV2<T extends HoodieAvroPayload, I, K, O> implements HoodieTimelineArchiver<T, I, K, O> {

  private final HoodieWriteConfig config;
  private final int maxInstantsToKeep;
  private final int minInstantsToKeep;
  private final HoodieTable<T, I, K, O> table;
  private final HoodieTableMetaClient metaClient;
  private final TransactionManager txnManager;

  private final LSMTimelineWriter timelineWriter;

  public TimelineArchiverV2(HoodieWriteConfig config, HoodieTable<T, I, K, O> table) {
    this.config = config;
    this.table = table;
    this.metaClient = table.getMetaClient();
    this.txnManager = new TransactionManager(config, table.getStorage());
    this.timelineWriter = LSMTimelineWriter.getInstance(config, table);
    Pair<Integer, Integer> minAndMaxInstants = getMinAndMaxInstantsToKeep(table, metaClient);
    this.minInstantsToKeep = minAndMaxInstants.getLeft();
    this.maxInstantsToKeep = minAndMaxInstants.getRight();
  }

  @Override
  public int archiveIfRequired(HoodieEngineContext context, boolean acquireLock) throws IOException {
    try {
      if (acquireLock) {
        // there is no owner or instant time per se for archival.
        txnManager.beginStateChange(Option.empty(), Option.empty());
      }
    } catch (HoodieLockException e) {
      log.error("Fail to begin transaction", e);
      return 0;
    }

    try {
      // Sort again because the cleaning and rollback instants could break the sequence.
      List<ActiveAction> instantsToArchive = getInstantsToArchive().sorted().collect(Collectors.toList());
      if (!instantsToArchive.isEmpty()) {
        log.info("Archiving and deleting instants {}", instantsToArchive);
        Consumer<Exception> exceptionHandler = e -> {
          if (this.config.isFailOnTimelineArchivingEnabled()) {
            throw new HoodieException(e);
          }
        };
        this.timelineWriter.write(instantsToArchive, Option.of(action -> deleteAnyLeftOverMarkers(context, action)), Option.of(exceptionHandler));
        log.debug("Deleting archived instants");
        deleteArchivedActions(instantsToArchive, context);
        // triggers compaction and cleaning only after archiving action
        this.timelineWriter.compactAndClean(context);
        Supplier<List<HoodieInstant>> archivedInstants = () -> instantsToArchive.stream()
            .flatMap(action -> Stream.concat(action.getCompletedInstants().stream(), action.getPendingInstants().stream()))
            .collect(Collectors.toList());
        // Call Table Format archive to allow archiving in table format.
        table.getMetaClient().getTableFormat().archive(archivedInstants, table.getContext(), table.getMetaClient(), table.getViewManager());
      } else {
        log.info("No Instants to archive");
      }
      return instantsToArchive.size();
    } finally {
      if (acquireLock) {
        txnManager.endStateChange(Option.empty());
      }
    }
  }

  private List<HoodieInstant> getCleanAndRollbackInstantsToArchive(HoodieInstant latestCommitInstantToArchive) {
    HoodieTimeline cleanAndRollbackTimeline = table.getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION, HoodieTimeline.ROLLBACK_ACTION))
        .filterCompletedInstants();

    // Since the commit instants to archive is continuous, we can use the latest commit instant to archive as the
    // right boundary to collect the clean or rollback instants to archive.
    //
    //                                                  latestCommitInstantToArchive
    //                                                               v
    //  | commit1 clean1 commit2 commit3 clean2 commit4 rollback1 commit5 | commit6 clean3 commit7 ...
    //  | <------------------  instants to archive  --------------------> |
    //
    //  CommitInstantsToArchive: commit1, commit2, commit3, commit4, commit5
    //  CleanAndRollbackInstantsToArchive: clean1, clean2, rollback1

    return cleanAndRollbackTimeline.getInstantsAsStream()
        .filter(s -> compareTimestamps(s.requestedTime(), LESSER_THAN, latestCommitInstantToArchive.requestedTime()))
        .collect(Collectors.toList());
  }

  private List<HoodieInstant> getCommitInstantsToArchive() throws IOException {
    HoodieTimeline completedCommitsTimeline = table.getCompletedCommitsTimeline();

    if (completedCommitsTimeline.countInstants() <= maxInstantsToKeep) {
      return Collections.emptyList();
    }

    // Step1: Get all candidates of earliestInstantToRetain.
    List<Option<HoodieInstant>> earliestInstantToRetainCandidates = new ArrayList<>();

    // 1. Earliest commit to retain is the greatest completed commit, that is less than the earliest pending instant.
    // In some cases when inflight is the lowest commit then earliest commit to retain will be equal to the earliest
    // inflight commit.
    Option<HoodieInstant> earliestPendingInstant = table.getActiveTimeline()
        .getWriteTimeline()
        .filter(instant -> !instant.isCompleted())
        .firstInstant();

    Option<HoodieInstant> earliestCommitToRetain;
    if (earliestPendingInstant.isPresent()) {
      Option<HoodieInstant> completedCommitBeforeEarliestPendingInstant = Option.fromJavaOptional(completedCommitsTimeline
          .filter(instant -> compareTimestamps(instant.requestedTime(), LESSER_THAN, earliestPendingInstant.get().requestedTime()))
          .getReverseOrderedInstants().findFirst());
      // Check if the completed instant is higher than the earliest inflight instant
      // in that case update the earliestCommitToRetain to earliestInflight commit time.
      if (!completedCommitBeforeEarliestPendingInstant.isPresent()) {
        earliestCommitToRetain = earliestPendingInstant;
      } else {
        earliestCommitToRetain = completedCommitBeforeEarliestPendingInstant;
      }
    } else {
      earliestCommitToRetain = Option.empty();
    }
    earliestInstantToRetainCandidates.add(earliestCommitToRetain);

    // 2. For Merge-On-Read table, inline or async compaction is enabled
    // We need to make sure that there are enough delta commits in the active timeline
    // to trigger compaction scheduling, when the trigger strategy of compaction is
    // NUM_COMMITS or NUM_AND_TIME.
    Option<HoodieInstant> earliestInstantToRetainForCompaction =
        (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ
            && (config.getInlineCompactTriggerStrategy() == CompactionTriggerStrategy.NUM_COMMITS
            || config.getInlineCompactTriggerStrategy() == CompactionTriggerStrategy.NUM_AND_TIME))
            ? CompactionUtils.getEarliestInstantToRetainForCompaction(
            table.getActiveTimeline(), config.getInlineCompactDeltaCommitMax())
            : Option.empty();
    earliestInstantToRetainCandidates.add(earliestInstantToRetainForCompaction);

    // 3. The clustering commit instant can not be archived unless we ensure that the replaced files have been cleaned,
    // without the replaced files metadata on the timeline, the fs view would expose duplicates for readers.
    // Meanwhile, when inline or async clustering is enabled, we need to ensure that there is a commit in the active timeline
    // to check whether the file slice generated in pending clustering after archive isn't committed.
    Option<HoodieInstant> earliestInstantToRetainForClustering =
        ClusteringUtils.getEarliestInstantToRetainForClustering(table.getActiveTimeline(), table.getMetaClient(),
            config.getCleanerPolicy());
    earliestInstantToRetainCandidates.add(earliestInstantToRetainForClustering);

    // 4. If metadata table is enabled, do not archive instants which are more recent than the last compaction on the
    // metadata table.
    if (config.isMetadataTableEnabled()
        && table.getMetaClient().getTableConfig().isMetadataTableAvailable()
        && table.getMetaClient().getTableFormat().getName().equals(NativeTableFormat.TABLE_FORMAT)) {
      try (HoodieTableMetadata tableMetadata = table.refreshAndGetTableMetadata()) {
        Option<String> latestCompactionTime = tableMetadata.getLatestCompactionTime();
        if (!latestCompactionTime.isPresent()) {
          log.info("Not archiving as there is no compaction yet on the metadata table");
          return Collections.emptyList();
        } else {
          log.info("Limiting archiving of instants to latest compaction on metadata table at {}", latestCompactionTime.get());
          earliestInstantToRetainCandidates.add(
              completedCommitsTimeline.findInstantsModifiedAfterByCompletionTime(latestCompactionTime.get()).firstInstant());
        }
      } catch (Exception e) {
        throw new HoodieException("Error limiting instant archival based on metadata table", e);
      }
    }

    // 5. If this is a metadata table, do not archive the commits that live in data set
    // active timeline. This is required by metadata table,
    // see HoodieTableMetadataUtil#processRollbackMetadata for details.
    if (table.isMetadataTable()) {
      HoodieTableMetaClient dataMetaClient = HoodieTableMetaClient.builder()
          .setBasePath(HoodieTableMetadata.getDatasetBasePath(config.getBasePath()))
          .setConf(metaClient.getStorageConf().newInstance())
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
      // TimelineArchiverV2#getCleanInstantsToArchive).  If we do so, a very old completed
      // CLEAN or ROLLBACK instant can block the archive of metadata table timeline and causes
      // the active timeline of metadata table to be extremely long, leading to performance issues
      // for loading the timeline.
      earliestInstantToRetainCandidates.add(qualifiedEarliestInstant);
    }

    // 6. If archival should consider `earliest retain instant` in the clean plan,
    // we should add the earliest retain instant from the clean plan to the candidates.
    if (config.shouldArchiveKeepCleanPlanRetainInstant()) {
      Option<HoodieInstant> latestCleanInstantOpt = table.getActiveTimeline().getCleanerTimeline().lastInstant();
      if (latestCleanInstantOpt.isPresent()) {
        HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(metaClient, latestCleanInstantOpt.get());
        Option<String> earliestInstantTimeToRetain = Option.ofNullable(cleanerPlan.getEarliestInstantToRetain()).map(HoodieActionInstant::getTimestamp);
        if (earliestInstantTimeToRetain.isPresent() && !StringUtils.isNullOrEmpty(earliestInstantTimeToRetain.get())) {
          HoodieActionInstant earliestInstantToRetainForCleaning = cleanerPlan.getEarliestInstantToRetain();
          earliestInstantToRetainCandidates.add(
                  Option.of(new HoodieInstant(
                      HoodieInstant.State.valueOf(earliestInstantToRetainForCleaning.getState()),
                      earliestInstantToRetainForCleaning.getAction(),
                      earliestInstantToRetainForCleaning.getTimestamp(),
                      InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)));
        }
      }
    }

    // Choose the instant in earliestInstantToRetainCandidates with the smallest
    // timestamp as earliestInstantToRetain.
    //TODO: HARDCODED TIMELINE OBJECT
    java.util.Optional<HoodieInstant> earliestInstantToRetain = earliestInstantToRetainCandidates
        .stream()
        .filter(Option::isPresent)
        .map(Option::get)
        .min(InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);

    // Step2: We cannot archive any commits which are made after the first savepoint present,
    // unless HoodieArchivalConfig#ARCHIVE_BEYOND_SAVEPOINT is enabled.
    Option<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
    Set<String> savepointTimestamps = table.getSavepointTimestamps();

    Stream<HoodieInstant> instantToArchiveStream = completedCommitsTimeline.getInstantsAsStream()
        .filter(s -> {
          if (config.shouldArchiveBeyondSavepoint()) {
            // skip savepoint commits and proceed further
            return !savepointTimestamps.contains(s.requestedTime());
          } else {
            // if no savepoint present, then don't filter
            // stop at first savepoint commit
            return !firstSavepoint.isPresent() || compareTimestamps(s.requestedTime(), LESSER_THAN, firstSavepoint.get().requestedTime());
          }
        }).filter(s -> earliestInstantToRetain
            .map(instant -> compareTimestamps(s.requestedTime(), LESSER_THAN, instant.requestedTime()))
            .orElse(true));
    return instantToArchiveStream.limit(completedCommitsTimeline.countInstants() - minInstantsToKeep)
        .collect(Collectors.toList());
  }

  private Stream<ActiveAction> getInstantsToArchive() throws IOException {
    if (config.isMetaserverEnabled()) {
      return Stream.empty();
    }

    // First get commit instants to archive.
    List<HoodieInstant> instantsToArchive = getCommitInstantsToArchive();
    if (!instantsToArchive.isEmpty()) {
      HoodieInstant latestCommitInstantToArchive = instantsToArchive.get(instantsToArchive.size() - 1);
      // Then get clean and rollback instants to archive.
      List<HoodieInstant> cleanAndRollbackInstantsToArchive =
          getCleanAndRollbackInstantsToArchive(latestCommitInstantToArchive);
      instantsToArchive.addAll(cleanAndRollbackInstantsToArchive);
      instantsToArchive.sort(InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    }

    if (instantsToArchive.isEmpty()) {
      return Stream.empty();
    }

    // For archive, we need to include instant's all states.
    // The re-instantiation of the timeline may result in inconsistencies with the existing meta client active timeline,
    // When there is no lock guard of the archiving process, the 'raw' timeline could contain less distinct instants
    // because of the metadata file removing from another archiving process.
    //TODO: HARDCODED TIMELINE OBJECT
    HoodieActiveTimeline rawActiveTimeline = new ActiveTimelineV2(metaClient, false);
    Map<Pair<String, String>, List<HoodieInstant>> groupByTsAction = rawActiveTimeline.getInstantsAsStream()
        .collect(Collectors.groupingBy(i -> Pair.of(i.requestedTime(),
            InstantComparatorV2.getComparableAction(i.getAction()))));

    return instantsToArchive.stream().flatMap(hoodieInstant -> {
      List<HoodieInstant> instantsToStream = groupByTsAction.get(Pair.of(hoodieInstant.requestedTime(),
          InstantComparatorV2.getComparableAction(hoodieInstant.getAction())));
      if (instantsToStream != null) {
        return Stream.of(ActiveAction.fromInstants(instantsToStream));
      } else {
        // if a concurrent writer archived the instant
        return Stream.empty();
      }
    });
  }

  private boolean deleteArchivedActions(List<ActiveAction> activeActions, HoodieEngineContext context) {
    List<HoodieInstant> pendingInstants = new ArrayList<>();
    List<HoodieInstant> completedInstants = new ArrayList<>();

    for (ActiveAction activeAction : activeActions) {
      completedInstants.addAll(activeAction.getCompletedInstants());
      pendingInstants.addAll(activeAction.getPendingInstants());
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

  private void deleteAnyLeftOverMarkers(HoodieEngineContext context, ActiveAction activeAction) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, activeAction.getInstantTime());
    if (writeMarkers.deleteMarkerDir(context, config.getMarkersDeleteParallelism())) {
      log.info("Cleaned up left over marker directory for instant: {}", activeAction);
    }
  }
}
