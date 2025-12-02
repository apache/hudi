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

package org.apache.hudi.common.table.timeline.versioning.v1;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.TimelineUtils.getInputStreamOptionLegacy;

public class ArchivedTimelineV1 extends BaseTimelineV1 implements HoodieArchivedTimeline, HoodieInstantReader {
  private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE_PREFIX = "commits";
  private static final String ACTION_TYPE_KEY = "actionType";
  private static final String ACTION_STATE = "actionState";
  private static final String STATE_TRANSITION_TIME = "stateTransitionTime";
  private HoodieTableMetaClient metaClient;
  // The first key is the timestamp -> multiple action types -> hoodie instant state and contents
  private final Map<String, Map<String, Map<HoodieInstant.State, byte[]>>> 
      readCommits = new HashMap<>();
  private final ArchivedTimelineLoaderV1 timelineLoader = new ArchivedTimelineLoaderV1();

  private static final Logger LOG = LoggerFactory.getLogger(org.apache.hudi.common.table.timeline.HoodieArchivedTimeline.class);

  /**
   * Loads all the archived instants.
   * Note that there is no lazy loading, so this may not work if the archived timeline range is really long.
   * TBD: Should we enforce maximum time range?
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    setInstants(this.loadInstants(false));
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
  }

  /**
   * Creates an archived timeline without loading any instants.
   * Instants can be loaded later using methods like loadCompletedInstantDetailsInMemory, loadCompactionDetailsInMemory, etc.
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, boolean shouldLoadInstants) {
    this.metaClient = metaClient;
    if (shouldLoadInstants) {
      setInstants(this.loadInstants(false));
    } else {
      setInstants(new ArrayList<>());
    }
  }

  private ArchivedTimelineV1(HoodieTableMetaClient metaClient, TimeRangeFilter timeRangeFilter) {
    this(metaClient, timeRangeFilter, null, Option.of(HoodieInstant.State.COMPLETED));
  }

  /**
   * Loads instants satisfying the given time range filter and state. If state is Option.EMPTY, instants of all states are loaded
   */
  private ArchivedTimelineV1(HoodieTableMetaClient metaClient, TimeRangeFilter timeRangeFilter, Option<HoodieInstant.State> state) {
    this(metaClient, timeRangeFilter, null, state);
  }

  /**
   * Loads instants satisfying the given state, time range filter, and log file filter.
   * If state is Option.EMPTY, instants of all states are loaded.
   * Note that there is no lazy loading, so a wide time range may not work.
   */
  private ArchivedTimelineV1(HoodieTableMetaClient metaClient, TimeRangeFilter timeRangeFilter, LogFileFilter logFileFilter, Option<HoodieInstant.State> state) {
    this.metaClient = metaClient;
    Function<GenericRecord, Boolean> commitsFilter;
    if (state.isPresent()) {
      commitsFilter = record -> state.get().toString().equals(record.get(ACTION_STATE).toString());
    } else {
      commitsFilter = record -> true;
    }
    setInstants(loadInstants(timeRangeFilter, logFileFilter, true, commitsFilter));
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
  }

  /**
   * Loads completed instants from startTs(inclusive).
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, String startTs) {
    this(metaClient, new StartTsFilter(startTs));
  }

  /**
   * Loads completed instants from startTs(inclusive) to endTs(inclusive).
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, String startTs, String endTs) {
    this(metaClient, new ClosedClosedTimeRangeFilter(startTs, endTs));
  }

  /**
   * Loads instants of input state from startTs(inclusive) to endTs(inclusive).
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, String startTs, String endTs, Option<HoodieInstant.State> state) {
    this(metaClient, new ClosedClosedTimeRangeFilter(startTs, endTs), state);
  }

  /**
   * Load completed instants in archived log files
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, Set<String> logFiles) {
    this(metaClient, null, new LogFileFilter(logFiles), Option.of(HoodieInstant.State.COMPLETED));
  }

  /**
   * Load instants of input state in archived log files. If state is Option.EMPTY, instants of all states are loaded.
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, Set<String> logFiles, Option<HoodieInstant.State> state) {
    this(metaClient, null, new LogFileFilter(logFiles), state);
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public ArchivedTimelineV1() {
  }

  @Override
  public HoodieInstantReader getInstantReader() {
    return this;
  }

  /**
   * This method is only used when this object is deserialized in a spark executor.
   *
   * @deprecated
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  @Override
public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return Option.ofNullable(
        readCommits
            .getOrDefault(instant.requestedTime(), Collections.emptyMap())
            .getOrDefault(instant.getAction(), Collections.emptyMap())
            .get(instant.getState()));
  }

  @Override
  public InputStream getContentStream(HoodieInstant instant) {
    Option<InputStream> stream = getInputStreamOptionLegacy(this, instant);
    if (stream.isEmpty()) {
      return new ByteArrayInputStream(new byte[]{});
    }
    return stream.get();
  }

  public static StoragePath getArchiveLogPath(StoragePath archiveFolder) {
    return new StoragePath(archiveFolder, HOODIE_COMMIT_ARCHIVE_LOG_FILE_PREFIX);
  }

  @Override
  public void loadInstantDetailsInMemory(String startTs, String endTs) {
    loadInstants(startTs, endTs);
  }

  @Override
  public void loadCompletedInstantDetailsInMemory() {
    loadInstants(null, null, true,
        record -> {
          // Very old archived instants don't have action state set.
          Object action = record.get(ACTION_STATE);
          return action == null || org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED.toString().equals(action.toString());
        });
  }

  @Override
  public void loadCompactionDetailsInMemory(String compactionInstantTime) {
    loadCompactionDetailsInMemory(compactionInstantTime, compactionInstantTime);
  }

  @Override
  public void loadCompactionDetailsInMemory(String startTs, String endTs) {
    // load compactionPlan
    List<HoodieInstant> loadedInstants = loadInstants(new ClosedClosedTimeRangeFilter(startTs, endTs), null, true,
        record -> {
          // Older files don't have action state set.
          Object action = record.get(ACTION_STATE);
          return record.get(ACTION_TYPE_KEY).toString().equals(HoodieTimeline.COMPACTION_ACTION)
              && (action == null || org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT.toString().equals(action.toString()));
        });
    appendLoadedInstants(loadedInstants);
  }

  @Override
  public void loadCompactionDetailsInMemory(int limit) {
    loadAndCacheInstantsWithLimit(limit, true,
        record -> {
          Object actionState = record.get(ACTION_STATE);
          // Older files & archivedTimelineV2 don't have action state set.
          return record.get(ACTION_TYPE_KEY).toString().equals(HoodieTimeline.COMPACTION_ACTION)
              && (actionState == null || org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT.toString().equals(actionState.toString()));
        });
  }

  @Override
  public void loadCompletedInstantDetailsInMemory(String startTs, String endTs) {
    List<HoodieInstant> loadedInstants = loadInstants(new ClosedClosedTimeRangeFilter(startTs, endTs), null, true,
        record -> {
          Object actionState = record.get(ACTION_STATE);
          return actionState == null || org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED.toString().equals(actionState.toString());
        });
    appendLoadedInstants(loadedInstants);
  }

  @Override
  public void loadCompletedInstantDetailsInMemory(int limit) {
    loadAndCacheInstantsWithLimit(limit, true,
        record -> {
          Object actionState = record.get(ACTION_STATE);
          return actionState == null || org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED.toString().equals(actionState.toString());
        });
  }

  @Override
  public void clearInstantDetailsFromMemory(String instantTime) {
    this.readCommits.remove(instantTime);
  }

  @Override
  public void clearInstantDetailsFromMemory(String startTs, String endTs) {
    this.findInstantsInRange(startTs, endTs).getInstants().forEach(instant ->
        this.readCommits.remove(instant.requestedTime()));
  }

  private List<HoodieInstant> loadInstants(boolean loadInstantDetails) {
    return loadInstants(null, loadInstantDetails);
  }

  private List<HoodieInstant> loadInstants(String startTs, String endTs) {
    return loadInstants(new HoodieArchivedTimeline.TimeRangeFilter(startTs, endTs), true);
  }

  private List<HoodieInstant> loadInstants(HoodieArchivedTimeline.TimeRangeFilter filter, boolean loadInstantDetails) {
    return loadInstants(filter, null, loadInstantDetails, genericRecord -> true);
  }

  private List<HoodieInstant> loadInstants(HoodieArchivedTimeline.TimeRangeFilter filter, LogFileFilter logFileFilter, boolean loadInstantDetails, Function<GenericRecord, Boolean> commitsFilter) {
    InstantsLoader loader = new InstantsLoader(loadInstantDetails);
    timelineLoader.loadInstants(
        metaClient, filter, Option.ofNullable(logFileFilter), LoadMode.PLAN, commitsFilter, loader, Option.empty());
    return loader.getInstantsInRangeCollected().values()
        .stream().flatMap(Collection::stream).sorted().collect(Collectors.toList());
  }

  private void loadAndCacheInstantsWithLimit(int limit, boolean loadInstantDetails, Function<GenericRecord, Boolean> commitsFilter) {
    InstantsLoader loader = new InstantsLoader(loadInstantDetails);
    timelineLoader.loadInstants(
        metaClient, null, Option.empty(), LoadMode.PLAN, commitsFilter, loader, Option.of(limit));
    List<HoodieInstant> collectedInstants = loader.getInstantsInRangeCollected().values()
        .stream()
        .flatMap(Collection::stream)
        .sorted()
        .collect(Collectors.toList());
    appendLoadedInstants(collectedInstants);
  }

  /**
   * Callback to read instant details.
   */
  public class InstantsLoader implements BiConsumer<String, GenericRecord> {
    private final Map<String, List<HoodieInstant>> instantsInRange = new ConcurrentHashMap<>();
    private final boolean loadInstantDetails;

    private InstantsLoader(boolean loadInstantDetails) {
      this.loadInstantDetails = loadInstantDetails;
    }

    @Override
    public void accept(String instantTime, GenericRecord record) {
      Option<HoodieInstant> instant = readCommit(instantTime, record, loadInstantDetails, null);
      if (instant.isPresent()) {
        instantsInRange.computeIfAbsent(instant.get().requestedTime(), s -> new ArrayList<>())
            .add(instant.get());
      }
    }

    public Map<String, List<HoodieInstant>> getInstantsInRangeCollected() {
      return instantsInRange;
    }
  }

  private Option<HoodieInstant> readCommit(String instantTime, GenericRecord record, boolean loadDetails,
                                           TimeRangeFilter timeRangeFilter) {
    final String action = record.get(ACTION_TYPE_KEY).toString();
    final String stateTransitionTime = (String) record.get(STATE_TRANSITION_TIME);
    final HoodieInstant.State actionState = Option.ofNullable(record.get(ACTION_STATE)).map(state -> HoodieInstant.State.valueOf(state.toString())).orElse(HoodieInstant.State.COMPLETED);
    final HoodieInstant hoodieInstant = new HoodieInstant(actionState, action,
        instantTime, stateTransitionTime, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    if (timeRangeFilter != null && !timeRangeFilter.isInRange(hoodieInstant.requestedTime())) {
      return Option.empty();
    }
    if (loadDetails) {
      getMetadataKey(hoodieInstant).map(key -> {
        Object actionData = record.get(key);
        if (actionData != null) {
          this.readCommits.computeIfAbsent(instantTime, k -> new HashMap<>()).computeIfAbsent(action, a -> new HashMap<>());
          if (action.equals(HoodieTimeline.COMPACTION_ACTION)) {
            readCommits.get(instantTime).get(action).put(hoodieInstant.getState(), HoodieAvroUtils.avroToBytes((IndexedRecord) actionData));
          } else {
            readCommits.get(instantTime).get(action).put(hoodieInstant.getState(), actionData.toString().getBytes(StandardCharsets.UTF_8));
          }
        }
        return null;
      });
    }
    return Option.of(hoodieInstant);
  }

  @Nonnull
  private static Option<String> getMetadataKey(HoodieInstant instant) {
    switch (instant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION:
        return Option.of("hoodieCleanMetadata");
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
        return Option.of("hoodieCommitMetadata");
      case HoodieTimeline.ROLLBACK_ACTION:
        return Option.of("hoodieRollbackMetadata");
      case HoodieTimeline.SAVEPOINT_ACTION:
        return Option.of("hoodieSavePointMetadata");
      case HoodieTimeline.COMPACTION_ACTION:
      case HoodieTimeline.LOG_COMPACTION_ACTION:
        return Option.of("hoodieCompactionPlan");
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        if (instant.isRequested()) {
          return Option.of("hoodieRequestedReplaceMetadata");
        }
        return Option.of("hoodieReplaceCommitMetadata");
      case HoodieTimeline.INDEXING_ACTION:
        return Option.of("hoodieIndexCommitMetadata");
      default:
        LOG.error(String.format("Unknown action in metadata (%s)", instant.getAction()));
        return Option.empty();
    }
  }

  @Override
  public HoodieArchivedTimeline reload() {
    return new ArchivedTimelineV1(metaClient);
  }

  @Override
  public HoodieArchivedTimeline reload(String startTs) {
    return new ArchivedTimelineV1(metaClient, startTs);
  }

  @Override
  public boolean isEmpty(HoodieInstant instant) {
    return getInstantDetails(instant).isEmpty();
  }

  /**
   * A log file filter based on the full file paths
   */
  static class LogFileFilter {
    private final Set<String> logFiles;

    public LogFileFilter(Set<String> logFiles) {
      this.logFiles = logFiles;
    }

    public boolean shouldLoadFile(StoragePathInfo storagePathInfo) {
      return logFiles.contains(storagePathInfo.getPath().toString());
    }
  }
}
