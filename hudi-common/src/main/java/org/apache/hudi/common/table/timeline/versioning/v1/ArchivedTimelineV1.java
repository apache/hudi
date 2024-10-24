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
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class ArchivedTimelineV1 extends BaseTimelineV1 implements HoodieArchivedTimeline {
  private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE_PREFIX = "commits";
  private static final String ACTION_TYPE_KEY = "actionType";
  private static final String ACTION_STATE = "actionState";
  private static final String STATE_TRANSITION_TIME = "stateTransitionTime";
  private HoodieTableMetaClient metaClient;
  private final Map<String, byte[]> readCommits = new HashMap<>();
  private final ArchivedTimelineLoader timelineLoader = new ArchivedTimelineLoaderV1();
  private final InstantFactory instantFactory = new InstantFactoryV1();

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
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  /**
   * Loads completed instants from startTs(inclusive).
   * Note that there is no lazy loading, so this may not work if really early startTs is specified.
   */
  public ArchivedTimelineV1(HoodieTableMetaClient metaClient, String startTs) {
    this.metaClient = metaClient;
    setInstants(loadInstants(new StartTsFilter(startTs), true,
        record -> HoodieInstant.State.COMPLETED.toString().equals(record.get(ACTION_STATE).toString())));
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public ArchivedTimelineV1() {
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
    return Option.ofNullable(readCommits.get(instant.getRequestTime()));
  }

  public static StoragePath getArchiveLogPath(String archiveFolder) {
    return new StoragePath(archiveFolder, HOODIE_COMMIT_ARCHIVE_LOG_FILE_PREFIX);
  }

  @Override
  public void loadInstantDetailsInMemory(String startTs, String endTs) {
    loadInstants(startTs, endTs);
  }

  @Override
  public void loadCompletedInstantDetailsInMemory() {
    loadInstants(null, true,
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
    loadInstants(new HoodieArchivedTimeline.TimeRangeFilter(startTs, endTs), true,
        record -> {
          // Older files don't have action state set.
          Object action = record.get(ACTION_STATE);
          return record.get(ACTION_TYPE_KEY).toString().equals(HoodieTimeline.COMPACTION_ACTION)
              && (action == null || org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT.toString().equals(action.toString()));
        });
  }

  @Override
  public void clearInstantDetailsFromMemory(String instantTime) {
    this.readCommits.remove(instantTime);
  }

  @Override
  public void clearInstantDetailsFromMemory(String startTs, String endTs) {
    this.findInstantsInRange(startTs, endTs).getInstants().forEach(instant ->
        this.readCommits.remove(instant.getRequestTime()));
  }

  private List<HoodieInstant> loadInstants(boolean loadInstantDetails) {
    return loadInstants(null, loadInstantDetails);
  }

  private List<HoodieInstant> loadInstants(String startTs, String endTs) {
    return loadInstants(new HoodieArchivedTimeline.TimeRangeFilter(startTs, endTs), true);
  }

  private List<HoodieInstant> loadInstants(HoodieArchivedTimeline.TimeRangeFilter filter, boolean loadInstantDetails) {
    return loadInstants(filter, loadInstantDetails, genericRecord -> true);
  }

  private List<HoodieInstant> loadInstants(HoodieArchivedTimeline.TimeRangeFilter filter, boolean loadInstantDetails, Function<GenericRecord, Boolean> commitsFilter) {
    InstantsLoader loader = new InstantsLoader(loadInstantDetails);
    timelineLoader.loadInstants(metaClient, filter, LoadMode.PLAN, commitsFilter, loader);
    List<HoodieInstant> result = new ArrayList<>(loader.getInstantsInRangeCollected().values());
    Collections.sort(result);
    return result;
  }

  /**
   * Callback to read instant details.
   */
  private class InstantsLoader implements BiConsumer<String, GenericRecord> {
    private final Map<String, HoodieInstant> instantsInRange = new ConcurrentHashMap<>();
    private final boolean loadInstantDetails;

    private InstantsLoader(boolean loadInstantDetails) {
      this.loadInstantDetails = loadInstantDetails;
    }

    @Override
    public void accept(String instantTime, GenericRecord record) {
      HoodieInstant instant = readCommit(instantTime, record, loadInstantDetails);
      instantsInRange.putIfAbsent(instant.getRequestTime(), instant);
    }

    public Map<String, HoodieInstant> getInstantsInRangeCollected() {
      return instantsInRange;
    }
  }

  private HoodieInstant readCommit(String instantTime, GenericRecord record, boolean loadDetails) {
    final String action = record.get(ACTION_TYPE_KEY).toString();
    final String stateTransitionTime = (String) record.get(STATE_TRANSITION_TIME);
    if (loadDetails) {
      getMetadataKey(action).map(key -> {
        Object actionData = record.get(key);
        if (actionData != null) {
          if (action.equals(HoodieTimeline.COMPACTION_ACTION)) {
            this.readCommits.put(instantTime, HoodieAvroUtils.indexedRecordToBytes((IndexedRecord) actionData));
          } else {
            this.readCommits.put(instantTime, actionData.toString().getBytes(StandardCharsets.UTF_8));
          }
        }
        return null;
      });
    }
    return instantFactory.createNewInstant(HoodieInstant.State.valueOf(record.get(ACTION_STATE).toString()), action,
        instantTime, stateTransitionTime);
  }

  @Nonnull
  private Option<String> getMetadataKey(String action) {
    switch (action) {
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
        return Option.of("hoodieReplaceCommitMetadata");
      case HoodieTimeline.INDEXING_ACTION:
        return Option.of("hoodieIndexCommitMetadata");
      default:
        LOG.error(String.format("Unknown action in metadata (%s)", action));
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
}
