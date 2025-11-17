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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.TimelineUtils.getInputStreamOptionLegacy;

public class ArchivedTimelineV2 extends BaseTimelineV2 implements HoodieArchivedTimeline, HoodieInstantReader {
  public static final String INSTANT_TIME_ARCHIVED_META_FIELD = "instantTime";
  public static final String COMPLETION_TIME_ARCHIVED_META_FIELD = "completionTime";
  public static final String ACTION_ARCHIVED_META_FIELD = "action";
  public static final String METADATA_ARCHIVED_META_FIELD = "metadata";
  public static final String PLAN_ARCHIVED_META_FIELD = "plan";
  private HoodieTableMetaClient metaClient;
  private final Map<String, byte[]> readCommits = new ConcurrentHashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(HoodieArchivedTimeline.class);

  /**
   * Used for loading the archived timeline incrementally, the earliest loaded instant time get memorized
   * each time the timeline is loaded. The instant time is then used as the end boundary
   * of the next loading.
   */
  private String cursorInstant;
  private final ArchivedTimelineLoader timelineLoader = new ArchivedTimelineLoaderV2();

  /**
   * Loads all the archived instants.
   * Note that there is no lazy loading, so this may not work if the archived timeline range is really long.
   * TBD: Should we enforce maximum time range?
   */
  public ArchivedTimelineV2(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    setInstants(this.loadInstants());
    this.cursorInstant = firstInstant().map(HoodieInstant::requestedTime).orElse(null);
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.instantReader = this;
  }

  /**
   * Loads completed instants from startTs(inclusive).
   * Note that there is no lazy loading, so this may not work if really early startTs is specified.
   */
  public ArchivedTimelineV2(HoodieTableMetaClient metaClient, String startTs) {
    this.metaClient = metaClient;
    setInstants(loadInstants(new HoodieArchivedTimeline.StartTsFilter(startTs), HoodieArchivedTimeline.LoadMode.METADATA));
    this.cursorInstant = startTs;
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.instantReader = this;
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public ArchivedTimelineV2() {
    this.instantReader = this;
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

  public void loadInstantDetailsInMemory(String startTs, String endTs) {
    loadInstants(startTs, endTs);
  }

  public void loadCompletedInstantDetailsInMemory() {
    loadInstants(null, HoodieArchivedTimeline.LoadMode.METADATA);
  }

  public void loadCompactionDetailsInMemory(String compactionInstantTime) {
    loadCompactionDetailsInMemory(compactionInstantTime, compactionInstantTime);
  }

  public void loadCompactionDetailsInMemory(String startTs, String endTs) {
    // load compactionPlan
    loadInstants(new HoodieArchivedTimeline.TimeRangeFilter(startTs, endTs), HoodieArchivedTimeline.LoadMode.PLAN,
        record -> record.get(ACTION_ARCHIVED_META_FIELD).toString().equals(COMMIT_ACTION)
            && record.get(PLAN_ARCHIVED_META_FIELD) != null
    );
  }

  @Override
  public void loadCompactionDetailsInMemory(int limit) {
    loadInstantsWithLimit(limit, HoodieArchivedTimeline.LoadMode.PLAN,
        record -> record.get(ACTION_ARCHIVED_META_FIELD).toString().equals(COMMIT_ACTION)
            && record.get(PLAN_ARCHIVED_META_FIELD) != null
    );
  }

  @Override
  public void loadCompletedInstantDetailsInMemory(String startTs, String endTs) {
    loadInstants(new HoodieArchivedTimeline.TimeRangeFilter(startTs, endTs), HoodieArchivedTimeline.LoadMode.METADATA);
  }

  @Override
  public void loadCompletedInstantDetailsInMemory(int limit) {
    loadInstantsWithLimit(limit, HoodieArchivedTimeline.LoadMode.METADATA, r -> true);
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

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return Option.ofNullable(readCommits.get(instant.requestedTime()));
  }

  @Override
  public InputStream getContentStream(HoodieInstant instant) {
    Option<InputStream> stream = getInputStreamOptionLegacy(this, instant);
    if (stream.isEmpty()) {
      return new ByteArrayInputStream(new byte[]{});
    }
    return stream.get();
  }

  @Override
  public HoodieArchivedTimeline reload() {
    return new ArchivedTimelineV2(metaClient);
  }

  @Override
  public HoodieArchivedTimeline reload(String startTs) {
    if (this.cursorInstant != null) {
      if (InstantComparison.compareTimestamps(startTs, LESSER_THAN, this.cursorInstant)) {
        appendInstants(loadInstants(new HoodieArchivedTimeline.ClosedOpenTimeRangeFilter(startTs, this.cursorInstant), HoodieArchivedTimeline.LoadMode.METADATA));
        this.cursorInstant = startTs;
      }
      return this;
    } else {
      // a null cursor instant indicates an empty timeline
      return new ArchivedTimelineV2(metaClient, startTs);
    }
  }

  private HoodieInstant readCommit(String instantTime, GenericRecord record, Option<BiConsumer<String, GenericRecord>> instantDetailsConsumer) {
    final String action = record.get(ACTION_ARCHIVED_META_FIELD).toString();
    final String completionTime = record.get(COMPLETION_TIME_ARCHIVED_META_FIELD).toString();
    instantDetailsConsumer.ifPresent(consumer -> consumer.accept(instantTime, record));
    return instantGenerator.createNewInstant(HoodieInstant.State.COMPLETED, action, instantTime, completionTime);
  }

  private BiConsumer<String, GenericRecord> getInstantDetailsFunc(HoodieArchivedTimeline.LoadMode loadMode) {
    switch (loadMode) {
      case METADATA:
        return (instant, record) -> {
          ByteBuffer commitMeta = (ByteBuffer) record.get(METADATA_ARCHIVED_META_FIELD);
          if (commitMeta != null) {
            // in case the entry comes from an empty completed meta file
            this.readCommits.put(instant, commitMeta.array());
          }
        };
      case PLAN:
        return (instant, record) -> {
          ByteBuffer plan = (ByteBuffer) record.get(PLAN_ARCHIVED_META_FIELD);
          if (plan != null) {
            // in case the entry comes from an empty completed meta file
            this.readCommits.put(instant, plan.array());
          }
        };
      default:
        return null;
    }
  }

  private List<HoodieInstant> loadInstants() {
    return loadInstants(null, HoodieArchivedTimeline.LoadMode.ACTION);
  }

  private List<HoodieInstant> loadInstants(String startTs, String endTs) {
    return loadInstants(new HoodieArchivedTimeline.TimeRangeFilter(startTs, endTs), HoodieArchivedTimeline.LoadMode.METADATA);
  }

  private List<HoodieInstant> loadInstants(HoodieArchivedTimeline.TimeRangeFilter filter, HoodieArchivedTimeline.LoadMode loadMode) {
    return loadInstants(filter, loadMode, r -> true);
  }

  /**
   * This is method to read selected instants. Do NOT use this directly use one of the helper methods above
   * If loadInstantDetails is set to true, this would also update 'readCommits' map with commit details
   * If filter is specified, only the filtered instants are loaded
   * If commitsFilter is specified, only the filtered records are loaded.
   */
  private List<HoodieInstant> loadInstants(
      HoodieArchivedTimeline.TimeRangeFilter filter,
      HoodieArchivedTimeline.LoadMode loadMode,
      Function<GenericRecord, Boolean> commitsFilter) {
    Map<String, HoodieInstant> instantsInRange = new ConcurrentHashMap<>();
    Option<BiConsumer<String, GenericRecord>> instantDetailsConsumer = Option.ofNullable(getInstantDetailsFunc(loadMode));
    timelineLoader.loadInstants(metaClient, filter, loadMode, commitsFilter,
        (instantTime, avroRecord) -> instantsInRange.putIfAbsent(instantTime, readCommit(instantTime, avroRecord, instantDetailsConsumer)));
    List<HoodieInstant> result = new ArrayList<>(instantsInRange.values());
    Collections.sort(result);
    return result;
  }

  /**
   * Loads instants with a limit on the number of instants to load.
   * This is used for limit-based loading where we only want to load the N most recent instants.
   */
  private void loadInstantsWithLimit(int limit, HoodieArchivedTimeline.LoadMode loadMode,
      Function<GenericRecord, Boolean> commitsFilter) {
    InstantsLoaderWithLimit loader = new InstantsLoaderWithLimit(limit, loadMode);
    timelineLoader.loadInstants(metaClient, null, loadMode, commitsFilter, loader);
  }

  /**
   * Callback to read instant details with a limit on the number of instants to load.
   * Extends BiConsumer to be used as a callback in the timeline loader.
   * The BiConsumer interface allows it to be passed as a lambda/function that accepts
   * (instantTime, GenericRecord) pairs during the loading process.
   */
  private class InstantsLoaderWithLimit implements BiConsumer<String, GenericRecord> {
    private final int limit;
    private final HoodieArchivedTimeline.LoadMode loadMode;
    private volatile int loadedCount = 0;

    private InstantsLoaderWithLimit(int limit, HoodieArchivedTimeline.LoadMode loadMode) {
      this.limit = limit;
      this.loadMode = loadMode;
    }

    @Override
    public void accept(String instantTime, GenericRecord record) {
      if (loadedCount >= limit) {
        return;
      }
      Option<BiConsumer<String, GenericRecord>> instantDetailsConsumer = Option.ofNullable(getInstantDetailsFunc(loadMode));
      readCommit(instantTime, record, instantDetailsConsumer);
      synchronized (this) {
        if (loadedCount < limit) {
          loadedCount++;
        }
      }
    }
  }

  @Override
  public HoodieTimeline getWriteTimeline() {
    // filter in-memory instants
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION,
        LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION);
    return new BaseTimelineV2(getInstantsAsStream().filter(i ->
            readCommits.containsKey(i.requestedTime()))
        .filter(s -> validActions.contains(s.getAction())), instantReader);
  }

  @Override
  public boolean isEmpty(HoodieInstant instant) {
    return getInstantDetails(instant).isEmpty();
  }
}
