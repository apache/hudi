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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieAvroParquetReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Represents the Archived Timeline for the Hoodie table.
 *
 * <p><h2>Timeline Refresh</h2>
 * <p>Instants are read from the archive file during initialization and never refreshed. To refresh, clients need to call
 * #reload().
 *
 * <p><h2>Serialization/De-serialization</h2>
 * <p>This class can be serialized and de-serialized and on de-serialization the FileSystem is re-initialized.
 */
public class HoodieArchivedTimeline extends HoodieDefaultTimeline {
  public static final String INSTANT_TIME_ARCHIVED_META_FIELD = "instantTime";
  public static final String COMPLETION_TIME_ARCHIVED_META_FIELD = "completionTime";
  private static final String ACTION_ARCHIVED_META_FIELD = "action";
  private static final String METADATA_ARCHIVED_META_FIELD = "metadata";
  private static final String PLAN_ARCHIVED_META_FIELD = "plan";
  private HoodieTableMetaClient metaClient;
  private final Map<String, byte[]> readCommits = new ConcurrentHashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(HoodieArchivedTimeline.class);

  /**
   * Loads all the archived instants.
   * Note that there is no lazy loading, so this may not work if the archived timeline range is really long.
   * TBD: Should we enforce maximum time range?
   */
  public HoodieArchivedTimeline(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    setInstants(this.loadInstants());
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  /**
   * Loads completed instants from startTs(inclusive).
   * Note that there is no lazy loading, so this may not work if really early startTs is specified.
   */
  public HoodieArchivedTimeline(HoodieTableMetaClient metaClient, String startTs) {
    this.metaClient = metaClient;
    setInstants(loadInstants(new StartTsFilter(startTs), LoadMode.METADATA));
    // multiple casts will make this lambda serializable -
    // http://docs.oracle.com/javase/specs/jls/se8/html/jls-15.html#jls-15.16
    this.details = (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails;
  }

  /**
   * For serialization and de-serialization only.
   *
   * @deprecated
   */
  public HoodieArchivedTimeline() {
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
    loadInstants(null, LoadMode.METADATA);
  }

  public void loadCompactionDetailsInMemory(String compactionInstantTime) {
    loadCompactionDetailsInMemory(compactionInstantTime, compactionInstantTime);
  }

  public void loadCompactionDetailsInMemory(String startTs, String endTs) {
    // load compactionPlan
    loadInstants(new TimeRangeFilter(startTs, endTs), LoadMode.PLAN,
        record -> record.get(ACTION_ARCHIVED_META_FIELD).toString().equals(HoodieTimeline.COMMIT_ACTION)
                && record.get(PLAN_ARCHIVED_META_FIELD) != null
    );
  }

  public void clearInstantDetailsFromMemory(String instantTime) {
    this.readCommits.remove(instantTime);
  }

  public void clearInstantDetailsFromMemory(String startTs, String endTs) {
    this.findInstantsInRange(startTs, endTs).getInstants().forEach(instant ->
            this.readCommits.remove(instant.getTimestamp()));
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return Option.ofNullable(readCommits.get(instant.getTimestamp()));
  }

  public HoodieArchivedTimeline reload() {
    return new HoodieArchivedTimeline(metaClient);
  }

  private HoodieInstant readCommit(String instantTime, GenericRecord record, Option<BiConsumer<String, GenericRecord>> instantDetailsConsumer) {
    final String action = record.get(ACTION_ARCHIVED_META_FIELD).toString();
    final String completionTime = record.get(COMPLETION_TIME_ARCHIVED_META_FIELD).toString();
    instantDetailsConsumer.ifPresent(consumer -> consumer.accept(instantTime, record));
    return new HoodieInstant(HoodieInstant.State.COMPLETED, action, instantTime, completionTime);
  }

  @Nullable
  private BiConsumer<String, GenericRecord> getInstantDetailsFunc(LoadMode loadMode) {
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
    return loadInstants(null, LoadMode.SLIM);
  }

  private List<HoodieInstant> loadInstants(String startTs, String endTs) {
    return loadInstants(new TimeRangeFilter(startTs, endTs), LoadMode.METADATA);
  }

  private List<HoodieInstant> loadInstants(TimeRangeFilter filter, LoadMode loadMode) {
    return loadInstants(filter, loadMode, r -> true);
  }

  /**
   * This is method to read selected instants. Do NOT use this directly use one of the helper methods above
   * If loadInstantDetails is set to true, this would also update 'readCommits' map with commit details
   * If filter is specified, only the filtered instants are loaded
   * If commitsFilter is specified, only the filtered records are loaded.
   */
  private List<HoodieInstant> loadInstants(
      @Nullable TimeRangeFilter filter,
      LoadMode loadMode,
      Function<GenericRecord, Boolean> commitsFilter) {
    Map<String, HoodieInstant> instantsInRange = new ConcurrentHashMap<>();
    Option<BiConsumer<String, GenericRecord>> instantDetailsConsumer = Option.ofNullable(getInstantDetailsFunc(loadMode));
    loadInstants(metaClient, filter, loadMode, commitsFilter, (instantTime, avroRecord) -> instantsInRange.putIfAbsent(instantTime, readCommit(instantTime, avroRecord, instantDetailsConsumer)));
    ArrayList<HoodieInstant> result = new ArrayList<>(instantsInRange.values());
    Collections.sort(result);
    return result;
  }

  /**
   * Loads the instants from the timeline.
   *
   * @param metaClient     The meta client.
   * @param filter         The time range filter where the target instant belongs to.
   * @param loadMode       The load mode.
   * @param commitsFilter  Filter of the instant type.
   * @param recordConsumer Consumer of the instant record payload.
   */
  public static void loadInstants(
      HoodieTableMetaClient metaClient,
      @Nullable TimeRangeFilter filter,
      LoadMode loadMode,
      Function<GenericRecord, Boolean> commitsFilter,
      BiConsumer<String, GenericRecord> recordConsumer) {
    try {
      // List all files
      List<String> fileNames = LSMTimeline.latestSnapshotManifest(metaClient).getFileNames();

      Schema readSchema = LSMTimeline.getReadSchema(loadMode);
      fileNames.stream()
          .filter(fileName -> filter == null || LSMTimeline.isFileInRange(filter, fileName))
          .parallel().forEach(fileName -> {
            // Read the archived file
            try (HoodieAvroParquetReader reader = (HoodieAvroParquetReader) HoodieFileReaderFactory.getReaderFactory(HoodieRecordType.AVRO)
                .getFileReader(metaClient.getHadoopConf(), new Path(metaClient.getArchivePath(), fileName))) {
              try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieLSMTimelineInstant.getClassSchema(), readSchema)) {
                while (iterator.hasNext()) {
                  GenericRecord record = (GenericRecord) iterator.next();
                  String instantTime = record.get(INSTANT_TIME_ARCHIVED_META_FIELD).toString();
                  if ((filter == null || filter.isInRange(instantTime))
                      && commitsFilter.apply(record)) {
                    recordConsumer.accept(instantTime, record);
                  }
                }
              }
            } catch (IOException ioException) {
              throw new HoodieIOException("Error open file reader for path: " + new Path(metaClient.getArchivePath(), fileName));
            }
          });
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not load archived commit timeline from path " + metaClient.getArchivePath(), e);
    }
  }

  @Override
  public HoodieDefaultTimeline getWriteTimeline() {
    // filter in-memory instants
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(i ->
            readCommits.containsKey(i.getTimestamp()))
        .filter(s -> validActions.contains(s.getAction())), details);
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Different mode for loading the archived instant metadata.
   */
  public enum LoadMode {
    /**
     * Loads the instantTime, completionTime, action.
     */
    SLIM,
    /**
     * Loads the instantTime, completionTime, action, metadata.
     */
    METADATA,
    /**
     * Loads the instantTime, completionTime, plan.
     */
    PLAN
  }

  /**
   * A time based filter with range (startTs, endTs].
   */
  public static class TimeRangeFilter {
    protected final String startTs;
    protected final String endTs;

    public TimeRangeFilter(String startTs, String endTs) {
      this.startTs = startTs;
      this.endTs = endTs;
    }

    public boolean isInRange(String instantTime) {
      return HoodieTimeline.isInRange(instantTime, this.startTs, this.endTs);
    }
  }

  /**
   * A time based filter with range [startTs, endTs).
   */
  public static class ClosedOpenTimeRangeFilter extends TimeRangeFilter {

    public ClosedOpenTimeRangeFilter(String startTs, String endTs) {
      super(startTs, endTs);
    }

    public boolean isInRange(String instantTime) {
      return HoodieTimeline.isInClosedOpenRange(instantTime, this.startTs, this.endTs);
    }
  }

  /**
   * A time based filter with range [startTs, +&#8734).
   */
  public static class StartTsFilter extends TimeRangeFilter {

    public StartTsFilter(String startTs) {
      super(startTs, null); // endTs is never used
    }

    public boolean isInRange(String instantTime) {
      return HoodieTimeline.compareTimestamps(instantTime, GREATER_THAN_OR_EQUALS, startTs);
    }
  }
}
