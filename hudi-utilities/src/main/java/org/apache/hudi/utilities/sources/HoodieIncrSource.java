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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer.QueryContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy;
import org.apache.hudi.utilities.streamer.SourceProfile;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.DataSourceReadOptions.END_COMMIT;
import static org.apache.hudi.DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.apache.hudi.DataSourceReadOptions.START_COMMIT;
import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.UtilHelpers.createRecordMerger;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.coalesceOrRepartition;

public class HoodieIncrSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIncrSource.class);
  public static final Set<String> HOODIE_INCR_SOURCE_READ_OPT_KEYS =
      CollectionUtils.createImmutableSet(HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key());
  private final Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter;
  private final Option<HoodieIngestionMetrics> metricsOption;

  public static class Config {

    /**
     * {@link #HOODIE_SRC_BASE_PATH} is the base-path for the source Hoodie table.
     */
    @Deprecated
    public static final String HOODIE_SRC_BASE_PATH = HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key();

    /**
     * {@link #NUM_INSTANTS_PER_FETCH} allows the max number of instants whose changes can be incrementally fetched.
     */
    @Deprecated
    static final String NUM_INSTANTS_PER_FETCH = HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key();
    @Deprecated
    static final Integer DEFAULT_NUM_INSTANTS_PER_FETCH = HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.defaultValue();

    /**
     * {@link #HOODIE_SRC_PARTITION_FIELDS} specifies partition fields that needs to be added to source table after
     * parsing _hoodie_partition_path.
     */
    @Deprecated
    static final String HOODIE_SRC_PARTITION_FIELDS = HoodieIncrSourceConfig.HOODIE_SRC_PARTITION_FIELDS.key();

    /**
     * {@link #HOODIE_SRC_PARTITION_EXTRACTORCLASS} PartitionValueExtractor class to extract partition fields from
     * _hoodie_partition_path.
     */
    @Deprecated
    static final String HOODIE_SRC_PARTITION_EXTRACTORCLASS = HoodieIncrSourceConfig.HOODIE_SRC_PARTITION_EXTRACTORCLASS.key();
    @Deprecated
    static final String DEFAULT_HOODIE_SRC_PARTITION_EXTRACTORCLASS =
        HoodieIncrSourceConfig.HOODIE_SRC_PARTITION_EXTRACTORCLASS.defaultValue();

    /**
     * {@link  #READ_LATEST_INSTANT_ON_MISSING_CKPT} allows Hudi Streamer to incrementally fetch from latest committed
     * instant when checkpoint is not provided. This config is deprecated. Please refer to {@link #MISSING_CHECKPOINT_STRATEGY}.
     */
    @Deprecated
    public static final String READ_LATEST_INSTANT_ON_MISSING_CKPT = HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.key();
    @Deprecated
    public static final Boolean DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT =
        HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.defaultValue();

    /**
     * {@link  #MISSING_CHECKPOINT_STRATEGY} allows Hudi Streamer to decide the checkpoint to consume from when checkpoint is not set.
     * instant when checkpoint is not provided.
     */
    @Deprecated
    public static final String MISSING_CHECKPOINT_STRATEGY = HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key();

    /**
     * {@link  #SOURCE_FILE_FORMAT} is passed to the reader while loading dataset. Default value is parquet.
     */
    @Deprecated
    static final String SOURCE_FILE_FORMAT = HoodieIncrSourceConfig.SOURCE_FILE_FORMAT.key();
    @Deprecated
    static final String DEFAULT_SOURCE_FILE_FORMAT = HoodieIncrSourceConfig.SOURCE_FILE_FORMAT.defaultValue();

    /**
     * Drops all meta fields from the source hudi table while ingesting into sink hudi table.
     */
    @Deprecated
    static final String HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE = HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE.key();
    @Deprecated
    public static final Boolean DEFAULT_HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE =
        HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE.defaultValue();
  }

  private final Map<String, String> readOpts = new HashMap<>();

  public HoodieIncrSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          StreamContext streamContext) {
    this(props, sparkContext, sparkSession, null, streamContext);
  }

  public HoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      HoodieIngestionMetrics metricsOption,
      StreamContext streamContext) {
    super(props, sparkContext, sparkSession, streamContext);
    for (Object key : props.keySet()) {
      String keyString = key.toString();
      if (HOODIE_INCR_SOURCE_READ_OPT_KEYS.contains(keyString)) {
        readOpts.put(keyString, props.getString(key.toString()));
      }
    }
    this.snapshotLoadQuerySplitter = SnapshotLoadQuerySplitter.getInstance(props);
    this.metricsOption = Option.ofNullable(metricsOption);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    checkRequiredConfigProperties(props, Collections.singletonList(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH));
    String srcPath = getStringWithAltKeys(props, HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH);
    boolean readLatestOnMissingCkpt = getBooleanWithAltKeys(
        props, HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT);
    IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy =
        (containsConfigProperty(props, HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY))
            ? IncrSourceHelper.MissingCheckpointStrategy.valueOf(
                getStringWithAltKeys(props, HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY))
            : null;
    if (readLatestOnMissingCkpt) {
      missingCheckpointStrategy = IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST;
    }

    IncrementalQueryAnalyzer analyzer = IncrSourceHelper.getIncrementalQueryAnalyzer(
        sparkContext, srcPath, lastCkptStr, missingCheckpointStrategy,
        getIntWithAltKeys(props, HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH),
        getLatestSourceProfile());
    QueryContext queryContext = analyzer.analyze();
    Option<InstantRange> instantRange = queryContext.getInstantRange();

    String endCompletionTime;
    // analyzer.getStartCompletionTime() is empty only when reading the latest instant
    // in the first batch
    if (queryContext.isEmpty()
        || (endCompletionTime = queryContext.getMaxCompletionTime())
        .equals(analyzer.getStartCompletionTime().orElseGet(() -> null))) {
      LOG.info("Already caught up. No new data to process");
      return Pair.of(Option.empty(), lastCkptStr.orElse(null));
    }

    DataFrameReader reader = sparkSession.read().format("hudi");
    String datasourceOpts = getStringWithAltKeys(props, HoodieIncrSourceConfig.HOODIE_INCREMENTAL_SPARK_DATASOURCE_OPTIONS, true);
    if (!StringUtils.isNullOrEmpty(datasourceOpts)) {
      Map<String, String> optionsMap = Arrays.stream(datasourceOpts.split(","))
          .map(option -> Pair.of(option.split("=")[0], option.split("=")[1]))
          .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
      reader = reader.options(optionsMap);
    }

    boolean shouldFullScan =
        missingCheckpointStrategy == MissingCheckpointStrategy.READ_UPTO_LATEST_COMMIT
            && queryContext.getActiveTimeline()
            .isBeforeTimelineStartsByCompletionTime(analyzer.getStartCompletionTime().get());
    Dataset<Row> source;
    if (instantRange.isEmpty() || shouldFullScan) {
      // snapshot query
      Dataset<Row> snapshot = reader
          .options(readOpts)
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
          .load(srcPath);

      Option<String> predicate = Option.empty();
      List<String> instantTimeList = queryContext.getInstantTimeList();
      if (snapshotLoadQuerySplitter.isPresent()) {
        Option<SnapshotLoadQuerySplitter.CheckpointWithPredicates> newCheckpointAndPredicate =
            snapshotLoadQuerySplitter.get().getNextCheckpointWithPredicates(snapshot, queryContext);
        if (newCheckpointAndPredicate.isPresent()) {
          endCompletionTime = newCheckpointAndPredicate.get().getEndCompletionTime();
          predicate = Option.of(newCheckpointAndPredicate.get().getPredicateFilter());
          instantTimeList = queryContext.getInstants().stream()
              .filter(instant -> HoodieTimeline.compareTimestamps(
                  instant.getCompletionTime(), HoodieTimeline.LESSER_THAN_OR_EQUALS, newCheckpointAndPredicate.get().getEndCompletionTime()))
              .map(HoodieInstant::getTimestamp)
              .collect(Collectors.toList());
        } else {
          endCompletionTime = queryContext.getMaxCompletionTime();
        }
      }

      snapshot = predicate.map(snapshot::filter).orElse(snapshot);
      source = snapshot
          // add filtering so that only interested records are returned.
          .filter(String.format("%s IN ('%s')", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              String.join("','", instantTimeList)));
    } else {
      // normal incremental query
      String inclusiveStartCompletionTime = queryContext.getInstants().stream()
          .min(HoodieInstant.COMPLETION_TIME_COMPARATOR)
          .map(HoodieInstant::getCompletionTime)
          .get();

      source = reader
          .options(readOpts)
          .option(QUERY_TYPE().key(), QUERY_TYPE_INCREMENTAL_OPT_VAL())
          .option(START_COMMIT().key(), inclusiveStartCompletionTime)
          .option(END_COMMIT().key(), endCompletionTime)
          .option(INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN().key(),
              props.getString(INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN().key(),
                  INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN().defaultValue()))
          .load(srcPath);
    }

    HoodieRecord.HoodieRecordType recordType = createRecordMerger(props).getRecordType();

    boolean shouldDropMetaFields = getBooleanWithAltKeys(
        props, HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE)
        // NOTE: In case when Spark native [[RecordMerger]] is used, we have to make sure
        //       all meta-fields have been properly cleaned up from the incoming dataset
        //
        || recordType == HoodieRecord.HoodieRecordType.SPARK;

    // Remove Hoodie meta columns except partition path from input source
    String[] colsToDrop = shouldDropMetaFields ? HoodieRecord.HOODIE_META_COLUMNS.stream().toArray(String[]::new) :
        HoodieRecord.HOODIE_META_COLUMNS.stream().filter(x -> !x.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toArray(String[]::new);
    Dataset<Row> sourceWithMetaColumnsDropped = source.drop(colsToDrop);
    Dataset<Row> src = getLatestSourceProfile().map(sourceProfile -> {
      metricsOption.ifPresent(metrics -> metrics.updateStreamerSourceBytesToBeIngestedInSyncRound(sourceProfile.getMaxSourceBytes()));
      metricsOption.ifPresent(metrics -> metrics.updateStreamerSourceParallelism(sourceProfile.getSourcePartitions()));
      return coalesceOrRepartition(sourceWithMetaColumnsDropped, sourceProfile.getSourcePartitions());
    }).orElse(sourceWithMetaColumnsDropped);
    return Pair.of(Option.of(src), endCompletionTime);
  }

  // Try to fetch the latestSourceProfile, this ensures the profile is refreshed if it's no longer valid.
  private Option<SourceProfile<Integer>> getLatestSourceProfile() {
    return sourceProfileSupplier.map(SourceProfileSupplier::getSourceProfile);
  }
}
