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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.table.log.InstantRange.RangeType;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.hudi.utilities.streamer.SourceProfile;

import lombok.Getter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static org.apache.hudi.DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT;
import static org.apache.hudi.common.table.checkpoint.CheckpointUtils.convertToCheckpointV1ForCommitTime;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.instantTimeMinusMillis;
import static org.apache.hudi.common.table.timeline.TimelineUtils.handleHollowCommitIfNeeded;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class IncrSourceHelper {

  private static final Logger LOG = LoggerFactory.getLogger(IncrSourceHelper.class);
  public static final String DEFAULT_START_TIMESTAMP = HoodieTimeline.INIT_INSTANT_TS;
  private static final String CUMULATIVE_COLUMN_NAME = "cumulativeSize";

  /**
   * When hollow commits are found while using incremental source with {@link HoodieDeltaStreamer},
   * unlike batch incremental query, we do not use {@link HollowCommitHandling#FAIL} by default,
   * instead we use {@link HollowCommitHandling#BLOCK} to block processing data from going beyond the
   * hollow commits to avoid unintentional skip.
   * <p>
   * Users can set {@link DataSourceReadOptions#INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT} to
   * {@link HollowCommitHandling#USE_TRANSITION_TIME} to avoid the blocking behavior.
   */
  public static HollowCommitHandling getHollowCommitHandleMode(TypedProperties props) {
    return HollowCommitHandling.valueOf(
        props.getString(INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT().key(), HollowCommitHandling.BLOCK.name()));
  }

  /**
   * Find begin and end instants to be set for the next fetch.
   *
   * @param jssc                      Java Spark Context
   * @param srcBasePath               Base path of Hudi source table
   * @param numInstantsPerFetch       Max Instants per fetch
   * @param beginInstant              Last Checkpoint String
   * @param missingCheckpointStrategy when begin instant is missing, allow reading based on missing checkpoint strategy
   * @param handlingMode              Hollow Commit Handling Mode
   * @param orderColumn               Column to order by (used for size based incr source)
   * @param keyColumn                 Key column (used for size based incr source)
   * @param limitColumn               Limit column (used for size based incr source)
   * @param sourceLimitBasedBatching  When sourceLimit based batching is used, we need to fetch the current commit as well,
   *                                  this flag is used to indicate that.
   * @param lastCheckpointKey         Last checkpoint key (used in the upgrade code path)
   * @return begin and end instants along with query type and other information.
   */
  public static QueryInfo generateQueryInfo(JavaSparkContext jssc, String srcBasePath,
                                            int numInstantsPerFetch, Option<Checkpoint> beginInstant,
                                            MissingCheckpointStrategy missingCheckpointStrategy,
                                            HollowCommitHandling handlingMode,
                                            String orderColumn, String keyColumn, String limitColumn,
                                            boolean sourceLimitBasedBatching,
                                            Option<String> lastCheckpointKey) {
    ValidationUtils.checkArgument(numInstantsPerFetch > 0,
        "Make sure the config hoodie.streamer.source.hoodieincr.num_instants is set to a positive value");
    HoodieTableMetaClient srcMetaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jssc.hadoopConfiguration()))
        .setBasePath(srcBasePath).setLoadActiveTimelineOnLoad(true).build();

    // TODO(yihua): handle transition time in StreamerCheckpointV1
    HoodieTimeline completedCommitTimeline = srcMetaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
    final HoodieTimeline activeCommitTimeline = handleHollowCommitIfNeeded(completedCommitTimeline, srcMetaClient, handlingMode);
    Function<HoodieInstant, String> timestampForLastInstant = instant -> handlingMode == HollowCommitHandling.USE_TRANSITION_TIME
        ? instant.getCompletionTime() : instant.requestedTime();

    Option<Checkpoint> translatedCheckpoint = beginInstant.isPresent()
        ? Option.of(convertToCheckpointV1ForCommitTime(beginInstant.get(), srcMetaClient))
        : Option.empty();
    String beginInstantTime = translatedCheckpoint.orElseGet(() -> {
      if (missingCheckpointStrategy != null) {
        if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST) {
          Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
          return new StreamerCheckpointV1(lastInstant.map(
              hoodieInstant -> instantTimeMinusMillis(timestampForLastInstant.apply(hoodieInstant), 1))
              .orElse(DEFAULT_START_TIMESTAMP));
        } else {
          return new StreamerCheckpointV1(DEFAULT_START_TIMESTAMP);
        }
      } else {
        throw new IllegalArgumentException("Missing begin instant for incremental pull. For reading from latest "
            + "committed instant set hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy to a valid value");
      }
    }).getCheckpointKey();

    // When `beginInstantTime` is present, `previousInstantTime` is set to the completed commit before `beginInstantTime` if that exists.
    // If there is no completed commit before `beginInstantTime`, e.g., `beginInstantTime` is the first commit in the active timeline,
    // `previousInstantTime` is set to `DEFAULT_BEGIN_TIMESTAMP`.
    String previousInstantTime = DEFAULT_START_TIMESTAMP;
    if (!beginInstantTime.equals(DEFAULT_START_TIMESTAMP)) {
      Option<HoodieInstant> previousInstant = activeCommitTimeline.findInstantBefore(beginInstantTime);
      if (previousInstant.isPresent()) {
        previousInstantTime = previousInstant.get().requestedTime();
      } else {
        // if begin instant time matches first entry in active timeline, we can set previous = beginInstantTime - 1
        if (activeCommitTimeline.filterCompletedInstants().firstInstant().isPresent()
            && activeCommitTimeline.filterCompletedInstants().firstInstant().get().requestedTime().equals(beginInstantTime)) {
          previousInstantTime = String.valueOf(Long.parseLong(beginInstantTime) - 1);
        }
      }
    }

    if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST || !activeCommitTimeline.isBeforeTimelineStarts(beginInstantTime)) {
      Option<HoodieInstant> nthInstant;
      // When we are in the upgrade code path from non-sourcelimit-based batching to sourcelimit-based batching, we need to avoid fetching the commit
      // that is read already. Else we will have duplicates in append-only use case if we use "findInstantsAfterOrEquals".
      // As soon as we have a new format of checkpoint and a key we will move to the new code of fetching the current commit as well.
      if (sourceLimitBasedBatching && lastCheckpointKey.isPresent()) {
        nthInstant = Option.fromJavaOptional(activeCommitTimeline
            .findInstantsAfterOrEquals(beginInstantTime, numInstantsPerFetch).getInstantsAsStream().reduce((x, y) -> y));
      } else {
        nthInstant = Option.fromJavaOptional(activeCommitTimeline
            .findInstantsAfter(beginInstantTime, numInstantsPerFetch).getInstantsAsStream().reduce((x, y) -> y));
      }
      return new QueryInfo(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL(), previousInstantTime,
          beginInstantTime, nthInstant.map(HoodieInstant::requestedTime).orElse(beginInstantTime),
          orderColumn, keyColumn, limitColumn);
    } else {
      // when MissingCheckpointStrategy is set to read everything until latest, trigger snapshot query.
      Option<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
      return new QueryInfo(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL(),
          previousInstantTime, beginInstantTime, lastInstant.get().requestedTime(),
          orderColumn, keyColumn, limitColumn);
    }
  }

  public static IncrementalQueryAnalyzer getIncrementalQueryAnalyzer(
      JavaSparkContext jssc,
      String srcPath,
      Option<Checkpoint> lastCheckpoint,
      MissingCheckpointStrategy missingCheckpointStrategy,
      int numInstantsFromConfig,
      Option<SourceProfile<Integer>> latestSourceProfile, TimelineUtils.HollowCommitHandling handlingMode) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jssc.hadoopConfiguration()))
        .setBasePath(srcPath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    String startCompletionTime;
    RangeType rangeType;

    if (lastCheckpoint.isPresent() && !lastCheckpoint.get().getCheckpointKey().isEmpty()) {
      // Translate checkpoint
      StreamerCheckpointV2 lastStreamerCheckpointV2 = CheckpointUtils.convertToCheckpointV2ForCommitTime(
          lastCheckpoint.get(), metaClient, handlingMode);
      startCompletionTime = lastStreamerCheckpointV2.getCheckpointKey();
      rangeType = RangeType.OPEN_CLOSED;
    } else if (missingCheckpointStrategy != null) {
      rangeType = RangeType.CLOSED_CLOSED;
      switch (missingCheckpointStrategy) {
        case READ_UPTO_LATEST_COMMIT:
          startCompletionTime = DEFAULT_START_TIMESTAMP;
          // disrespect numInstantsFromConfig when reading up to latest
          numInstantsFromConfig = -1;
          break;
        case READ_LATEST:
          // rely on IncrementalQueryAnalyzer to use the latest completed instant
          startCompletionTime = null;
          break;
        default:
          throw new IllegalArgumentException("Unknown missing checkpoint strategy: " + missingCheckpointStrategy);
      }
    } else {
      throw new IllegalArgumentException("Missing start completion time for incremental pull. For reading from latest "
          + "committed instant, set " + MISSING_CHECKPOINT_STRATEGY.key() + " to a valid value");
    }

    final int numInstantsFromConfigFinal = numInstantsFromConfig;
    // If source profile exists, use the numInstants from source profile.
    int numInstantsPerFetch = latestSourceProfile.map(sourceProfile -> {
      int numInstantsFromSourceProfile = sourceProfile.getSourceSpecificContext();
      LOG.info("Overriding numInstantsPerFetch from source profile numInstantsFromSourceProfile {} , numInstantsFromConfig {}",
          numInstantsFromSourceProfile, numInstantsFromConfigFinal);
      return numInstantsFromSourceProfile;
    }).orElse(numInstantsFromConfig);

    return IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startCompletionTime(startCompletionTime)
        .endCompletionTime(null)
        .rangeType(rangeType)
        .limit(numInstantsPerFetch)
        .build();
  }

  /**
   * Adjust the source dataset to size based batch based on last checkpoint key.
   *
   * @param sourceData  Source dataset
   * @param sourceLimit Max number of bytes to be read from source
   * @param queryInfo   Query Info
   * @return end instants along with filtered rows.
   */
  public static Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> filterAndGenerateCheckpointBasedOnSourceLimit(Dataset<Row> sourceData,
                                                                                                                    long sourceLimit, QueryInfo queryInfo,
                                                                                                                    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint) {
    if (sourceData.isEmpty()) {
      // There is no file matching the prefix.
      CloudObjectIncrCheckpoint updatedCheckpoint =
              queryInfo.getEndInstant().equals(cloudObjectIncrCheckpoint.getCommit())
                      ? cloudObjectIncrCheckpoint
                      : new CloudObjectIncrCheckpoint(queryInfo.getEndInstant(), null);
      return Pair.of(updatedCheckpoint, Option.empty());
    }
    // Let's persist the dataset to avoid triggering the dag repeatedly
    sourceData.persist(StorageLevel.MEMORY_AND_DISK());
    // Set ordering in query to enable batching
    Dataset<Row> orderedDf = QueryRunner.applyOrdering(sourceData, queryInfo.getOrderByColumns());
    Option<String> lastCheckpoint = Option.of(cloudObjectIncrCheckpoint.getCommit());
    Option<String> lastCheckpointKey = Option.ofNullable(cloudObjectIncrCheckpoint.getKey());
    Option<String> concatenatedKey = lastCheckpoint.flatMap(checkpoint -> lastCheckpointKey.map(key -> checkpoint + key));

    // Filter until last checkpoint key
    if (concatenatedKey.isPresent()) {
      orderedDf = orderedDf.withColumn("commit_key",
          functions.concat(functions.col(queryInfo.getOrderColumn()), functions.col(queryInfo.getKeyColumn())));
      // Apply incremental filter
      orderedDf = orderedDf.filter(functions.col("commit_key").gt(concatenatedKey.get())).drop("commit_key");
      // If there are no more files where commit_key is greater than lastCheckpointCommit#lastCheckpointKey
      if (orderedDf.isEmpty()) {
        LOG.info("Empty ordered source, returning endpoint:" + queryInfo.getEndInstant());
        sourceData.unpersist();
        // queryInfo.getEndInstant() represents source table's last completed instant
        // If current checkpoint is c1#abc and queryInfo.getEndInstant() is c1, return c1#abc.
        // If current checkpoint is c1#abc and queryInfo.getEndInstant() is c2, return c2.
        CloudObjectIncrCheckpoint updatedCheckpoint =
            queryInfo.getEndInstant().equals(cloudObjectIncrCheckpoint.getCommit())
                ? cloudObjectIncrCheckpoint
                : new CloudObjectIncrCheckpoint(queryInfo.getEndInstant(), null);
        return Pair.of(updatedCheckpoint, Option.empty());
      }
    }

    // Limit based on sourceLimit
    WindowSpec windowSpec = Window.orderBy(col(queryInfo.getOrderColumn()), col(queryInfo.getKeyColumn()));
    // Add the 'cumulativeSize' column with running sum of 'limitColumn'
    Dataset<Row> aggregatedData = orderedDf.withColumn(CUMULATIVE_COLUMN_NAME,
        sum(col(queryInfo.getLimitColumn())).over(windowSpec));
    Dataset<Row> collectedRows = aggregatedData.filter(col(CUMULATIVE_COLUMN_NAME).leq(sourceLimit));

    Row row = null;
    if (collectedRows.isEmpty()) {
      // If the first element itself exceeds limits then return first element
      LOG.info("First object exceeding source limit: " + sourceLimit + " bytes");
      row = aggregatedData.select(queryInfo.getOrderColumn(), queryInfo.getKeyColumn(), CUMULATIVE_COLUMN_NAME).first();
      collectedRows = aggregatedData.limit(1);
    } else {
      // Get the last row and form composite key
      row = collectedRows.select(queryInfo.getOrderColumn(), queryInfo.getKeyColumn(), CUMULATIVE_COLUMN_NAME).orderBy(
          col(queryInfo.getOrderColumn()).desc(), col(queryInfo.getKeyColumn()).desc()).first();
    }
    LOG.info("Processed batch size: " + row.get(row.fieldIndex(CUMULATIVE_COLUMN_NAME)) + " bytes");
    sourceData.unpersist();
    return Pair.of(new CloudObjectIncrCheckpoint(row.getString(0), row.getString(1)), Option.of(collectedRows));
  }

  /**
   * Determine the policy to choose if a checkpoint is missing (detected by the absence of a start commit),
   * during a run of a {@link HoodieIncrSource}.
   *
   * @param props the usual Hudi props object
   * @return
   */
  public static MissingCheckpointStrategy getMissingCheckpointStrategy(TypedProperties props) {
    boolean readLatestOnMissingCkpt = getBooleanWithAltKeys(props, READ_LATEST_INSTANT_ON_MISSING_CKPT);

    if (readLatestOnMissingCkpt) {
      return MissingCheckpointStrategy.READ_LATEST;
    }

    if (containsConfigProperty(props, MISSING_CHECKPOINT_STRATEGY)) {
      return MissingCheckpointStrategy.valueOf(getStringWithAltKeys(props, MISSING_CHECKPOINT_STRATEGY));
    }

    return null;
  }

  public static Dataset<Row> coalesceOrRepartition(Dataset dataset, int numPartitions) {
    int existingNumPartitions = dataset.rdd().getNumPartitions();
    LOG.info("Existing number of partitions={}, required number of partitions={}", existingNumPartitions, numPartitions);
    if (existingNumPartitions < numPartitions) {
      dataset = dataset.repartition(numPartitions);
    } else {
      dataset = dataset.coalesce(numPartitions);
    }
    return dataset;
  }

  /**
   * Kafka reset offset strategies.
   */
  @Getter
  public enum MissingCheckpointStrategy {
    READ_LATEST("Read from latest commit in hoodie source table"),
    READ_UPTO_LATEST_COMMIT("Read everything upto latest commit");

    private final String description;

    MissingCheckpointStrategy(String description) {
      this.description = description;
    }

    private static MissingCheckpointStrategy nullEnum() {
      return null;
    }

    @Override
    public String toString() {
      return String.format("%s (%s)", name(), description);
    }
  }
}
