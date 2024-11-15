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
import org.apache.hudi.common.table.log.InstantRange.RangeType;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.sources.HoodieIncrSource;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.CloudDataColumnInfo;
import org.apache.hudi.utilities.streamer.SourceProfile;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT;
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
   * @param numInstantsFromConfig       Max Instants per fetch
   * @param beginInstant              Last Checkpoint String
   * @param missingCheckpointStrategy when begin instant is missing, allow reading based on missing checkpoint strategy
   * @param sourceLimitBasedBatching  When sourceLimit based batching is used, we need to fetch the current commit as well,
   *                                  this flag is used to indicate that.
   * @param lastCheckpointKey         Last checkpoint key (used in the upgrade code path)
   * @return begin and end instants along with query type and other information.
   */
  //TODO rename this method
  public static IncrementalQueryAnalyzer generateQueryInfo(JavaSparkContext jssc, String srcBasePath,
                                            int numInstantsFromConfig, Option<String> beginInstant,
                                            MissingCheckpointStrategy missingCheckpointStrategy,
                                            boolean sourceLimitBasedBatching,
                                            Option<String> lastCheckpointKey) {
    ValidationUtils.checkArgument(numInstantsFromConfig > 0,
        "Make sure the config hoodie.streamer.source.hoodieincr.num_instants is set to a positive value");
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jssc.hadoopConfiguration()))
        .setBasePath(srcBasePath).setLoadActiveTimelineOnLoad(true).build();

    final HoodieTimeline completedCommitTimeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
    String startCompletionTime;
    RangeType rangeType;

    if (beginInstant.isPresent() && !beginInstant.get().isEmpty()) {
      startCompletionTime = beginInstant.get();
      rangeType = RangeType.OPEN_CLOSED;
    } else if (missingCheckpointStrategy != null) {
      rangeType = RangeType.CLOSED_CLOSED;
      switch (missingCheckpointStrategy) {
        case READ_UPTO_LATEST_COMMIT:
          startCompletionTime = DEFAULT_START_TIMESTAMP;
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

    // TODO: why is previous instant time needed? It's not used anywhere
    // When `beginInstantTime` is present, `previousInstantTime` is set to the completed commit before `beginInstantTime` if that exists.
    // If there is no completed commit before `beginInstantTime`, e.g., `beginInstantTime` is the first commit in the active timeline,
    // `previousInstantTime` is set to `DEFAULT_BEGIN_TIMESTAMP`.
    String previousInstantTime = DEFAULT_START_TIMESTAMP; // TODO: this should be removed
    if (startCompletionTime != null && !startCompletionTime.equals(DEFAULT_START_TIMESTAMP)) {
      // has a valid start completion time, try to find previous instant
      Option<HoodieInstant> previousCompletedInstant = completedCommitTimeline.findInstantBeforeByCompletionTime(startCompletionTime);
      if (previousCompletedInstant.isPresent()) {
        previousInstantTime = previousCompletedInstant.get().getTimestamp();
      } else {
        // if begin instant time matches first entry in active timeline, we can set previous = beginInstantTime - 1
        Option<HoodieInstant> firstCompletedInstant = completedCommitTimeline.filterCompletedInstants().firstInstant();
        if (firstCompletedInstant.isPresent()
            && firstCompletedInstant.get().getCompletionTime().equals(startCompletionTime)) {
          previousInstantTime = String.valueOf(Long.parseLong(startCompletionTime) - 1);
        }
      }
    }

    IncrementalQueryAnalyzer.Builder analyzerBuilder = IncrementalQueryAnalyzer.builder();

    if (missingCheckpointStrategy == MissingCheckpointStrategy.READ_LATEST
        || !completedCommitTimeline.isBeforeTimelineStartsByCompletionTime(startCompletionTime)) {
      Option<HoodieInstant> nthInstant; // TODO: remove this
      // When we are in the upgrade code path from non-sourcelimit-based batching to sourcelimit-based batching, we need to avoid fetching the commit
      // that is read already. Else we will have duplicates in append-only use case if we use "findInstantsAfterOrEquals".
      // As soon as we have a new format of checkpoint and a key we will move to the new code of fetching the current commit as well.
      if (sourceLimitBasedBatching && lastCheckpointKey.isPresent()) {
        //        nthInstant = Option.fromJavaOptional(completedCommitTimeline
        //            .findInstantsAfterOrEqualsByCompletionTime(beginCompletionTime, numInstantsFromConfig).getInstantsAsStream().reduce((x, y) -> y));

        // range stays as CLOSED_CLOSED to include the start instant
        rangeType = RangeType.CLOSED_CLOSED;
      } else {
        // set the range type to OPEN_CLOSED to avoid duplicates
        rangeType = RangeType.OPEN_CLOSED;
      }

      return analyzerBuilder
          .metaClient(metaClient)
          .startCompletionTime(startCompletionTime)
          .endCompletionTime(null)
          .rangeType(rangeType)
          .limit(numInstantsFromConfig)
          .build();
    } else {
      // when MissingCheckpointStrategy is set to read everything until latest, trigger snapshot query.
      return analyzerBuilder
          .metaClient(metaClient)
          .startCompletionTime(startCompletionTime)
          .endCompletionTime(null)
          .rangeType(rangeType)
          .limit(-1) // snapshot query, disrespect limit
          .build();

      // TODO: Revisit this, maybe we can remove this check and have IncrementalQueryAnalyzer to handle

      //      Option<HoodieInstant> lastInstant = completedCommitTimeline.lastInstant();
      //      return new QueryInfo(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL(),
      //          previousInstantTime, beginCompletionTime, lastInstant.get().getTimestamp(),
      //          orderColumn, keyColumn, limitColumn);
    }
  }

  public static IncrementalQueryAnalyzer getIncrementalQueryAnalyzer(
      JavaSparkContext jssc,
      String srcPath,
      Option<String> lastCkptStr,
      MissingCheckpointStrategy missingCheckpointStrategy,
      int numInstantsFromConfig,
      Option<SourceProfile<Integer>> latestSourceProfile) {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jssc.hadoopConfiguration()))
        .setBasePath(srcPath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    String startCompletionTime;
    RangeType rangeType;

    if (lastCkptStr.isPresent() && !lastCkptStr.get().isEmpty()) {
      startCompletionTime = lastCkptStr.get();
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
   * @param endCheckpoint  New checkpoint using completion time
   * @return end instants along with filtered rows.
   */
  public static Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> filterAndGenerateCheckpointBasedOnSourceLimit(
      Dataset<Row> sourceData,
      long sourceLimit,
      String endCheckpoint,
      CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint,
      CloudDataColumnInfo cloudDataColumnInfo) {
    if (sourceData.isEmpty()) {
      // There is no file matching the prefix.
      CloudObjectIncrCheckpoint updatedCheckpoint =
              endCheckpoint.equals(cloudObjectIncrCheckpoint.getCommit())
                      ? cloudObjectIncrCheckpoint
                      : new CloudObjectIncrCheckpoint(endCheckpoint, null);
      return Pair.of(updatedCheckpoint, Option.empty());
    }
    // Let's persist the dataset to avoid triggering the dag repeatedly
    sourceData.persist(StorageLevel.MEMORY_AND_DISK());

    // Set ordering in query to enable batching
    Dataset<Row> orderedDf = QueryRunner.applyOrdering(sourceData, cloudDataColumnInfo.getOrderByColumns());
    Option<String> lastCheckpoint = Option.of(cloudObjectIncrCheckpoint.getCommit());
    Option<String> lastCheckpointKey = Option.ofNullable(cloudObjectIncrCheckpoint.getKey());
    Option<String> concatenatedKey = lastCheckpoint.flatMap(checkpoint -> lastCheckpointKey.map(key -> checkpoint + key));

    // Filter until last checkpoint key
    if (concatenatedKey.isPresent()) {
      orderedDf = orderedDf.withColumn("commit_key",
          functions.concat(
              functions.col(cloudDataColumnInfo.getOrderColumn()), functions.col(cloudDataColumnInfo.getKeyColumn())));
      // Apply incremental filter
      orderedDf = orderedDf.filter(functions.col("commit_key").gt(concatenatedKey.get())).drop("commit_key");
      // If there are no more files where commit_key is greater than lastCheckpointCommit#lastCheckpointKey
      if (orderedDf.isEmpty()) {
        LOG.info("Empty ordered source, returning endpoint:" + endCheckpoint);
        sourceData.unpersist();
        // endCheckpoint represents source table's last completed instant
        // If current checkpoint is c1#abc and endCheckpoint is c1, return c1#abc.
        // If current checkpoint is c1#abc and endCheckpoint is c2, return c2.
        CloudObjectIncrCheckpoint updatedCheckpoint =
            endCheckpoint.equals(cloudObjectIncrCheckpoint.getCommit())
                ? cloudObjectIncrCheckpoint
                : new CloudObjectIncrCheckpoint(endCheckpoint, null);
        return Pair.of(updatedCheckpoint, Option.empty());
      }
    }

    // Limit based on sourceLimit
    WindowSpec windowSpec = Window.orderBy(col(cloudDataColumnInfo.getOrderColumn()), col(cloudDataColumnInfo.getKeyColumn()));
    // Add the 'cumulativeSize' column with running sum of 'limitColumn'
    Dataset<Row> aggregatedData = orderedDf.withColumn(CUMULATIVE_COLUMN_NAME,
        sum(col(cloudDataColumnInfo.getLimitColumn())).over(windowSpec));
    Dataset<Row> collectedRows = aggregatedData.filter(col(CUMULATIVE_COLUMN_NAME).leq(sourceLimit));

    Row row;
    if (collectedRows.isEmpty()) {
      // If the first element itself exceeds limits then return first element
      LOG.info("First object exceeding source limit: " + sourceLimit + " bytes");
      row = aggregatedData.select(cloudDataColumnInfo.getOrderColumn(), cloudDataColumnInfo.getKeyColumn(), CUMULATIVE_COLUMN_NAME).first();
      collectedRows = aggregatedData.limit(1);
    } else {
      // Get the last row and form composite key
      row = collectedRows.select(cloudDataColumnInfo.getOrderColumn(), cloudDataColumnInfo.getKeyColumn(), CUMULATIVE_COLUMN_NAME).orderBy(
          col(cloudDataColumnInfo.getOrderColumn()).desc(), col(cloudDataColumnInfo.getKeyColumn()).desc()).first();
    }
    LOG.info("Processed batch size: " + row.get(row.fieldIndex(CUMULATIVE_COLUMN_NAME)) + " bytes");
    sourceData.unpersist();
    // TODO: row.getString(0) is the commit time, not the completion time, need queryContext (filtered by snapshotLoadSplitter)
    //  here to map commit time to completion time again
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
    LOG.info(String.format("existing number of partitions=%d, required number of partitions=%d", existingNumPartitions, numPartitions));
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
  public enum MissingCheckpointStrategy {
    READ_LATEST("Read from latest commit in hoodie source table"),
    READ_UPTO_LATEST_COMMIT("Read everything upto latest commit");

    private final String description;

    MissingCheckpointStrategy(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
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
