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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.KinesisSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KinesisReadConfig;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.sources.helpers.KinesisOffsetGen.calculateNumEvents;

/**
 * Source to read JSON data from AWS Kinesis Data Streams using Spark.
 */
@Slf4j
public class JsonKinesisSource extends KinesisSource<JavaRDD<String>> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Metadata-only summary for checkpoint; avoids bringing records to driver. */
  @AllArgsConstructor
  @Getter
  private static class ShardFetchSummary implements Serializable {
    private final String shardId;
    private final Option<String> lastSequenceNumber;
    // UTC in milliseconds.
    private final Option<Long> lastArrivalTime;
    private final int recordCount;
    private final boolean reachedEndOfShard;
  }

  /**
   * Per-shard fetch result stored in the persisted RDD.
   * Records are eagerly materialized as List&lt;String&gt; so that both fields survive RDD spill
   * to disk: List and ShardFetchSummary are fully serializable, unlike a transient Iterable.
   */
  @AllArgsConstructor
  @Getter
  private static class ShardFetchResult implements Serializable {
    private final List<String> records;
    private final ShardFetchSummary summary;
  }

  /** Persisted fetch RDD - must be unpersisted in releaseResources to avoid memory leak. */
  private transient org.apache.spark.api.java.JavaRDD<ShardFetchResult> persistedFetchRdd;
  /** Record count from fetch, avoids redundant batch.count() Spark job. */
  private long lastRecordCount;
  /** Shard IDs where the executor observed nextShardIterator==null (end-of-shard reached). */
  protected Set<String> shardsReachedEnd;
  /** Arrival time (epoch millis) of the record with last sequence number, per shard. */
  protected Map<String, Long> lastArrivalTimes;

  public JsonKinesisSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                           SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    this(properties, sparkContext, sparkSession, metrics,
        new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public JsonKinesisSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                           HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(properties, sparkContext, sparkSession, SourceType.JSON, metrics,
        new DefaultStreamContext(streamContext.getSchemaProvider(), streamContext.getSourceProfileSupplier()));
    this.offsetGen = new KinesisOffsetGen(props);
  }

  @Override
  protected JavaRDD<String> toBatch(KinesisOffsetGen.KinesisShardRange[] shardRanges, long sourceLimit) {
    long numEvents = calculateNumEvents(sourceLimit, props);
    KinesisReadConfig readConfig = new KinesisReadConfig(
        offsetGen.getStreamName(),
        offsetGen.getRegion(),
        offsetGen.getEndpointUrl().orElse(null),
        getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_ACCESS_KEY, null),
        getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_SECRET_KEY, null),
        offsetGen.getStartingPositionStrategy(),
        shouldAddMetaFields,
        getBooleanWithAltKeys(props, KinesisSourceConfig.KINESIS_ENABLE_DEAGGREGATION),
        getIntWithAltKeys(props, KinesisSourceConfig.KINESIS_MAX_RECORDS_PER_REQUEST),
        getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_GET_RECORDS_INTERVAL_MS),
        // NOTE that: Evenly set the max events per shard.
        shardRanges.length > 0 ? Math.max(1, numEvents / shardRanges.length) : numEvents,
        getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_RETRY_INITIAL_INTERVAL_MS),
        getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_RETRY_MAX_INTERVAL_MS),
        getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_THROTTLE_TIMEOUT_MS));

    JavaRDD<ShardFetchResult> fetchRdd = sparkContext.parallelize(
        java.util.Arrays.asList(shardRanges), shardRanges.length)
        .mapPartitions(shardRangeIt -> {
          List<ShardFetchResult> results = new ArrayList<>();
          try (KinesisClient client = KinesisOffsetGen.createKinesisClient(
              readConfig.getRegion(), readConfig.getEndpointUrl(),
              readConfig.getAccessKey(), readConfig.getSecretKey())) {
            while (shardRangeIt.hasNext()) {
              KinesisOffsetGen.KinesisShardRange range = shardRangeIt.next();
              // Lazy iterator: fetches one GetRecords page at a time, keeping only one page in
              // executor memory instead of the full shard batch. Records are GC-eligible as soon
              // as they are converted to JSON strings below.
              KinesisSource.ShardRecordIterator recordIt = KinesisSource.readShardRecords(
                  client, readConfig.getStreamName(), range, readConfig.getStartingPosition(),
                  readConfig.getMaxRecordsPerRequest(), readConfig.getIntervalMilliSeconds(),
                  readConfig.getMaxRecordsPerShard(), readConfig.isEnableDeaggregation(),
                  readConfig.getRetryInitialIntervalMs(), readConfig.getRetryMaxIntervalMs(),
                  readConfig.getThrottleTimeoutMs());

              String shardId = range.getShardId();
              boolean addMetaFields = readConfig.isShouldAddMetaFields();
              List<String> jsonRecords = new ArrayList<>();
              long numNull = 0;
              java.time.Instant lastArrivalTimestamp = null;
              while (recordIt.hasNext()) {
                Record r = recordIt.next();
                lastArrivalTimestamp = r.approximateArrivalTimestamp();
                String s = recordToJsonStatic(r, shardId, addMetaFields);
                if (s != null) {
                  jsonRecords.add(s);
                } else {
                  numNull++;
                }
              }
              if (numNull > 0) {
                log.warn("There are {} null strings for shard id {}", numNull, shardId);
              }
              // Capture the arrival time of the last record (same record whose sequence number
              // becomes the checkpoint lastSeq) so it can be embedded in the checkpoint.
              Option<Long> lastArrivalTime = lastArrivalTimestamp != null
                  ? Option.of(lastArrivalTimestamp.toEpochMilli()) : Option.empty();
              // recordCount reflects actual output records (null-filtered), not raw Kinesis count.
              // NOTE: getLastSequenceNumber/isReachedEndOfShard are final only after hasNext()==false.
              ShardFetchSummary summary = new ShardFetchSummary(shardId,
                  recordIt.getLastSequenceNumber(), lastArrivalTime,
                  jsonRecords.size(), recordIt.isReachedEndOfShard());
              results.add(new ShardFetchResult(jsonRecords, summary));
            }
          }
          return results.iterator();
        });

    if (persistedFetchRdd != null) {
      persistedFetchRdd.unpersist();
      persistedFetchRdd = null;
    }
    boolean persistFetchRdd = getBooleanWithAltKeys(props, KinesisSourceConfig.KINESIS_PERSIST_FETCH_RDD);
    if (persistFetchRdd) {
      fetchRdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
      persistedFetchRdd = fetchRdd;
    } else {
      log.debug("{} is false: fetch RDD is not persisted. The same Kinesis fetch may run twice (checkpoint + "
          + "record write), which can cause duplicate records to be written. Set to true for correct behavior.",
          KinesisSourceConfig.KINESIS_PERSIST_FETCH_RDD.key());
    }
    // Guard: if anything below throws, unpersist immediately so the cached RDD doesn't leak.
    boolean succeeded = false;
    try {
      // Collect basic information that will be used to construct the final checkpoint.
      collectCheckpointInfo(fetchRdd);
      // RDD for the shard data.
      JavaRDD<String> recordRdd = fetchRdd.flatMap(r -> r.getRecords().iterator());
      // Apply minimum partitions for downstream parallelism (similar to Kafka source)
      long manualPartitions = getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_SOURCE_MANUAL_PARTITIONS);
      if (manualPartitions > 0) {
        int targetPartitions = (int) manualPartitions;
        log.info("Repartitioning from {} shards to {} partitions (manualPartitions={})",
            shardRanges.length, targetPartitions, manualPartitions);
        recordRdd = recordRdd.repartition(targetPartitions);
      }
      succeeded = true;
      return recordRdd;
    } finally {
      if (!succeeded) {
        releaseResources();
      }
    }
  }

  private static String recordToJsonStatic(Record record, String shardId, boolean shouldAddMetaFields) {
    String dataStr = record.data().asUtf8String();
    // Pure empty or null records in Kinesis is not meaningful.
    if (dataStr == null || dataStr.trim().isEmpty()) {
      return null;
    }
    if (shouldAddMetaFields) {
      try {
        ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(dataStr);
        node.put("_hoodie_kinesis_source_sequence_number", record.sequenceNumber());
        node.put("_hoodie_kinesis_source_shard_id", shardId);
        node.put("_hoodie_kinesis_source_partition_key", record.partitionKey());
        if (record.approximateArrivalTimestamp() != null) {
          node.put("_hoodie_kinesis_source_timestamp",
              record.approximateArrivalTimestamp().toEpochMilli());
        }
        return OBJECT_MAPPER.writeValueAsString(node);
      } catch (Exception e) {
        // We can disable the flag for mitigation.
        throw new HoodieException("Failed to add metadata fields", e);
      }
    }
    return dataStr;
  }

  private Map<String, String> buildCheckpointFromSummaries(List<ShardFetchSummary> summaries) {
    Map<String, String> checkpoint = new HashMap<>();
    for (ShardFetchSummary s : summaries) {
      if (s.getLastSequenceNumber().isPresent()) {
        checkpoint.put(s.getShardId(), s.getLastSequenceNumber().get());
      }
    }
    return checkpoint;
  }

  private Map<String, Long> buildArrivalTimesFromSummaries(List<ShardFetchSummary> summaries) {
    Map<String, Long> arrivalTimes = new HashMap<>();
    for (ShardFetchSummary s : summaries) {
      if (s.getLastArrivalTime().isPresent()) {
        arrivalTimes.put(s.getShardId(), s.getLastArrivalTime().get());
      }
    }
    return arrivalTimes;
  }

  @Override
  protected String createCheckpointFromBatch(JavaRDD<String> batch,
      KinesisOffsetGen.KinesisShardRange[] shardRangesWithUnreadRecords,
      KinesisOffsetGen.KinesisShardRange[] allOpenClosedShardRanges) {
    // STEP 1: Basic information has been collected through function: collectCheckpointInfo.
    // STEP 2: Build checkpoint for each shard.
    // We need to preserve the fully consumed shards since otherwise
    // the next run would omit them from the checkpoint and re-read from TRIM_HORIZON.
    Map<String, String> fullCheckpoint = new HashMap<>();
    for (KinesisOffsetGen.KinesisShardRange range : allOpenClosedShardRanges) {
      String lastSeq;
      String endSeq;
      // CASE 1: basic case
      // Shards that we read
      boolean wasRead = lastCheckpointData != null && lastCheckpointData.containsKey(range.getShardId());
      if (wasRead) {
        lastSeq = lastCheckpointData.get(range.getShardId());
        endSeq = range.getEndingSequenceNumber().orElse(null);
      } else {
        // Shards that we did not read
        lastSeq = range.getStartingSequenceNumber().orElse("");
        endSeq = range.getEndingSequenceNumber().orElse(null);
      }
      endSeq = normalizeEndSeq(lastSeq, endSeq);
      // CASE 2: corner case: an open shard becomes a closed shard after the read.
      if (endSeq == null && shardsReachedEnd != null && shardsReachedEnd.contains(range.getShardId())
          && lastSeq != null && !lastSeq.isEmpty()) {
        endSeq = lastSeq;
      }
      // CASE 3: corner case: a closed shard is found, lastSeq == '', endSeq != null
      // This can happen: the shard type is latest for a closed shard.
      if (lastSeq != null && lastSeq.isEmpty() && endSeq != null && !endSeq.isEmpty()) {
        lastSeq = endSeq;
      }
      // Include arrival time only for shards we actually read in this batch.
      Long arrivalTime = wasRead && lastArrivalTimes != null ? lastArrivalTimes.get(range.getShardId()) : null;
      String value = KinesisOffsetGen.CheckpointUtils.buildCheckpointValue(lastSeq, arrivalTime, endSeq);
      if (lastSeq != null && !lastSeq.isEmpty()) {
        fullCheckpoint.put(range.getShardId(), value);
      }
    }
    // Step 3: generate the final checkpoint.
    return KinesisOffsetGen.CheckpointUtils.offsetsToStr(offsetGen.getStreamName(), fullCheckpoint);
  }

  /**
   * Hook for subclasses to adjust the endSeq stored in the checkpoint.
   * The default (production) implementation is a no-op.
   */
  protected String normalizeEndSeq(String lastSeq, String endSeq) {
    return endSeq;
  }

  @Override
  protected long getRecordCount(JavaRDD<String> batch) {
    return lastRecordCount;
  }

  @Override
  public void releaseResources() {
    super.releaseResources();
    if (persistedFetchRdd != null) {
      persistedFetchRdd.unpersist();
      persistedFetchRdd = null;
    }
  }

  private void collectCheckpointInfo(JavaRDD<ShardFetchResult> fetchRdd) {
    List<ShardFetchSummary> summaries = fetchRdd.map(ShardFetchResult::getSummary).collect();
    lastCheckpointData = buildCheckpointFromSummaries(summaries);
    lastArrivalTimes = buildArrivalTimesFromSummaries(summaries);
    Set<String> reached = new HashSet<>();
    for (ShardFetchSummary s : summaries) {
      if (s.isReachedEndOfShard()) {
        reached.add(s.getShardId());
      }
    }
    shardsReachedEnd = reached;
    lastRecordCount = summaries.stream().mapToLong(ShardFetchSummary::getRecordCount).sum();
  }
}
