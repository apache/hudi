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

/**
 * Source to read JSON data from AWS Kinesis Data Streams using Spark.
 */
@Slf4j
public class JsonKinesisSource extends KinesisSource<JavaRDD<String>> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /** Result from reading a single shard in a partition. */
  @AllArgsConstructor
  @Getter
  private static class ShardFetchResult implements Serializable {
    private final List<String> records;
    private final String shardId;
    private final Option<String> lastSequenceNumber;
    private final boolean reachedEndOfShard;
  }

  /** Metadata-only summary for checkpoint; avoids bringing records to driver. */
  @AllArgsConstructor
  @Getter
  private static class ShardFetchSummary implements Serializable {
    private final String shardId;
    private final Option<String> lastSequenceNumber;
    private final int recordCount;
    private final boolean reachedEndOfShard;
  }

  /** Persisted fetch RDD - must be unpersisted in releaseResources to avoid memory leak. */
  private transient org.apache.spark.api.java.JavaRDD<ShardFetchResult> persistedFetchRdd;
  /** Record count from fetch, avoids redundant batch.count() Spark job. */
  private long lastRecordCount;
  /** Shard IDs where the executor observed nextShardIterator==null (end-of-shard reached). */
  private Set<String> shardsReachedEnd;

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
  protected JavaRDD<String> toBatch(KinesisOffsetGen.KinesisShardRange[] shardRanges) {
    KinesisReadConfig readConfig = new KinesisReadConfig(
        offsetGen.getStreamName(),
        offsetGen.getRegion(),
        offsetGen.getEndpointUrl().orElse(null),
        getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_ACCESS_KEY, null),
        getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_SECRET_KEY, null),
        offsetGen.getStartingPosition(),
        shouldAddOffsets,
        getBooleanWithAltKeys(props, KinesisSourceConfig.KINESIS_ENABLE_DEAGGREGATION),
        getIntWithAltKeys(props, KinesisSourceConfig.KINESIS_GET_RECORDS_MAX_RECORDS),
        getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_GET_RECORDS_INTERVAL_MS),
        shardRanges.length > 0 ? Math.max(1, getLongWithAltKeys(props, KinesisSourceConfig.MAX_EVENTS_FROM_KINESIS_SOURCE) / shardRanges.length) : Long.MAX_VALUE);

    // Assume: number of closed shards is small.
    // TODO: filter closed shards in which all records have been consumed.
    JavaRDD<ShardFetchResult> fetchRdd = sparkContext.parallelize(
        java.util.Arrays.asList(shardRanges), shardRanges.length)
        .mapPartitions(shardRangeIt -> {
          List<ShardFetchResult> results = new ArrayList<>();
          try (KinesisClient client = createKinesisClientFromConfig(readConfig)) {
            while (shardRangeIt.hasNext()) {
              KinesisOffsetGen.KinesisShardRange range = shardRangeIt.next();
              KinesisOffsetGen.ShardReadResult readResult = KinesisOffsetGen.readShardRecords(
                  client, readConfig.getStreamName(), range, readConfig.getStartingPosition(),
                  readConfig.getMaxRecordsPerRequest(), readConfig.getIntervalMs(), readConfig.getMaxRecordsPerShard(),
                  readConfig.isEnableDeaggregation());

              List<String> recordStrings = new ArrayList<>();
              for (Record r : readResult.getRecords()) {
                String json = recordToJsonStatic(r, range.getShardId(), readConfig.isShouldAddOffsets());
                if (json != null) {
                  recordStrings.add(json);
                }
              }
              results.add(new ShardFetchResult(recordStrings, range.getShardId(),
                  readResult.getLastSequenceNumber(), readResult.isReachedEndOfShard()));
            }
          }
          return results.iterator();
        });

    // Cache so we can both get records and checkpoint from the same RDD
    fetchRdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
    persistedFetchRdd = fetchRdd;

    JavaRDD<String> recordRdd = fetchRdd.flatMap(r -> r.getRecords().iterator());

    // Apply minimum partitions for downstream parallelism (similar to Kafka source)
    long minPartitions = getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_SOURCE_MIN_PARTITIONS);
    if (minPartitions > 0 && minPartitions > shardRanges.length) {
      int targetPartitions = (int) minPartitions;
      log.info("Repartitioning from {} shards to {} partitions (minPartitions={})",
          shardRanges.length, targetPartitions, minPartitions);
      recordRdd = recordRdd.repartition(targetPartitions);
    }

    // Collect only metadata (shardId, lastSeq, count, reachedEnd) to driver - not the records (avoids OOM)
    List<ShardFetchSummary> summaries = fetchRdd
        .map(r -> new ShardFetchSummary(r.getShardId(), r.getLastSequenceNumber(), r.getRecords().size(),
            r.isReachedEndOfShard()))
        .collect();
    lastCheckpointData = buildCheckpointFromSummaries(summaries);
    Set<String> reached = new HashSet<>();
    for (ShardFetchSummary s : summaries) {
      if (s.isReachedEndOfShard()) {
        reached.add(s.getShardId());
      }
    }
    shardsReachedEnd = reached;
    lastRecordCount = summaries.stream().mapToLong(ShardFetchSummary::getRecordCount).sum();

    return recordRdd;
  }

  private static KinesisClient createKinesisClientFromConfig(KinesisReadConfig config) {
    software.amazon.awssdk.services.kinesis.KinesisClientBuilder builder =
        KinesisClient.builder().region(software.amazon.awssdk.regions.Region.of(config.getRegion()));
    if (config.getEndpointUrl() != null && !config.getEndpointUrl().isEmpty()) {
      builder = builder.endpointOverride(java.net.URI.create(config.getEndpointUrl()));
    }
    if (config.getAccessKey() != null && !config.getAccessKey().isEmpty()
        && config.getSecretKey() != null && !config.getSecretKey().isEmpty()) {
      builder = builder.credentialsProvider(
          software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
              software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                  config.getAccessKey(), config.getSecretKey())));
    }
    return builder.build();
  }

  private static String recordToJsonStatic(Record record, String shardId, boolean shouldAddOffsets) {
    String dataStr = record.data().asUtf8String();

    // Pure empty or null records in Kinesis is not meaningful.
    if (dataStr == null || dataStr.trim().isEmpty()) {
      return null;
    }

    if (shouldAddOffsets) {
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
        return dataStr;
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

  /** LocalStack returns Long.MAX_VALUE for closed shards' endingSequenceNumber; real AWS returns actual value. */
  private static final String LOCALSTACK_END_SEQ_SENTINEL = "9223372036854775807";

  @Override
  protected String createCheckpointFromBatch(JavaRDD<String> batch, KinesisOffsetGen.KinesisShardRange[] shardRanges) {
    // Build checkpoint: for each shard, use lastSeq from read (or startSeq if no records) and endSeq for closed shards
    Map<String, String> fullCheckpoint = new HashMap<>();
    for (KinesisOffsetGen.KinesisShardRange range : shardRanges) {
      // CASE 1: non-first read, open shard.
      String lastSeq = lastCheckpointData != null && lastCheckpointData.containsKey(range.getShardId())
          ? lastCheckpointData.get(range.getShardId())
          : range.getStartingSequenceNumber().orElse("");
      String endSeq = range.getEndingSequenceNumber().orElse(null);
      // for test only
      // LocalStack returns Long.MAX_VALUE for closed shards; use lastSeq as endSeq so we can detect
      // "fully consumed" when the parent shard expires (lastSeq >= endSeq).
      if (LOCALSTACK_END_SEQ_SENTINEL.equals(endSeq) && lastSeq != null && !lastSeq.isEmpty()) {
        endSeq = lastSeq;
      }
      // CASE 2: The executor reached end-of-shard (nextShardIterator was null) but listShards had not yet
      // reflected the shard close - a race between resharding and our listShards call. Record
      // endSeq=lastSeq so that expiry detection works correctly when the shard later disappears.
      if (endSeq == null && shardsReachedEnd != null && shardsReachedEnd.contains(range.getShardId())
          && lastSeq != null && !lastSeq.isEmpty()) {
        endSeq = lastSeq;
      }
      // CASE 3: Closed shard with 0 records on first read: use endSeq as lastSeq so checkpoint
      // "fully consumed" (lastSeq >= endSeq when shard expires).
      if (lastSeq != null && lastSeq.isEmpty() && endSeq != null && !endSeq.isEmpty()) {
        lastSeq = endSeq;
      }
      String value = KinesisOffsetGen.CheckpointUtils.buildCheckpointValue(lastSeq, endSeq);
      // Only include shards we've read from or closed shards we've exhausted. Omit open shards with 0 records:
      // we cannot use the shard's startingSequenceNumber as lastSeq (AFTER_SEQUENCE_NUMBER would skip the first record).
      if (lastSeq != null && !lastSeq.isEmpty()) {
        fullCheckpoint.put(range.getShardId(), value);
      }
    }
    return KinesisOffsetGen.CheckpointUtils.offsetsToStr(offsetGen.getStreamName(), fullCheckpoint);
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
}
