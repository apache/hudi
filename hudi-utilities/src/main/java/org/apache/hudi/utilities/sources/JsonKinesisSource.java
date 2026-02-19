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
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;

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
  }

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
    int maxRecordsPerRequest = getIntWithAltKeys(props, KinesisSourceConfig.KINESIS_GET_RECORDS_MAX_RECORDS);
    long intervalMs = getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_GET_RECORDS_INTERVAL_MS);
    long maxEvents = getLongWithAltKeys(props, KinesisSourceConfig.MAX_EVENTS_FROM_KINESIS_SOURCE);
    long maxRecordsPerShard = shardRanges.length > 0 ? Math.max(1, maxEvents / shardRanges.length) : maxEvents;

    JavaRDD<ShardFetchResult> fetchRdd = sparkContext.parallelize(
        java.util.Arrays.asList(shardRanges), shardRanges.length)
        .mapPartitions(shardRangeIt -> {
          List<ShardFetchResult> results = new ArrayList<>();
          try (KinesisClient client = offsetGen.createKinesisClient()) {
            while (shardRangeIt.hasNext()) {
              KinesisOffsetGen.KinesisShardRange range = shardRangeIt.next();
              KinesisOffsetGen.ShardReadResult readResult = KinesisOffsetGen.readShardRecords(
                  client, offsetGen.getStreamName(), range, offsetGen.getStartingPosition(),
                  maxRecordsPerRequest, intervalMs, maxRecordsPerShard);

              List<String> recordStrings = new ArrayList<>();
              for (Record r : readResult.getRecords()) {
                String json = recordToJson(r, range.getShardId());
                if (json != null) {
                  recordStrings.add(json);
                }
              }
              results.add(new ShardFetchResult(recordStrings, range.getShardId(),
                  readResult.getLastSequenceNumber()));
            }
          }
          return results.iterator();
        });

    // Cache so we can both get records and checkpoint from the same RDD
    fetchRdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());

    JavaRDD<String> recordRdd = fetchRdd.flatMap(r -> r.getRecords().iterator());

    // Collect fetch results to build checkpoint - this triggers execution
    List<ShardFetchResult> fetchResults = fetchRdd.collect();
    lastCheckpointData = buildCheckpointFromFetchResults(fetchResults);

    return recordRdd;
  }

  private String recordToJson(Record record, String shardId) {
    String dataStr = record.data().asUtf8String();

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

  private Map<String, String> buildCheckpointFromFetchResults(List<ShardFetchResult> results) {
    Map<String, String> checkpoint = new HashMap<>();
    for (ShardFetchResult r : results) {
      if (r.getLastSequenceNumber().isPresent()) {
        checkpoint.put(r.getShardId(), r.getLastSequenceNumber().get());
      }
    }
    return checkpoint;
  }

  @Override
  protected String createCheckpointFromBatch(JavaRDD<String> batch, KinesisOffsetGen.KinesisShardRange[] shardRanges) {
    // Start with previous checkpoint for shards we didn't read from
    Map<String, String> fullCheckpoint = new HashMap<>();
    for (KinesisOffsetGen.KinesisShardRange range : shardRanges) {
      fullCheckpoint.put(range.getShardId(), range.getStartingSequenceNumber().orElse(""));
    }
    if (lastCheckpointData != null) {
      fullCheckpoint.putAll(lastCheckpointData);
    }
    // Remove shards with empty sequence
    fullCheckpoint.entrySet().removeIf(e -> e.getValue() == null || e.getValue().isEmpty());
    return KinesisOffsetGen.CheckpointUtils.offsetsToStr(offsetGen.getStreamName(), fullCheckpoint);
  }

  @Override
  protected long getRecordCount(JavaRDD<String> batch) {
    return batch.count();
  }
}
