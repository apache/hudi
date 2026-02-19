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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.KinesisSourceConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * Helper for reading from Kinesis Data Streams and managing checkpoints.
 * Checkpoint format: streamName,shardId:sequenceNumber,shardId:sequenceNumber,...
 */
@Slf4j
public class KinesisOffsetGen {

  private static final String METRIC_NAME_KINESIS_MESSAGE_DELAY = "kinesisMessageDelay";

  public static class CheckpointUtils {
    /**
     * Kinesis checkpoint pattern.
     * Format: streamName,shardId:sequenceNumber,shardId:sequenceNumber,...
     */
    private static final Pattern PATTERN = Pattern.compile(".*,.*:.*");

    /**
     * Parse checkpoint string to shardId -> sequenceNumber map.
     */
    public static Map<String, String> strToOffsets(String checkpointStr) {
      Map<String, String> offsetMap = new HashMap<>();
      String[] splits = checkpointStr.split(",");
      for (int i = 1; i < splits.length; i++) {
        String part = splits[i];
        int colonIdx = part.lastIndexOf(':');
        if (colonIdx > 0) {
          String shardId = part.substring(0, colonIdx);
          String seqNum = part.substring(colonIdx + 1);
          offsetMap.put(shardId, seqNum);
        }
      }
      return offsetMap;
    }

    /**
     * String representation of checkpoint.
     * Format: streamName,shardId:sequenceNumber,shardId:sequenceNumber,...
     */
    public static String offsetsToStr(String streamName, Map<String, String> shardToSequenceNumber) {
      String parts = shardToSequenceNumber.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(e -> e.getKey() + ":" + e.getValue())
          .collect(Collectors.joining(","));
      return streamName + "," + parts;
    }

    public static boolean checkStreamCheckpoint(Option<String> lastCheckpointStr) {
      return lastCheckpointStr.isPresent() && PATTERN.matcher(lastCheckpointStr.get()).matches();
    }
  }

  /**
   * Represents a shard to read from, with optional starting sequence number.
   */
  @AllArgsConstructor
  @Getter
  public static class KinesisShardRange implements java.io.Serializable {
    private final String shardId;
    /** If empty, use TRIM_HORIZON or LATEST based on config. */
    private final Option<String> startingSequenceNumber;

    public static KinesisShardRange of(String shardId, Option<String> seqNum) {
      return new KinesisShardRange(shardId, seqNum);
    }
  }

  @Getter
  private final String streamName;
  private final String region;
  private final Option<String> endpointUrl;
  @Getter
  private final KinesisSourceConfig.KinesisStartingPosition startingPosition;
  private final TypedProperties props;

  public KinesisOffsetGen(TypedProperties props) {
    this.props = props;
    checkRequiredConfigProperties(props,
        Arrays.asList(KinesisSourceConfig.KINESIS_STREAM_NAME, KinesisSourceConfig.KINESIS_REGION));
    this.streamName = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_STREAM_NAME);
    this.region = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_REGION);
    this.endpointUrl = Option.ofNullable(getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_ENDPOINT_URL, null));
    String posStr = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_STARTING_POSITION, true);
    String normalized = posStr.toUpperCase().replace("EARLIEST", "TRIM_HORIZON");
    this.startingPosition = KinesisSourceConfig.KinesisStartingPosition.valueOf(normalized);
  }

  public KinesisClient createKinesisClient() {
    KinesisClientBuilder builder = KinesisClient.builder().region(Region.of(region));
    if (endpointUrl.isPresent()) {
      builder = builder.endpointOverride(URI.create(endpointUrl.get()));
    }
    return builder.build();
  }

  /**
   * List all active shards for the stream.
   */
  public List<Shard> listShards(KinesisClient client) {
    List<Shard> allShards = new ArrayList<>();
    String nextToken = null;
    do {
      ListShardsRequest.Builder requestBuilder = ListShardsRequest.builder().streamName(streamName);
      if (nextToken != null) {
        requestBuilder.nextToken(nextToken);
      }
      ListShardsResponse response = client.listShards(requestBuilder.build());
      allShards.addAll(response.shards());
      nextToken = response.nextToken();
    } while (nextToken != null);

    // Filter to only open shards (shards that have an open range)
    List<Shard> openShards = allShards.stream()
        .filter(s -> s.sequenceNumberRange() != null && s.sequenceNumberRange().endingSequenceNumber() == null)
        .collect(Collectors.toList());
    log.info("Found {} open shards for stream {} ({} total)", openShards.size(), streamName, allShards.size());
    return openShards;
  }

  /**
   * Get shard ranges to read, based on checkpoint and limits.
   */
  public KinesisShardRange[] getNextShardRanges(Option<Checkpoint> lastCheckpoint, long sourceLimit,
      HoodieIngestionMetrics metrics) {
    long maxEvents = getLongWithAltKeys(props, KinesisSourceConfig.MAX_EVENTS_FROM_KINESIS_SOURCE);
    long numEvents = sourceLimit == Long.MAX_VALUE ? maxEvents : Math.min(sourceLimit, maxEvents);
    long minPartitions = getLongWithAltKeys(props, KinesisSourceConfig.KINESIS_SOURCE_MIN_PARTITIONS);

    try (KinesisClient client = createKinesisClient()) {
      List<Shard> shards = listShards(client);
      if (shards.isEmpty()) {
        return new KinesisShardRange[0];
      }

      Map<String, String> fromSequenceNumbers = new HashMap<>();
      Option<String> lastCheckpointStr = lastCheckpoint.isPresent()
          ? Option.of(lastCheckpoint.get().getCheckpointKey()) : Option.empty();

      if (lastCheckpointStr.isPresent() && CheckpointUtils.checkStreamCheckpoint(lastCheckpointStr)) {
        Map<String, String> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
        if (!checkpointOffsets.isEmpty() && lastCheckpointStr.get().startsWith(streamName + ",")) {
          fromSequenceNumbers.putAll(checkpointOffsets);
        }
      }

      List<KinesisShardRange> ranges = new ArrayList<>();
      for (Shard shard : shards) {
        String shardId = shard.shardId();
        Option<String> startSeq = fromSequenceNumbers.containsKey(shardId)
            ? Option.of(fromSequenceNumbers.get(shardId))
            : Option.empty();
        ranges.add(KinesisShardRange.of(shardId, startSeq));
      }

      metrics.updateStreamerSourceParallelism(ranges.size());
      long eventsPerShard = minPartitions > 0 ? Math.max(1, numEvents / Math.max(minPartitions, ranges.size())) : numEvents;

      log.info("About to read up to {} events from {} shards in stream {}",
          numEvents, ranges.size(), streamName);
      return ranges.toArray(new KinesisShardRange[0]);
    }
  }

  /**
   * Result of reading from a shard: records and the last sequence number for checkpoint.
   */
  @AllArgsConstructor
  @Getter
  public static class ShardReadResult implements java.io.Serializable {
    private final List<Record> records;
    private final Option<String> lastSequenceNumber;
  }

  /**
   * Read records from a single shard.
   */
  public static ShardReadResult readShardRecords(KinesisClient client, String streamName,
      KinesisShardRange range, KinesisSourceConfig.KinesisStartingPosition defaultPosition,
      int maxRecordsPerRequest, long intervalMs, long maxTotalRecords) throws InterruptedException {
    String shardIterator = getShardIterator(client, streamName, range, defaultPosition);
    List<Record> allRecords = new ArrayList<>();
    String lastSequenceNumber = null;
    int requestCount = 0;

    while (allRecords.size() < maxTotalRecords && shardIterator != null) {
      GetRecordsResponse response = client.getRecords(
          GetRecordsRequest.builder()
              .shardIterator(shardIterator)
              .limit(Math.min(maxRecordsPerRequest, (int) (maxTotalRecords - allRecords.size())))
              .build());

      List<Record> records = response.records();
      for (Record r : records) {
        allRecords.add(r);
        lastSequenceNumber = r.sequenceNumber();
      }

      shardIterator = response.nextShardIterator();
      requestCount++;
      if (shardIterator != null && intervalMs > 0 && !records.isEmpty()) {
        Thread.sleep(intervalMs);
      }
    }

    log.debug("Read {} records from shard {} in {} requests", allRecords.size(), range.getShardId(), requestCount);
    return new ShardReadResult(allRecords, Option.ofNullable(lastSequenceNumber));
  }

  private static String getShardIterator(KinesisClient client, String streamName,
      KinesisShardRange range, KinesisSourceConfig.KinesisStartingPosition defaultPosition) {
    GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
        .streamName(streamName)
        .shardId(range.getShardId());

    if (range.getStartingSequenceNumber().isPresent()) {
      builder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
      builder.startingSequenceNumber(range.getStartingSequenceNumber().get());
    } else {
      if (defaultPosition == KinesisSourceConfig.KinesisStartingPosition.TRIM_HORIZON
          || defaultPosition == KinesisSourceConfig.KinesisStartingPosition.EARLIEST) {
        builder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
      } else {
        builder.shardIteratorType(ShardIteratorType.LATEST);
      }
    }

    return client.getShardIterator(builder.build()).shardIterator();
  }

  /**
   * Convert Kinesis Record to JSON string (data as UTF-8).
   */
  public static String recordToJsonString(Record record) {
    return record.data().asUtf8String();
  }
}
