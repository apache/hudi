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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDeltaStreamerException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Source to read data from Kafka, incrementally.
 */
public class KafkaOffsetGen {

  private static final Logger LOG = LogManager.getLogger(KafkaOffsetGen.class);

  /**
   * kafka checkpoint Pattern.
   * Format: topic_name,partition_num:offset,partition_num:offset,....
   */
  private final Pattern pattern = Pattern.compile(".*,.*:.*");

  public static class CheckpointUtils {

    /**
     * Reconstruct checkpoint from timeline.
     */
    public static HashMap<TopicPartition, Long> strToOffsets(String checkpointStr) {
      HashMap<TopicPartition, Long> offsetMap = new HashMap<>();
      String[] splits = checkpointStr.split(",");
      String topic = splits[0];
      for (int i = 1; i < splits.length; i++) {
        String[] subSplits = splits[i].split(":");
        offsetMap.put(new TopicPartition(topic, Integer.parseInt(subSplits[0])), Long.parseLong(subSplits[1]));
      }
      return offsetMap;
    }

    /**
     * String representation of checkpoint
     * <p>
     * Format: topic1,0:offset0,1:offset1,2:offset2, .....
     */
    public static String offsetsToStr(OffsetRange[] ranges) {
      StringBuilder sb = new StringBuilder();
      // at least 1 partition will be present.
      sb.append(ranges[0].topic() + ",");
      sb.append(Arrays.stream(ranges).map(r -> String.format("%s:%d", r.partition(), r.untilOffset()))
              .collect(Collectors.joining(",")));
      return sb.toString();
    }

    /**
     * Compute the offset ranges to read from Kafka, while handling newly added partitions, skews, event limits.
     *
     * @param fromOffsetMap offsets where we left off last time
     * @param toOffsetMap offsets of where each partitions is currently at
     * @param numEvents maximum number of events to read.
     */
    public static OffsetRange[] computeOffsetRanges(Map<TopicPartition, Long> fromOffsetMap,
                                                    Map<TopicPartition, Long> toOffsetMap, long numEvents) {

      Comparator<OffsetRange> byPartition = Comparator.comparing(OffsetRange::partition);

      // Create initial offset ranges for each 'to' partition, with from = to offsets.
      OffsetRange[] ranges = new OffsetRange[toOffsetMap.size()];
      toOffsetMap.keySet().stream().map(tp -> {
        long fromOffset = fromOffsetMap.getOrDefault(tp, 0L);
        return OffsetRange.create(tp, fromOffset, fromOffset);
      }).sorted(byPartition).collect(Collectors.toList()).toArray(ranges);

      long allocedEvents = 0;
      Set<Integer> exhaustedPartitions = new HashSet<>();
      // keep going until we have events to allocate and partitions still not exhausted.
      while (allocedEvents < numEvents && exhaustedPartitions.size() < toOffsetMap.size()) {
        long remainingEvents = numEvents - allocedEvents;
        long eventsPerPartition =
                (long) Math.ceil((1.0 * remainingEvents) / (toOffsetMap.size() - exhaustedPartitions.size()));

        // Allocate the remaining events to non-exhausted partitions, in round robin fashion
        for (int i = 0; i < ranges.length; i++) {
          OffsetRange range = ranges[i];
          if (!exhaustedPartitions.contains(range.partition())) {
            long toOffsetMax = toOffsetMap.get(range.topicPartition());
            long toOffset = Math.min(toOffsetMax, range.untilOffset() + eventsPerPartition);
            if (toOffset == toOffsetMax) {
              exhaustedPartitions.add(range.partition());
            }
            allocedEvents += toOffset - range.untilOffset();
            // We need recompute toOffset if allocedEvents larger than numEvents.
            if (allocedEvents > numEvents) {
              long offsetsToAdd = Math.min(eventsPerPartition, (numEvents - allocedEvents));
              toOffset = Math.min(toOffsetMax, toOffset + offsetsToAdd);
            }
            ranges[i] = OffsetRange.create(range.topicPartition(), range.fromOffset(), toOffset);
          }
        }
      }

      return ranges;
    }

    public static long totalNewMessages(OffsetRange[] ranges) {
      return Arrays.stream(ranges).mapToLong(OffsetRange::count).sum();
    }
  }

  /**
   * Kafka reset offset strategies.
   */
  enum KafkaResetOffsetStrategies {
    LATEST, EARLIEST
  }

  /**
   * Configs to be passed for this source. All standard Kafka consumer configs are also respected
   */
  public static class Config {

    private static final String KAFKA_TOPIC_NAME = "hoodie.deltastreamer.source.kafka.topic";
    private static final String MAX_EVENTS_FROM_KAFKA_SOURCE_PROP = "hoodie.deltastreamer.kafka.source.maxEvents";
    // "auto.reset.offsets" is kafka native config param. Do not change the config param name.
    public static final String KAFKA_AUTO_RESET_OFFSETS = "auto.reset.offsets";
    private static final KafkaResetOffsetStrategies DEFAULT_KAFKA_AUTO_RESET_OFFSETS = KafkaResetOffsetStrategies.LATEST;
    public static final long DEFAULT_MAX_EVENTS_FROM_KAFKA_SOURCE = 5000000;
    public static long maxEventsFromKafkaSource = DEFAULT_MAX_EVENTS_FROM_KAFKA_SOURCE;
  }

  private final HashMap<String, Object> kafkaParams;
  private final TypedProperties props;
  protected final String topicName;
  private KafkaResetOffsetStrategies autoResetValue;

  public KafkaOffsetGen(TypedProperties props) {
    this.props = props;

    kafkaParams = new HashMap<>();
    for (Object prop : props.keySet()) {
      kafkaParams.put(prop.toString(), props.get(prop.toString()));
    }
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.KAFKA_TOPIC_NAME));
    topicName = props.getString(Config.KAFKA_TOPIC_NAME);
    String kafkaAutoResetOffsetsStr = props.getString(Config.KAFKA_AUTO_RESET_OFFSETS, Config.DEFAULT_KAFKA_AUTO_RESET_OFFSETS.name().toLowerCase());
    boolean found = false;
    for (KafkaResetOffsetStrategies entry: KafkaResetOffsetStrategies.values()) {
      if (entry.name().toLowerCase().equals(kafkaAutoResetOffsetsStr)) {
        found = true;
        autoResetValue = entry;
        break;
      }
    }
    if (!found) {
      throw new HoodieDeltaStreamerException(Config.KAFKA_AUTO_RESET_OFFSETS + " config set to unknown value " + kafkaAutoResetOffsetsStr);
    }
  }

  public OffsetRange[] getNextOffsetRanges(Option<String> lastCheckpointStr, long sourceLimit, HoodieDeltaStreamerMetrics metrics) {

    // Obtain current metadata for the topic
    Map<TopicPartition, Long> fromOffsets;
    Map<TopicPartition, Long> toOffsets;
    try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
      if (!checkTopicExists(consumer)) {
        throw new HoodieException("Kafka topic:" + topicName + " does not exist");
      }
      List<PartitionInfo> partitionInfoList;
      partitionInfoList = consumer.partitionsFor(topicName);
      Set<TopicPartition> topicPartitions = partitionInfoList.stream()
              .map(x -> new TopicPartition(x.topic(), x.partition())).collect(Collectors.toSet());

      // Determine the offset ranges to read from
      if (lastCheckpointStr.isPresent() && !lastCheckpointStr.get().isEmpty() && checkTopicCheckpoint(lastCheckpointStr)) {
        fromOffsets = fetchValidOffsets(consumer, lastCheckpointStr, topicPartitions);
        metrics.updateDeltaStreamerKafkaDelayCountMetrics(delayOffsetCalculation(lastCheckpointStr, topicPartitions, consumer));
      } else {
        switch (autoResetValue) {
          case EARLIEST:
            fromOffsets = consumer.beginningOffsets(topicPartitions);
            break;
          case LATEST:
            fromOffsets = consumer.endOffsets(topicPartitions);
            break;
          default:
            throw new HoodieNotSupportedException("Auto reset value must be one of 'earliest' or 'latest' ");
        }
      }

      // Obtain the latest offsets.
      toOffsets = consumer.endOffsets(topicPartitions);
    }

    // Come up with final set of OffsetRanges to read (account for new partitions, limit number of events)
    long maxEventsToReadFromKafka = props.getLong(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP,
        Config.maxEventsFromKafkaSource);

    long numEvents;
    if (sourceLimit == Long.MAX_VALUE) {
      numEvents = maxEventsToReadFromKafka;
      LOG.info("SourceLimit not configured, set numEvents to default value : " + maxEventsToReadFromKafka);
    } else {
      numEvents = sourceLimit;
    }

    if (numEvents < toOffsets.size()) {
      throw new HoodieException("sourceLimit should not be less than the number of kafka partitions");
    }

    return CheckpointUtils.computeOffsetRanges(fromOffsets, toOffsets, numEvents);
  }

  /**
   * Fetch checkpoint offsets for each partition.
   * @param consumer instance of {@link KafkaConsumer} to fetch offsets from.
   * @param lastCheckpointStr last checkpoint string.
   * @param topicPartitions set of topic partitions.
   * @return a map of Topic partitions to offsets.
   */
  private Map<TopicPartition, Long> fetchValidOffsets(KafkaConsumer consumer,
                                                        Option<String> lastCheckpointStr, Set<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(topicPartitions);
    Map<TopicPartition, Long> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
    boolean checkpointOffsetReseter = checkpointOffsets.entrySet().stream()
        .anyMatch(offset -> offset.getValue() < earliestOffsets.get(offset.getKey()));
    return checkpointOffsetReseter ? earliestOffsets : checkpointOffsets;
  }

  private Long delayOffsetCalculation(Option<String> lastCheckpointStr, Set<TopicPartition> topicPartitions, KafkaConsumer consumer) {
    Long delayCount = 0L;
    Map<TopicPartition, Long> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
    Map<TopicPartition, Long> lastOffsets = consumer.endOffsets(topicPartitions);

    for (Map.Entry<TopicPartition, Long> entry : lastOffsets.entrySet()) {
      Long offect = checkpointOffsets.getOrDefault(entry.getKey(), 0L);
      delayCount += entry.getValue() - offect > 0 ? entry.getValue() - offect : 0L;
    }
    return delayCount;
  }

  /**
   * Check if topic exists.
   * @param consumer kafka consumer
   * @return
   */
  public boolean checkTopicExists(KafkaConsumer consumer)  {
    Map<String, List<PartitionInfo>> result = consumer.listTopics();
    return result.containsKey(topicName);
  }

  private boolean checkTopicCheckpoint(Option<String> lastCheckpointStr) {
    Matcher matcher = pattern.matcher(lastCheckpointStr.get());
    return matcher.matches();
  }

  public String getTopicName() {
    return topicName;
  }

  public HashMap<String, Object> getKafkaParams() {
    return kafkaParams;
  }
}
