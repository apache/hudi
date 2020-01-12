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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Source to read data from Kafka, incrementally.
 */
public class KafkaOffsetGen {

  private static Logger log = LogManager.getLogger(KafkaOffsetGen.class);

  public static class CheckpointUtils {

    /**
     * Reconstruct checkpoint from string.
     */
    public static HashMap<TopicPartition, Long> strToOffsets(String checkpointStr) {
      HashMap<TopicPartition, Long> offsetMap = new HashMap<>();
      if (checkpointStr.length() == 0) {
        return offsetMap;
      }
      String[] splits = checkpointStr.split(",");
      String topic = splits[0];
      for (int i = 1; i < splits.length; i++) {
        String[] subSplits = splits[i].split(":");
        offsetMap.put(new TopicPartition(topic, Integer.parseInt(subSplits[0])),
                  Long.parseLong(subSplits[1]));
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
    public static OffsetRange[] computeOffsetRanges(HashMap<TopicPartition, Long> fromOffsetMap,
        HashMap<TopicPartition, Long> toOffsetMap, long numEvents) {

      Comparator<OffsetRange> byPartition = Comparator.comparing(OffsetRange::partition);

      // Create initial offset ranges for each 'to' partition, with from = to offsets.
      OffsetRange[] ranges = new OffsetRange[toOffsetMap.size()];
      toOffsetMap.entrySet().stream().map(e -> {
        TopicPartition tp = e.getKey();
        long fromOffset = fromOffsetMap.getOrDefault(tp, 0L);
        return OffsetRange.create(tp, fromOffset, fromOffset);
      }).sorted(byPartition).collect(Collectors.toList()).toArray(ranges);

      long allocedEvents = 0;
      java.util.Set<Integer> exhaustedPartitions = new HashSet<>();
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
    latest, earliest
  }

  /**
   * Configs to be passed for this source. All standard Kafka consumer configs are also respected
   */
  public static class Config {

    private static final String KAFKA_TOPIC_NAME = "hoodie.deltastreamer.source.kafka.topic";
    private static final String MAX_EVENTS_FROM_KAFKA_SOURCE_PROP = "hoodie.deltastreamer.kafka.source.maxEvents";
    private static final KafkaResetOffsetStrategies DEFAULT_AUTO_RESET_OFFSET = KafkaResetOffsetStrategies.latest;
    public static final long DEFAULT_MAX_EVENTS_FROM_KAFKA_SOURCE = 5000000;
    public static long maxEventsFromKafkaSource = DEFAULT_MAX_EVENTS_FROM_KAFKA_SOURCE;
  }

  private final HashMap<String, Object> kafkaParams;
  private final TypedProperties props;
  protected final String topicName;

  public KafkaOffsetGen(TypedProperties props) {
    this.props = props;
    kafkaParams = new HashMap<String, Object>();
    for (Object prop : props.keySet()) {
      kafkaParams.put(prop.toString(), props.getString(prop.toString()));
    }
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.KAFKA_TOPIC_NAME));
    topicName = props.getString(Config.KAFKA_TOPIC_NAME);
  }

  public HashMap<String, Object> getKafkaProperties() {
    final HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
    for (Object prop : props.keySet()) {
      kafkaParams.put(prop.toString(), props.get(prop));
    }
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(new Random().nextInt(10000)));
    kafkaParams.put("auto.offset.reset", KafkaResetOffsetStrategies.earliest.name());
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    return kafkaParams;
  }

  public OffsetRange[] getNextOffsetRanges(Option<String> lastCheckpointStr, long sourceLimit) {

    // Obtain current metadata for the topic
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getKafkaProperties());

    List<PartitionInfo> partInfo = consumer.partitionsFor(topicName);
    Set<TopicPartition> topicPartitions = new HashSet<>();
    for (PartitionInfo partitionInfo: partInfo) {
      topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
    }

    // Determine the offset ranges to read from
    HashMap<TopicPartition, Long> fromOffsets;
    HashMap<TopicPartition, Long> checkpointOffsets;
    if (lastCheckpointStr.isPresent()) {
      fromOffsets = checkupValidOffsets(consumer, lastCheckpointStr, topicPartitions);
    } else {
      KafkaResetOffsetStrategies autoResetValue = KafkaResetOffsetStrategies.valueOf((String)getKafkaProperties().get("auto.offset.reset"));
      switch (autoResetValue) {
        case earliest:
          fromOffsets =
                      new HashMap(consumer.beginningOffsets(topicPartitions));
          break;
        case latest:
          fromOffsets =
                      new HashMap(consumer.endOffsets(topicPartitions));
          break;
        default:
          throw new HoodieNotSupportedException("Auto reset value must be one of 'smallest' or 'largest' ");
      }
    }

    // Obtain the latest offsets.
    java.util.Map<TopicPartition, Long> toOffsets = consumer.endOffsets(topicPartitions);

    // Come up with final set of OffsetRanges to read (account for new partitions, limit number of events)
    long maxEventsToReadFromKafka = props.getLong(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP,
        Config.maxEventsFromKafkaSource);
    maxEventsToReadFromKafka = (maxEventsToReadFromKafka == Long.MAX_VALUE || maxEventsToReadFromKafka == Integer.MAX_VALUE)
        ? Config.maxEventsFromKafkaSource : maxEventsToReadFromKafka;
    long numEvents = sourceLimit == Long.MAX_VALUE ? maxEventsToReadFromKafka : sourceLimit;
    OffsetRange[] offsetRanges = CheckpointUtils.computeOffsetRanges(fromOffsets, new HashMap(toOffsets), numEvents);

    return offsetRanges;
  }

  // check up checkpoint offsets is valid or not, if true, return checkpoint offsets,
  // else return earliest offsets
  private HashMap<TopicPartition, Long> checkupValidOffsets(KafkaConsumer consumer, Option<String> lastCheckpointStr, java.util.Set<TopicPartition> topicPartitions) {
    HashMap<TopicPartition, Long> checkpointOffsets =
              CheckpointUtils.strToOffsets(lastCheckpointStr.get());
    HashMap<TopicPartition, Long> earliestOffsets =
            new HashMap(consumer.beginningOffsets(topicPartitions));
    boolean checkpointOffsetReseter = checkpointOffsets.entrySet().stream()
        .anyMatch(offset -> offset.getValue() < earliestOffsets.get(offset.getKey()));
    return checkpointOffsetReseter ? earliestOffsets : checkpointOffsets;
  }

  public String getTopicName() {
    return topicName;
  }

  public HashMap<String, Object> getKafkaParams() {
    return getKafkaProperties();
  }
}
