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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.exception.HoodieStreamerException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.AvroKafkaSource;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Source to read data from Kafka, incrementally.
 */
public class KafkaOffsetGen {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetGen.class);
  private static final String METRIC_NAME_KAFKA_DELAY_COUNT = "kafkaDelayCount";
  private static final Comparator<OffsetRange> SORT_BY_PARTITION = Comparator.comparing(OffsetRange::partition);

  public static final String KAFKA_CHECKPOINT_TYPE_TIMESTAMP = "timestamp";

  /**
   * kafka checkpoint Pattern.
   * Format: topic_name,partition_num:offset,partition_num:offset,....
   */
  private final Pattern pattern = Pattern.compile(".*,.*:.*");

  public static class CheckpointUtils {

    /**
     * Reconstruct checkpoint from timeline.
     */
    public static Map<TopicPartition, Long> strToOffsets(String checkpointStr) {
      Map<TopicPartition, Long> offsetMap = new HashMap<>();
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
      // merge the ranges by partition to maintain one offset range map to one topic partition.
      ranges = mergeRangesByTopicPartition(ranges);
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
                                                    Map<TopicPartition, Long> toOffsetMap,
                                                    long numEvents,
                                                    long minPartitions) {
      // Create initial offset ranges for each 'to' partition, with default from = 0 offsets.
      OffsetRange[] ranges = toOffsetMap.keySet().stream().map(tp -> {
        long fromOffset = fromOffsetMap.getOrDefault(tp, 0L);
        return OffsetRange.create(tp, fromOffset, toOffsetMap.get(tp));
      })
          .sorted(SORT_BY_PARTITION)
          .collect(Collectors.toList())
          .toArray(new OffsetRange[toOffsetMap.size()]);
      LOG.debug("numEvents {}, minPartitions {}, ranges {}", numEvents, minPartitions, ranges);

      boolean needSplitToMinPartitions = minPartitions > toOffsetMap.size();
      long totalEvents = totalNewMessages(ranges);
      long allocedEvents = 0;
      Set<Integer> exhaustedPartitions = new HashSet<>();
      List<OffsetRange> finalRanges = new ArrayList<>();
      // choose the actualNumEvents with min(totalEvents, numEvents)
      long actualNumEvents = Math.min(totalEvents, numEvents);

      // keep going until we have events to allocate and partitions still not exhausted.
      while (allocedEvents < numEvents && exhaustedPartitions.size() < toOffsetMap.size()) {
        // Allocate the remaining events to non-exhausted partitions, in round robin fashion
        Set<Integer> allocatedPartitionsThisLoop = new HashSet<>(exhaustedPartitions);
        for (int i = 0; i < ranges.length; i++) {
          long remainingEvents = actualNumEvents - allocedEvents;
          long remainingPartitions = toOffsetMap.size() - allocatedPartitionsThisLoop.size();
          // if need tp split into minPartitions, recalculate the remainingPartitions
          if (needSplitToMinPartitions) {
            remainingPartitions = minPartitions - finalRanges.size();
          }
          long eventsPerPartition = (long) Math.ceil((1.0 * remainingEvents) / remainingPartitions);

          OffsetRange range = ranges[i];
          if (exhaustedPartitions.contains(range.partition())) {
            continue;
          }

          long toOffset = Math.min(range.untilOffset(), range.fromOffset() + eventsPerPartition);
          if (toOffset == range.untilOffset()) {
            exhaustedPartitions.add(range.partition());
          }
          allocedEvents += toOffset - range.fromOffset();
          // We need recompute toOffset if allocedEvents larger than actualNumEvents.
          if (allocedEvents > actualNumEvents) {
            long offsetsToAdd = Math.min(eventsPerPartition, (actualNumEvents - allocedEvents));
            toOffset = Math.min(range.untilOffset(), toOffset + offsetsToAdd);
          }
          OffsetRange thisRange = OffsetRange.create(range.topicPartition(), range.fromOffset(), toOffset);
          finalRanges.add(thisRange);
          ranges[i] = OffsetRange.create(range.topicPartition(), range.fromOffset() + thisRange.count(), range.untilOffset());
          allocatedPartitionsThisLoop.add(range.partition());
        }
      }

      if (!needSplitToMinPartitions) {
        LOG.debug("final ranges merged by topic partition {}", Arrays.toString(mergeRangesByTopicPartition(finalRanges.toArray(new OffsetRange[0]))));
        return mergeRangesByTopicPartition(finalRanges.toArray(new OffsetRange[0]));
      }
      finalRanges.sort(SORT_BY_PARTITION);
      LOG.debug("final ranges {}", Arrays.toString(finalRanges.toArray(new OffsetRange[0])));
      return finalRanges.toArray(new OffsetRange[0]);
    }

    /**
     * Merge ranges by topic partition, because we need to maintain the checkpoint with one offset range per topic partition.
     * @param oldRanges to merge
     * @return ranges merged by partition
     */
    public static OffsetRange[] mergeRangesByTopicPartition(OffsetRange[] oldRanges) {
      List<OffsetRange> newRanges = new ArrayList<>();
      Map<TopicPartition, List<OffsetRange>> tpOffsets = Arrays.stream(oldRanges).collect(Collectors.groupingBy(OffsetRange::topicPartition));
      for (Map.Entry<TopicPartition, List<OffsetRange>> entry : tpOffsets.entrySet()) {
        long from = entry.getValue().stream().map(OffsetRange::fromOffset).min(Long::compare).get();
        long until = entry.getValue().stream().map(OffsetRange::untilOffset).max(Long::compare).get();
        newRanges.add(OffsetRange.create(entry.getKey(), from, until));
      }
      // make sure the result ranges is order by partition
      newRanges.sort(SORT_BY_PARTITION);
      return newRanges.toArray(new OffsetRange[0]);
    }

    public static long totalNewMessages(OffsetRange[] ranges) {
      return Arrays.stream(ranges).mapToLong(OffsetRange::count).sum();
    }
  }

  private final Map<String, Object> kafkaParams;
  private final TypedProperties props;
  protected final String topicName;
  private KafkaSourceConfig.KafkaResetOffsetStrategies autoResetValue;
  private final String kafkaCheckpointType;

  public KafkaOffsetGen(TypedProperties props) {
    this.props = props;
    kafkaParams = excludeHoodieConfigs(props);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(KafkaSourceConfig.KAFKA_TOPIC_NAME.key()));
    topicName = props.getString(KafkaSourceConfig.KAFKA_TOPIC_NAME.key());
    kafkaCheckpointType = props.getString(KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE.key(), KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE.defaultValue());
    String kafkaAutoResetOffsetsStr = props.getString(KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.key(), KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.defaultValue().name().toLowerCase());
    boolean found = false;
    for (KafkaSourceConfig.KafkaResetOffsetStrategies entry: KafkaSourceConfig.KafkaResetOffsetStrategies.values()) {
      if (entry.name().toLowerCase().equals(kafkaAutoResetOffsetsStr)) {
        found = true;
        autoResetValue = entry;
        break;
      }
    }
    if (!found) {
      throw new HoodieStreamerException(KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.key() + " config set to unknown value " + kafkaAutoResetOffsetsStr);
    }
    if (autoResetValue.equals(KafkaSourceConfig.KafkaResetOffsetStrategies.GROUP)) {
      this.kafkaParams.put(KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.key(), KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.defaultValue().name().toLowerCase());
    }
  }

  public OffsetRange[] getNextOffsetRanges(Option<String> lastCheckpointStr, long sourceLimit, HoodieIngestionMetrics metrics) {

    // Obtain current metadata for the topic
    Map<TopicPartition, Long> fromOffsets;
    Map<TopicPartition, Long> toOffsets;
    try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
      if (!checkTopicExists(consumer)) {
        throw new HoodieException("Kafka topic:" + topicName + " does not exist");
      }
      List<PartitionInfo> partitionInfoList = fetchPartitionInfos(consumer, topicName);
      Set<TopicPartition> topicPartitions = partitionInfoList.stream()
              .map(x -> new TopicPartition(x.topic(), x.partition())).collect(Collectors.toSet());

      if (KAFKA_CHECKPOINT_TYPE_TIMESTAMP.equals(kafkaCheckpointType) && isValidTimestampCheckpointType(lastCheckpointStr)) {
        lastCheckpointStr = getOffsetsByTimestamp(consumer, partitionInfoList, topicPartitions, topicName, Long.parseLong(lastCheckpointStr.get()));
      }
      // Determine the offset ranges to read from
      if (lastCheckpointStr.isPresent() && !lastCheckpointStr.get().isEmpty() && checkTopicCheckpoint(lastCheckpointStr)) {
        fromOffsets = fetchValidOffsets(consumer, lastCheckpointStr, topicPartitions);
        metrics.updateStreamerSourceDelayCount(METRIC_NAME_KAFKA_DELAY_COUNT, delayOffsetCalculation(lastCheckpointStr, topicPartitions, consumer));
      } else {
        switch (autoResetValue) {
          case EARLIEST:
            fromOffsets = consumer.beginningOffsets(topicPartitions);
            break;
          case LATEST:
            fromOffsets = consumer.endOffsets(topicPartitions);
            break;
          case GROUP:
            fromOffsets = getGroupOffsets(consumer, topicPartitions);
            break;
          default:
            throw new HoodieNotSupportedException("Auto reset value must be one of 'earliest' or 'latest' or 'group' ");
        }
      }

      // Obtain the latest offsets.
      toOffsets = consumer.endOffsets(topicPartitions);
    }

    // Come up with final set of OffsetRanges to read (account for new partitions, limit number of events)
    long maxEventsToReadFromKafka = props.getLong(KafkaSourceConfig.MAX_EVENTS_FROM_KAFKA_SOURCE.key(),
        KafkaSourceConfig.MAX_EVENTS_FROM_KAFKA_SOURCE.defaultValue());

    long numEvents;
    if (sourceLimit == Long.MAX_VALUE) {
      numEvents = maxEventsToReadFromKafka;
      LOG.info("SourceLimit not configured, set numEvents to default value : " + maxEventsToReadFromKafka);
    } else {
      numEvents = sourceLimit;
    }

    // TODO(HUDI-4625) remove
    if (numEvents < toOffsets.size()) {
      throw new HoodieException("sourceLimit should not be less than the number of kafka partitions");
    }

    long minPartitions = props.getLong(KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key(),
            KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.defaultValue());
    LOG.info("getNextOffsetRanges set config " + KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key() + " to " + minPartitions);

    return CheckpointUtils.computeOffsetRanges(fromOffsets, toOffsets, numEvents, minPartitions);
  }

  /**
   * Fetch partition infos for given topic.
   *
   * @param consumer
   * @param topicName
   */
  private List<PartitionInfo> fetchPartitionInfos(KafkaConsumer consumer, String topicName) {
    long timeout = this.props.getLong(KafkaSourceConfig.KAFKA_FETCH_PARTITION_TIME_OUT.key(), KafkaSourceConfig.KAFKA_FETCH_PARTITION_TIME_OUT.defaultValue());
    long start = System.currentTimeMillis();

    List<PartitionInfo> partitionInfos;
    do {
      // TODO(HUDI-4625) cleanup, introduce retrying client
      partitionInfos = consumer.partitionsFor(topicName);
      try {
        if (partitionInfos == null) {
          TimeUnit.SECONDS.sleep(10);
        }
      } catch (InterruptedException e) {
        LOG.error("Sleep failed while fetching partitions");
      }
    } while (partitionInfos == null && (System.currentTimeMillis() <= (start + timeout)));

    if (partitionInfos == null) {
      throw new HoodieStreamerException(String.format("Can not find metadata for topic %s from kafka cluster", topicName));
    }
    return partitionInfos;
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
    boolean isCheckpointOutOfBounds = checkpointOffsets.entrySet().stream()
        .anyMatch(offset -> offset.getValue() < earliestOffsets.get(offset.getKey()));
    if (isCheckpointOutOfBounds) {
      if (this.props.getBoolean(KafkaSourceConfig.ENABLE_FAIL_ON_DATA_LOSS.key(), KafkaSourceConfig.ENABLE_FAIL_ON_DATA_LOSS.defaultValue())) {
        throw new HoodieStreamerException("Some data may have been lost because they are not available in Kafka any more;"
            + " either the data was aged out by Kafka or the topic may have been deleted before all the data in the topic was processed.");
      } else {
        LOG.warn("Some data may have been lost because they are not available in Kafka any more;"
            + " either the data was aged out by Kafka or the topic may have been deleted before all the data in the topic was processed."
            + " If you want Hudi Streamer to fail on such cases, set \"" + KafkaSourceConfig.ENABLE_FAIL_ON_DATA_LOSS.key() + "\" to \"true\".");
      }
    }
    return isCheckpointOutOfBounds ? earliestOffsets : checkpointOffsets;
  }

  /**
   * Check if the checkpoint is a timestamp.
   * @param lastCheckpointStr
   * @return
   */
  private Boolean isValidTimestampCheckpointType(Option<String> lastCheckpointStr) {
    if (!lastCheckpointStr.isPresent()) {
      return false;
    }
    Pattern pattern = Pattern.compile("[-+]?[0-9]+(\\.[0-9]+)?");
    Matcher isNum = pattern.matcher(lastCheckpointStr.get());
    return isNum.matches() && (lastCheckpointStr.get().length() == 13 || lastCheckpointStr.get().length() == 10);
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
   * Get the checkpoint by timestamp.
   * This method returns the checkpoint format based on the timestamp.
   * example:
   * 1. input: timestamp, etc.
   * 2. output: topicName,partition_num_0:100,partition_num_1:101,partition_num_2:102.
   *
   * @param consumer
   * @param topicName
   * @param timestamp
   * @return
   */
  private Option<String> getOffsetsByTimestamp(KafkaConsumer consumer, List<PartitionInfo> partitionInfoList, Set<TopicPartition> topicPartitions,
                                               String topicName, Long timestamp) {

    Map<TopicPartition, Long> topicPartitionsTimestamp = partitionInfoList.stream()
                                                    .map(x -> new TopicPartition(x.topic(), x.partition()))
                                                    .collect(Collectors.toMap(Function.identity(), x -> timestamp));

    Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(topicPartitions);
    Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestamp = consumer.offsetsForTimes(topicPartitionsTimestamp);

    StringBuilder sb = new StringBuilder();
    sb.append(topicName + ",");
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> map : offsetAndTimestamp.entrySet()) {
      if (map.getValue() != null) {
        sb.append(map.getKey().partition()).append(":").append(map.getValue().offset()).append(",");
      } else {
        sb.append(map.getKey().partition()).append(":").append(earliestOffsets.get(map.getKey())).append(",");
      }
    }
    return Option.of(sb.deleteCharAt(sb.length() - 1).toString());
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

  public Map<String, Object> getKafkaParams() {
    return kafkaParams;
  }

  private Map<String, Object> excludeHoodieConfigs(TypedProperties props) {
    Map<String, Object> kafkaParams = new HashMap<>();
    props.keySet().stream().filter(prop -> {
      // In order to prevent printing unnecessary warn logs, here filter out the hoodie
      // configuration items before passing to kafkaParams
      return !prop.toString().startsWith("hoodie.")
              // We need to pass some properties to kafka client so that KafkaAvroSchemaDeserializer can use it
              || prop.toString().startsWith(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX);
    }).forEach(prop -> {
      kafkaParams.put(prop.toString(), props.get(prop.toString()));
    });
    return kafkaParams;
  }

  /**
   * Commit offsets to Kafka only after hoodie commit is successful.
   * @param checkpointStr checkpoint string containing offsets.
   */
  public void commitOffsetToKafka(String checkpointStr) {
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(ConsumerConfig.GROUP_ID_CONFIG));
    Map<TopicPartition, Long> offsetMap = CheckpointUtils.strToOffsets(checkpointStr);
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>(offsetMap.size());
    try (KafkaConsumer consumer = new KafkaConsumer(kafkaParams)) {
      offsetMap.forEach((topicPartition, offset) -> offsetAndMetadataMap.put(topicPartition, new OffsetAndMetadata(offset)));
      consumer.commitSync(offsetAndMetadataMap);
    } catch (CommitFailedException | TimeoutException e) {
      LOG.warn("Committing offsets to Kafka failed, this does not impact processing of records", e);
    }
  }

  private Map<TopicPartition, Long> getGroupOffsets(KafkaConsumer consumer, Set<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> fromOffsets = new HashMap<>();
    for (TopicPartition topicPartition : topicPartitions) {
      OffsetAndMetadata committedOffsetAndMetadata = consumer.committed(topicPartition);
      if (committedOffsetAndMetadata != null) {
        fromOffsets.put(topicPartition, committedOffsetAndMetadata.offset());
      } else {
        LOG.warn("There are no commits associated with this consumer group, starting to consume from latest offset");
        fromOffsets = consumer.endOffsets(topicPartitions);
        break;
      }
    }
    return fromOffsets;
  }
}
