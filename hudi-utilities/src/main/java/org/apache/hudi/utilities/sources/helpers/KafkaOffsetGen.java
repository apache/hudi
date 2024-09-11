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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.exception.HoodieStreamerException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.sources.AvroKafkaSource;

import org.apache.hudi.utilities.sources.HoodieRetryingKafkaConsumer;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.checkRequiredProperties;
import static org.apache.hudi.common.util.ConfigUtils.containsConfigProperty;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE_TIMESTAMP;
import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils.checkTopicCheckpoint;

/**
 * Source to read data from Kafka, incrementally.
 */
public class KafkaOffsetGen {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetGen.class);
  private static final String METRIC_NAME_KAFKA_DELAY_COUNT = "kafkaDelayCount";
  private static final Comparator<OffsetRange> SORT_BY_PARTITION = Comparator.comparing(OffsetRange::partition);

  public static class CheckpointUtils {
    /**
     * kafka checkpoint Pattern.
     * Format: topic_name,partition_num:offset,partition_num:offset,....
     */
    private static final Pattern PATTERN = Pattern.compile(".*,.*:.*");

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
     * @param minPartitions minimum partitions used for
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

      // choose the actualNumEvents with min(totalEvents, numEvents)
      long actualNumEvents = Math.min(totalNewMessages(ranges), numEvents);
      minPartitions = Math.max(minPartitions, toOffsetMap.size());
      // Each OffsetRange computed will have maximum of eventsPerPartition,
      // this ensures all ranges are evenly distributed and there's no skew in one particular range.
      long eventsPerPartition = Math.max(1L, actualNumEvents / minPartitions);
      long allocatedEvents = 0;
      Map<TopicPartition, List<OffsetRange>> finalRanges = new HashMap<>();
      Map<TopicPartition, Long> partitionToAllocatedOffset = new HashMap<>();
      // keep going until we have events to allocate.
      while (allocatedEvents < actualNumEvents) {
        // Allocate the remaining events in round-robin fashion.
        for (OffsetRange range : ranges) {
          // if we have already allocated required no of events, exit
          if (allocatedEvents == actualNumEvents) {
            break;
          }
          // Compute startOffset.
          long startOffset = range.fromOffset();
          if (partitionToAllocatedOffset.containsKey(range.topicPartition())) {
            startOffset = partitionToAllocatedOffset.get(range.topicPartition());
          }
          // for last bucket, we may not have full eventsPerPartition msgs.
          long eventsForThisPartition = Math.min(eventsPerPartition, (actualNumEvents - allocatedEvents));
          // Compute toOffset.
          long toOffset = -1L;
          if (startOffset + eventsForThisPartition > startOffset) {
            toOffset = Math.min(range.untilOffset(), startOffset + eventsForThisPartition);
          } else {
            // handling Long overflow
            toOffset = range.untilOffset();
          }
          allocatedEvents += toOffset - startOffset;
          OffsetRange thisRange = OffsetRange.create(range.topicPartition(), startOffset, toOffset);
          // Add the offsetRange(startOffset,toOffset) to finalRanges.
          if (!finalRanges.containsKey(range.topicPartition())) {
            finalRanges.put(range.topicPartition(), new ArrayList<>(Collections.singleton(thisRange)));
            partitionToAllocatedOffset.put(range.topicPartition(), thisRange.untilOffset());
          } else if (toOffset > startOffset) {
            finalRanges.get(range.topicPartition()).add(thisRange);
            partitionToAllocatedOffset.put(range.topicPartition(), thisRange.untilOffset());
          }
        }
      }
      // We need to ensure every partition is part of returned offset ranges even if we are not consuming any new msgs (for instance, if its already caught up).
      // as this will be tracked as the checkpoint, we need to ensure all partitions are part of final ranges.
      Map<TopicPartition, List<OffsetRange>> missedRanges = fromOffsetMap.entrySet().stream()
              .filter((kv) -> !finalRanges.containsKey(kv.getKey()))
              .map((kv) -> Pair.of(kv.getKey(), Collections.singletonList(
                      OffsetRange.create(kv.getKey(), kv.getValue(), kv.getValue()))))
              .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      finalRanges.putAll(missedRanges);

      OffsetRange[] sortedRangeArray = finalRanges.values().stream().flatMap(Collection::stream)
          .sorted(SORT_BY_PARTITION).toArray(OffsetRange[]::new);
      if (actualNumEvents == 0) {
        // We return the same ranges back in case of 0 events for checkpoint computation.
        sortedRangeArray = ranges;
      }
      LOG.info("final ranges {}", Arrays.toString(sortedRangeArray));
      return sortedRangeArray;
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

    public static boolean checkTopicCheckpoint(Option<String> lastCheckpointStr) {
      Matcher matcher = PATTERN.matcher(lastCheckpointStr.get());
      return matcher.matches();
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
    checkRequiredConfigProperties(props, Collections.singletonList(KafkaSourceConfig.KAFKA_TOPIC_NAME));
    topicName = getStringWithAltKeys(props, KafkaSourceConfig.KAFKA_TOPIC_NAME);
    kafkaCheckpointType = getStringWithAltKeys(props, KafkaSourceConfig.KAFKA_CHECKPOINT_TYPE, true);
    String kafkaAutoResetOffsetsStr = props.getString(KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.key(), KafkaSourceConfig.KAFKA_AUTO_OFFSET_RESET.defaultValue().name().toLowerCase());
    boolean found = false;
    for (KafkaSourceConfig.KafkaResetOffsetStrategies entry : KafkaSourceConfig.KafkaResetOffsetStrategies.values()) {
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
    // Come up with final set of OffsetRanges to read (account for new partitions, limit number of events)
    long maxEventsToReadFromKafka = getLongWithAltKeys(props, KafkaSourceConfig.MAX_EVENTS_FROM_KAFKA_SOURCE);

    long numEvents;
    if (sourceLimit == Long.MAX_VALUE) {
      numEvents = maxEventsToReadFromKafka;
      LOG.info("SourceLimit not configured, set numEvents to default value : {}", maxEventsToReadFromKafka);
    } else {
      numEvents = sourceLimit;
    }

    long minPartitions = getLongWithAltKeys(props, KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS);
    LOG.info("getNextOffsetRanges set config {} to {}", KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS.key(), minPartitions);

    return getNextOffsetRanges(lastCheckpointStr, numEvents, minPartitions, metrics);
  }

  public OffsetRange[] getNextOffsetRanges(Option<String> lastCheckpointStr, long numEvents, long minPartitions, HoodieIngestionMetrics metrics) {
    // Obtain current metadata for the topic
    Map<TopicPartition, Long> fromOffsets;
    Map<TopicPartition, Long> toOffsets;
    try (KafkaConsumer consumer = new HoodieRetryingKafkaConsumer(props, kafkaParams)) {
      if (!checkTopicExists(consumer)) {
        throw new HoodieException("Kafka topic:" + topicName + " does not exist");
      }
      List<PartitionInfo> partitionInfoList = fetchPartitionInfos(consumer, topicName);
      Set<TopicPartition> topicPartitions = partitionInfoList.stream()
              .map(x -> new TopicPartition(x.topic(), x.partition())).collect(Collectors.toSet());

      if (KAFKA_CHECKPOINT_TYPE_TIMESTAMP.equalsIgnoreCase(kafkaCheckpointType) && isValidTimestampCheckpointType(lastCheckpointStr)) {
        lastCheckpointStr = getOffsetsByTimestamp(consumer, partitionInfoList, topicPartitions, topicName, Long.parseLong(lastCheckpointStr.get()));
      } else if (KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET.equalsIgnoreCase(kafkaCheckpointType) && partitionInfoList.size() != 1) {
        throw new HoodieException("Kafka topic " + topicName + " has " + partitionInfoList.size()
            + " partitions (more than 1). single_offset checkpoint type is not applicable.");
      } else if (KAFKA_CHECKPOINT_TYPE_SINGLE_OFFSET.equalsIgnoreCase(kafkaCheckpointType)
          && partitionInfoList.size() == 1 && isValidOffsetCheckpointType(lastCheckpointStr)) {
        lastCheckpointStr = Option.of(topicName + ",0:" + lastCheckpointStr.get());
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
    return CheckpointUtils.computeOffsetRanges(fromOffsets, toOffsets, numEvents, minPartitions);
  }
  
  /**
   * Fetch partition infos for given topic.
   *
   * @param consumer
   * @param topicName
   */
  private List<PartitionInfo> fetchPartitionInfos(KafkaConsumer consumer, String topicName) {
    if (containsConfigProperty(this.props, KafkaSourceConfig.KAFKA_FETCH_PARTITION_TIME_OUT)) {
      LOG.warn("{} is deprecated and is not taking effect anymore. Use {}, {} and {} for setting up retrying configuration of KafkaConsumer",
          KafkaSourceConfig.KAFKA_FETCH_PARTITION_TIME_OUT.key(), KafkaSourceConfig.INITIAL_RETRY_INTERVAL_MS.key(),
          KafkaSourceConfig.MAX_RETRY_INTERVAL_MS.key(), KafkaSourceConfig.MAX_RETRY_COUNT.key());
    }
    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
    if (partitionInfos == null) {
      throw new HoodieStreamerException(String.format("Can not find metadata for topic %s from kafka cluster", topicName));
    }
    return partitionInfos;
  }

  /**
   * Fetch checkpoint offsets for each partition.
   *
   * @param consumer          instance of {@link KafkaConsumer} to fetch offsets from.
   * @param lastCheckpointStr last checkpoint string.
   * @param topicPartitions   set of topic partitions.
   * @return a map of Topic partitions to offsets.
   */
  private Map<TopicPartition, Long> fetchValidOffsets(KafkaConsumer consumer,
                                                      Option<String> lastCheckpointStr, Set<TopicPartition> topicPartitions) {
    Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(topicPartitions);
    Map<TopicPartition, Long> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
    List<TopicPartition> outOfBoundPartitionList = checkpointOffsets.entrySet().stream()
        .filter(offset -> offset.getValue() < earliestOffsets.get(offset.getKey()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    boolean isCheckpointOutOfBounds = !outOfBoundPartitionList.isEmpty();

    if (isCheckpointOutOfBounds) {
      String outOfBoundOffsets = outOfBoundPartitionList.stream()
          .map(p -> p.toString() + ":{checkpoint=" + checkpointOffsets.get(p)
              + ",earliestOffset=" + earliestOffsets.get(p) + "}")
          .collect(Collectors.joining(","));
      String message = "Some data may have been lost because they are not available in Kafka any more;"
          + " either the data was aged out by Kafka or the topic may have been deleted before all the data in the topic was processed. "
          + "Kafka partitions that have out-of-bound checkpoints: " + outOfBoundOffsets + " .";

      if (getBooleanWithAltKeys(this.props, KafkaSourceConfig.ENABLE_FAIL_ON_DATA_LOSS)) {
        throw new HoodieStreamerException(message);
      } else {
        LOG.warn("{} If you want Hudi Streamer to fail on such cases, set \"{}\" to \"true\".", message, KafkaSourceConfig.ENABLE_FAIL_ON_DATA_LOSS.key());
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

  /**
   * Check if checkpoint is a single offset
   * @param lastCheckpointStr
   * @return
   */
  private Boolean isValidOffsetCheckpointType(Option<String> lastCheckpointStr) {
    if (!lastCheckpointStr.isPresent()) {
      return false;
    }
    try {
      Long.parseUnsignedLong(lastCheckpointStr.get());
      return true;
    } catch (NumberFormatException ex) {
      LOG.warn("Checkpoint type is set to single_offset, but provided value of checkpoint=\"{}\" is not a valid number", lastCheckpointStr.get());
      return false;
    }
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

    StringBuilder sb = new StringBuilder(topicName);
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> map : offsetAndTimestamp.entrySet()) {
      if (map.getValue() != null) {
        sb.append(",").append(map.getKey().partition()).append(":").append(map.getValue().offset());
      } else {
        sb.append(",").append(map.getKey().partition()).append(":").append(earliestOffsets.get(map.getKey()));
      }
    }
    return Option.of(sb.toString());
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

  public String getTopicName() {
    return topicName;
  }

  public Map<String, Object> getKafkaParams() {
    return kafkaParams;
  }

  public static Map<String, Object> excludeHoodieConfigs(TypedProperties props) {
    Map<String, Object> kafkaParams = new HashMap<>();
    props.keySet().stream().filter(prop -> {
      // In order to prevent printing unnecessary warn logs, here filter out the hoodie
      // configuration items before passing to kafkaParams
      return !prop.toString().startsWith("hoodie.")
          // We need to pass some properties to kafka client so that KafkaAvroSchemaDeserializer can use it
          || prop.toString().startsWith(AvroKafkaSource.KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX)
          || prop.toString().startsWith(AvroKafkaSource.OLD_KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX);
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
    checkRequiredProperties(props, Collections.singletonList(ConsumerConfig.GROUP_ID_CONFIG));
    Map<TopicPartition, Long> offsetMap = CheckpointUtils.strToOffsets(checkpointStr);
    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>(offsetMap.size());
    try (KafkaConsumer consumer = new HoodieRetryingKafkaConsumer(props, kafkaParams)) {
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
