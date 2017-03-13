/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.exception.HoodieNotSupportedException;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.SchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import kafka.common.TopicAndPartition;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import kafka.serializer.DefaultDecoder;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.StringBuilder;
import scala.util.Either;


/**
 * Source to read data from Kafka, incrementally
 */
public class KafkaSource extends Source {

    private static volatile Logger log = LogManager.getLogger(KafkaSource.class);


    static class CheckpointUtils {

        /**
         * Reconstruct checkpoint from string.
         *
         * @param checkpointStr
         * @return
         */
        public static HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> strToOffsets(String checkpointStr) {
            HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> offsetMap = new HashMap<>();
            String[] splits = checkpointStr.split(",");
            String topic = splits[0];
            for (int i = 1; i < splits.length; i++) {
                String[] subSplits = splits[i].split(":");
                offsetMap.put(new TopicAndPartition(topic, Integer.parseInt(subSplits[0])),
                        new KafkaCluster.LeaderOffset("", -1, Long.parseLong(subSplits[1])));
            }
            return offsetMap;
        }

        /**
         * String representation of checkpoint
         *
         * Format:
         * topic1,0:offset0,1:offset1,2:offset2, .....
         *
         * @param offsetMap
         * @return
         */
        public static String offsetsToStr(HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> offsetMap) {
            StringBuilder sb = new StringBuilder();
            // atleast 1 partition will be present.
            sb.append(offsetMap.entrySet().stream().findFirst().get().getKey().topic() + ",");
            sb.append(offsetMap.entrySet().stream()
                    .map(e -> String.format("%s:%d",e.getKey().partition(), e.getValue().offset()))
                    .collect(Collectors.joining(",")));
            return sb.toString();
        }

        public static OffsetRange[] computeOffsetRanges(HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> fromOffsetMap,
                                                        HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> toOffsetMap) {
            Comparator<OffsetRange> byPartition = (OffsetRange o1, OffsetRange o2) -> {
                return Integer.valueOf(o1.partition()).compareTo(Integer.valueOf(o2.partition()));
            };
            List<OffsetRange> offsetRanges = toOffsetMap.entrySet().stream().map(e -> {
                TopicAndPartition tp = e.getKey();
                long fromOffset = -1;
                if (fromOffsetMap.containsKey(tp)){
                    fromOffset = fromOffsetMap.get(tp).offset();
                }
                return OffsetRange.create(tp, fromOffset, e.getValue().offset());
            }).sorted(byPartition).collect(Collectors.toList());

            OffsetRange[] ranges = new OffsetRange[offsetRanges.size()];
            return offsetRanges.toArray(ranges);
        }

        public static long totalNewMessages(OffsetRange[] ranges) {
            long totalMsgs = 0;
            for (OffsetRange range: ranges) {
                totalMsgs += Math.max(range.untilOffset()-range.fromOffset(), 0);
            }
            return totalMsgs;
        }
    }

    /**
     * Helpers to deal with tricky scala <=> java conversions. (oh my!)
     */
    static class ScalaHelpers {
        public static <K,V>  Map<K, V> toScalaMap(HashMap<K, V> m) {
            return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                    Predef.<Tuple2<K, V>>conforms()
            );
        }

        public static Set<String> toScalaSet(HashSet<String> s) {
            return JavaConverters.asScalaSetConverter(s).asScala().<String>toSet();
        }

        public static <K, V>  java.util.Map<K, V> toJavaMap(Map<K, V> m) {
            return JavaConverters.<K, V>mapAsJavaMapConverter(m).asJava();
        }
    }


    /**
     * Configs to be passed for this source. All standard Kafka consumer configs are also
     * respected
     */
    static class Config {
        private final static String KAFKA_TOPIC_NAME = "hoodie.deltastreamer.source.kafka.topic";
        private final static String DEFAULT_AUTO_RESET_OFFSET = "largest";
    }


    private HashMap<String, String> kafkaParams;

    private final String topicName;

    public KafkaSource(PropertiesConfiguration config, JavaSparkContext sparkContext, SourceDataFormat dataFormat, SchemaProvider schemaProvider) {
        super(config, sparkContext, dataFormat, schemaProvider);

        kafkaParams = new HashMap<>();
        Stream<String> keys = StreamSupport.stream(Spliterators.spliteratorUnknownSize(config.getKeys(), Spliterator.NONNULL), false);
        keys.forEach(k -> kafkaParams.put(k, config.getString(k)));

        UtilHelpers.checkRequiredProperties(config, Arrays.asList(Config.KAFKA_TOPIC_NAME));
        topicName = config.getString(Config.KAFKA_TOPIC_NAME);
    }

    @Override
    public Pair<Optional<JavaRDD<GenericRecord>>, String> fetchNewData(Optional<String> lastCheckpointStr, long maxInputBytes) {

        // Obtain current metadata for the topic
        KafkaCluster cluster = new KafkaCluster(ScalaHelpers.toScalaMap(kafkaParams));
        Either<ArrayBuffer<Throwable>, Set<TopicAndPartition>> either = cluster.getPartitions(ScalaHelpers.toScalaSet(new HashSet<>(Arrays.asList(topicName))));
        if (either.isLeft()) {
            // log errors. and bail out.
            throw new HoodieDeltaStreamerException("Error obtaining partition metadata", either.left().get().head());
        }
        Set<TopicAndPartition> topicPartitions = either.right().get();

        // Determine the offset ranges to read from
        HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> fromOffsets;
        if (lastCheckpointStr.isPresent()) {
            fromOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
        } else {
            String autoResetValue = config.getString("auto.offset.reset", Config.DEFAULT_AUTO_RESET_OFFSET);
            if (autoResetValue.equals("smallest")) {
                fromOffsets = new HashMap(ScalaHelpers.toJavaMap(cluster.getEarliestLeaderOffsets(topicPartitions).right().get()));
            } else if (autoResetValue.equals("largest")) {
                fromOffsets = new HashMap(ScalaHelpers.toJavaMap(cluster.getLatestLeaderOffsets(topicPartitions).right().get()));
            } else {
                throw new HoodieNotSupportedException("Auto reset value must be one of 'smallest' or 'largest' ");
            }
        }

        // Always read until the latest offset
        HashMap<TopicAndPartition, KafkaCluster.LeaderOffset> toOffsets = new HashMap(ScalaHelpers.toJavaMap(cluster.getLatestLeaderOffsets(topicPartitions).right().get()));


        // Come up with final set of OffsetRanges to read (account for new partitions)
        // TODO(vc): Respect maxInputBytes, by estimating number of messages to read each batch from partition size
        OffsetRange[] offsetRanges = CheckpointUtils.computeOffsetRanges(fromOffsets, toOffsets);
        long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
        if (totalNewMsgs <= 0) {
            return new ImmutablePair<>(Optional.empty(), lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : CheckpointUtils.offsetsToStr(toOffsets));
        } else {
            log.info("About to read " + totalNewMsgs + " from Kafka for topic :" + topicName);
        }


        // Perform the actual read from Kafka
        JavaRDD<byte[]> kafkaRDD =  KafkaUtils.createRDD(
                sparkContext,
                byte[].class,
                byte[].class,
                DefaultDecoder.class,
                DefaultDecoder.class,
                kafkaParams,
                offsetRanges).values();

        // Produce a RDD[GenericRecord]
        final AvroConvertor avroConvertor = new AvroConvertor(schemaProvider.getSourceSchema().toString());
        JavaRDD<GenericRecord> newDataRDD;
        if (dataFormat == SourceDataFormat.AVRO) {
            newDataRDD = kafkaRDD.map(bytes -> avroConvertor.fromAvroBinary(bytes));
        } else if (dataFormat == SourceDataFormat.JSON) {
            newDataRDD = kafkaRDD.map(bytes -> avroConvertor.fromJson(new String(bytes, Charset.forName("utf-8"))));
        } else {
            throw new HoodieNotSupportedException("Unsupport data format :" + dataFormat);
        }

        return new ImmutablePair<>(Optional.of(newDataRDD), CheckpointUtils.offsetsToStr(toOffsets));
    }
}
