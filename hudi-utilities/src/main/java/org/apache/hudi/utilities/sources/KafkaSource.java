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
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.streamer.SourceProfile;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;

public abstract class KafkaSource<T> extends Source<T> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
  private static final String COMMA_DELIMITER = ",";
  // these are native kafka's config. do not change the config names.
  protected static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  protected static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  protected static final String METRIC_NAME_KAFKA_MESSAGE_IN_COUNT = "kafkaMessageInCount";

  protected final HoodieIngestionMetrics metrics;
  protected final SchemaProvider schemaProvider;
  protected KafkaOffsetGen offsetGen;

  protected final boolean shouldAddOffsets;

  protected KafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                        SourceType sourceType, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(props, sparkContext, sparkSession, sourceType, streamContext);
    this.schemaProvider = streamContext.getSchemaProvider();
    this.metrics = metrics;
    this.shouldAddOffsets = KafkaOffsetPostProcessor.Config.shouldAddOffsets(props);
  }

  @Override
  protected final InputBatch<T> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    throw new UnsupportedOperationException("KafkaSource#fetchNewData should not be called");
  }

  @Override
  protected InputBatch<T> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    try {
      OffsetRange[] offsetRanges = getOffsetRanges(props, sourceProfileSupplier, offsetGen, metrics,
          lastCheckpoint, sourceLimit);
      return toInputBatch(offsetRanges);
    } catch (org.apache.kafka.common.errors.TimeoutException e) {
      throw new HoodieSourceTimeoutException("Kafka Source timed out " + e.getMessage());
    } catch (KafkaException ex) {
      if (hasConfigException(ex)) {
        throw new HoodieReadFromSourceException("kafka source config issue ", ex);
      }
      throw ex;
    }
  }

  @SuppressWarnings("unchecked")
  public static OffsetRange[] getOffsetRanges(TypedProperties props,
                                              Option<SourceProfileSupplier> sourceProfileSupplier,
                                              KafkaOffsetGen offsetGen,
                                              HoodieIngestionMetrics metrics,
                                              Option<Checkpoint> lastCheckpoint,
                                              long sourceLimit) {
    OffsetRange[] offsetRanges;
    if (sourceProfileSupplier.isPresent() && sourceProfileSupplier.get().getSourceProfile() != null) {
      SourceProfile<Long> kafkaSourceProfile = sourceProfileSupplier.get().getSourceProfile();
      offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpoint, kafkaSourceProfile.getSourceSpecificContext(),
          kafkaSourceProfile.getSourcePartitions(), metrics);
      metrics.updateStreamerSourceParallelism(kafkaSourceProfile.getSourcePartitions());
      metrics.updateStreamerSourceBytesToBeIngestedInSyncRound(kafkaSourceProfile.getMaxSourceBytes());
      LOG.info("About to read maxEventsInSyncRound {} of size {} bytes in {} partitions from Kafka for topic {} with offsetRanges {}",
          kafkaSourceProfile.getSourceSpecificContext(), kafkaSourceProfile.getMaxSourceBytes(),
          kafkaSourceProfile.getSourcePartitions(), offsetGen.getTopicName(), offsetRanges);
    } else {
      int minPartitions = (int) getLongWithAltKeys(props, KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS);
      metrics.updateStreamerSourceParallelism(minPartitions);
      offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpoint, sourceLimit, metrics);
      LOG.info("About to read sourceLimit {} in {} spark partitions from kafka for topic {} with offset ranges {}",
          sourceLimit, minPartitions, offsetGen.getTopicName(),
          Arrays.toString(offsetRanges));
    }
    return offsetRanges;
  }

  private InputBatch<T> toInputBatch(OffsetRange[] offsetRanges) {
    long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read {} from Kafka for topic :{} after offset generation with offset ranges {}",
        totalNewMsgs, offsetGen.getTopicName(), Arrays.toString(offsetRanges));
    if (totalNewMsgs <= 0) {
      metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KAFKA_MESSAGE_IN_COUNT, 0);
      return new InputBatch<>(Option.empty(), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    }
    metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KAFKA_MESSAGE_IN_COUNT, totalNewMsgs);
    T newBatch = toBatch(offsetRanges);
    return new InputBatch<>(Option.of(newBatch), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  /**
   * Creates a Kafka RDD with the specified key-value deserialization types.
   *
   * @param props        Configuration properties for the Kafka source, including topic, bootstrap servers, etc.
   * @param sparkContext Spark context used to create the RDD.
   * @param offsetGen    Generator that provides Kafka parameters and helps map offset ranges.
   * @param offsetRanges Kafka offset ranges to read data from.
   *
   * @param <K>          Type of the Kafka record key.
   * @param <V>          Type of the Kafka record value.
   * @return JavaRDD containing Kafka ConsumerRecord entries with deserialized key-value pairs.
   */
  public static <K, V> JavaRDD<ConsumerRecord<K, V>> createKafkaRDD(
      TypedProperties props,
      JavaSparkContext sparkContext,
      KafkaOffsetGen offsetGen,
      OffsetRange[] offsetRanges) {
    Map<String, Object> kafkaParams =
        filterKafkaParameters(offsetGen.getKafkaParams(), ConfigUtils.getStringWithAltKeys(props, KafkaSourceConfig.IGNORE_PREFIX_CONFIG_LIST, true));
    LOG.debug("Original kafka params " + offsetGen.getKafkaParams() + "\n After filtering kafka params " + kafkaParams);
    return KafkaUtils.createRDD(sparkContext, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent());
  }

  protected abstract T toBatch(OffsetRange[] offsetRanges);

  @Override
  public void onCommit(String lastCkptStr) {
    if (getBooleanWithAltKeys(this.props, KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET)) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }

  /**
  * Utility method that removes configs with keys that match (start with) any one of the prefixes.
  *
  * @param kafkaParams The incoming kafka params
  * @param commaSeparatedPrefixes all configs with keys starting with any one of these comma-separated prefixes will be ignored.
  * @return a new set of kafkaParams with the configs matching the prefixes removed.
  **/
  @VisibleForTesting
  static Map<String, Object> filterKafkaParameters(Map<String, Object> kafkaParams, String commaSeparatedPrefixes) {
    if (commaSeparatedPrefixes == null || commaSeparatedPrefixes.isEmpty()) {
      return kafkaParams;
    }

    String[] prefixes = Arrays.stream(commaSeparatedPrefixes.split(COMMA_DELIMITER))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toArray(String[]::new);

    Map<String, Object> filteredInParams = new HashMap<>();
    for (Map.Entry<String, Object> entry : kafkaParams.entrySet()) {
      boolean beginsWithAtleastOnePrefix = false;
      for (String prefix : prefixes) {
        if (entry.getKey().startsWith(prefix)) {
          beginsWithAtleastOnePrefix = true;
          break;
        }
      }

      if (!beginsWithAtleastOnePrefix) {
        filteredInParams.put(entry.getKey(), entry.getValue());
      }
    }

    return filteredInParams;
  }

  private boolean hasConfigException(Throwable e) {
    if (e == null) {
      return false;
    }

    if (e instanceof ConfigException || e instanceof io.confluent.common.config.ConfigException) {
      return true;
    }

    return hasConfigException(e.getCause());
  }
}
