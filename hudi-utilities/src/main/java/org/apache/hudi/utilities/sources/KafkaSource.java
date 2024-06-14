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
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.streamer.SourceProfile;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;

public abstract class KafkaSource<T> extends Source<T> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
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
  protected InputBatch<T> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    try {
      return toInputBatch(getOffsetRanges(props, sourceProfileSupplier, offsetGen, metrics,
          lastCheckpointStr, sourceLimit));
    } catch (org.apache.kafka.common.errors.TimeoutException e) {
      throw new HoodieSourceTimeoutException("Kafka Source timed out " + e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  public static OffsetRange[] getOffsetRanges(TypedProperties props,
                                              Option<SourceProfileSupplier> sourceProfileSupplier,
                                              KafkaOffsetGen offsetGen,
                                              HoodieIngestionMetrics metrics,
                                              Option<String> lastCheckpointStr,
                                              long sourceLimit) {
    OffsetRange[] offsetRanges;
    if (sourceProfileSupplier.isPresent() && sourceProfileSupplier.get().getSourceProfile() != null) {
      SourceProfile<Long> kafkaSourceProfile = sourceProfileSupplier.get().getSourceProfile();
      offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, kafkaSourceProfile.getSourceSpecificContext(),
          kafkaSourceProfile.getSourcePartitions(), metrics);
      metrics.updateStreamerSourceParallelism(kafkaSourceProfile.getSourcePartitions());
      metrics.updateStreamerSourceBytesToBeIngestedInSyncRound(kafkaSourceProfile.getMaxSourceBytes());
      LOG.info("About to read maxEventsInSyncRound {} of size {} bytes in {} partitions from Kafka for topic {} with offsetRanges {}",
          kafkaSourceProfile.getSourceSpecificContext(), kafkaSourceProfile.getMaxSourceBytes(),
          kafkaSourceProfile.getSourcePartitions(), offsetGen.getTopicName(), offsetRanges);
    } else {
      int minPartitions = (int) getLongWithAltKeys(props, KafkaSourceConfig.KAFKA_SOURCE_MIN_PARTITIONS);
      metrics.updateStreamerSourceParallelism(minPartitions);
      offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
      LOG.info("About to read sourceLimit {} in {} spark partitions from kafka for topic {} with offset ranges {}",
          sourceLimit, minPartitions, offsetGen.getTopicName(),
          Arrays.toString(offsetRanges));
    }
    return offsetRanges;
  }

  private InputBatch<T> toInputBatch(OffsetRange[] offsetRanges) {
    long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    if (totalNewMsgs <= 0) {
      metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KAFKA_MESSAGE_IN_COUNT, 0);
      return new InputBatch<>(Option.empty(), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    }
    metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KAFKA_MESSAGE_IN_COUNT, totalNewMsgs);
    T newBatch = toBatch(offsetRanges);
    return new InputBatch<>(Option.of(newBatch), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  protected abstract T toBatch(OffsetRange[] offsetRanges);

  @Override
  public void onCommit(String lastCkptStr) {
    if (getBooleanWithAltKeys(this.props, KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET)) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}
