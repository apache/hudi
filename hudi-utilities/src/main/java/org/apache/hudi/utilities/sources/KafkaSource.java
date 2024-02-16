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
import org.apache.hudi.utilities.streamer.StreamProfile;
import org.apache.hudi.utilities.streamer.StreamProfileSupplier;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

abstract class KafkaSource<T> extends Source<JavaRDD<T>> {
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
                        SchemaProvider schemaProvider, SourceType sourceType, HoodieIngestionMetrics metrics, Option<StreamProfileSupplier> streamProfileSupplier) {
    super(props, sparkContext, sparkSession, schemaProvider, sourceType, streamProfileSupplier);
    this.schemaProvider = schemaProvider;
    this.metrics = metrics;
    this.shouldAddOffsets = KafkaOffsetPostProcessor.Config.shouldAddOffsets(props);
  }

  @Override
  protected InputBatch<JavaRDD<T>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    try {
      OffsetRange[] offsetRanges;
      if (streamProfilerSupplier.isPresent() && streamProfilerSupplier.get().getStreamProfile() != null) {
        StreamProfile<Long> kafkaStreamProfile = streamProfilerSupplier.get().getStreamProfile();
        offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, kafkaStreamProfile.getSourceSpecificContext(), kafkaStreamProfile.getSourcePartitions(), metrics);
        LOG.info("About to read numEvents {} of size {} bytes in {} partitions from Kafka for topic {} with offsetRanges {}",
            kafkaStreamProfile.getSourceSpecificContext(), kafkaStreamProfile.getMaxSourceBytes(),
            kafkaStreamProfile.getSourcePartitions(), offsetGen.getTopicName(), offsetRanges);
      } else {
        offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
      }
      return toInputBatch(offsetRanges);
    } catch (org.apache.kafka.common.errors.TimeoutException e) {
      throw new HoodieSourceTimeoutException("Kafka Source timed out " + e.getMessage());
    }
  }

  private InputBatch<JavaRDD<T>> toInputBatch(OffsetRange[] offsetRanges) {
    long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    if (totalNewMsgs <= 0) {
      metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KAFKA_MESSAGE_IN_COUNT, 0);
      return new InputBatch<>(Option.empty(), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    }
    metrics.updateStreamerSourceNewMessageCount(METRIC_NAME_KAFKA_MESSAGE_IN_COUNT, totalNewMsgs);
    JavaRDD<T> newDataRDD = toRDD(offsetRanges);
    return new InputBatch<>(Option.of(newDataRDD), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  abstract JavaRDD<T> toRDD(OffsetRange[] offsetRanges);

  @Override
  public void onCommit(String lastCkptStr) {
    if (getBooleanWithAltKeys(this.props, KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET)) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}
