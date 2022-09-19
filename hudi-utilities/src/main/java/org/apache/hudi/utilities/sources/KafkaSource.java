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
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;

abstract class KafkaSource<T> extends Source<JavaRDD<T>> {
  private static final Logger LOG = LogManager.getLogger(KafkaSource.class);
  // these are native kafka's config. do not change the config names.
  protected static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  protected static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";

  protected final HoodieDeltaStreamerMetrics metrics;
  protected final SchemaProvider schemaProvider;
  protected KafkaOffsetGen offsetGen;

  protected KafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                        SchemaProvider schemaProvider, SourceType sourceType, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider, sourceType);

    this.schemaProvider = schemaProvider;
    this.metrics = metrics;
  }

  @Override
  protected InputBatch<JavaRDD<T>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    try {
      OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
      long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(offsetRanges);
      LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
      if (totalNewMsgs <= 0) {
        metrics.updateDeltaStreamerKafkaMessageInCount(0);
        return new InputBatch<>(Option.empty(), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
      }
      metrics.updateDeltaStreamerKafkaMessageInCount(totalNewMsgs);
      JavaRDD<T> newDataRDD = toRDD(offsetRanges);
      return new InputBatch<>(Option.of(newDataRDD), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    } catch (org.apache.kafka.common.errors.TimeoutException e) {
      throw new HoodieSourceTimeoutException("Kafka Source timed out " + e.getMessage());
    }
  }

  abstract JavaRDD<T> toRDD(OffsetRange[] offsetRanges);

  @Override
  public void onCommit(String lastCkptStr) {
    if (this.props.getBoolean(KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.key(), KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.defaultValue())) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}
