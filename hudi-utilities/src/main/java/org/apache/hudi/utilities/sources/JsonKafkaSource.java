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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.exception.HoodieSourceTimeoutException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.IOException;

/**
 * Read json kafka data.
 */
public class JsonKafkaSource extends JsonSource {

  private static final Logger LOG = LogManager.getLogger(JsonKafkaSource.class);

  private final KafkaOffsetGen offsetGen;

  private final HoodieDeltaStreamerMetrics metrics;

  public JsonKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                         SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(properties, sparkContext, sparkSession, schemaProvider);
    this.metrics = metrics;
    properties.put("key.deserializer", StringDeserializer.class.getName());
    properties.put("value.deserializer", StringDeserializer.class.getName());
    offsetGen = new KafkaOffsetGen(properties);
  }

  @Override
  protected InputBatch<JavaRDD<String>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    try {
      OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit, metrics);
      long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
      LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
      if (totalNewMsgs <= 0) {
        return new InputBatch<>(Option.empty(), CheckpointUtils.offsetsToStr(offsetRanges));
      }
      JavaRDD<String> newDataRDD = toRDD(offsetRanges);
      return new InputBatch<>(Option.of(newDataRDD), CheckpointUtils.offsetsToStr(offsetRanges));
    } catch (org.apache.kafka.common.errors.TimeoutException e) {
      throw new HoodieSourceTimeoutException("Kafka Source timed out " + e.getMessage());
    }
  }

  private JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {
    JavaRDD<String> jsonStringRDD = KafkaUtils.createRDD(sparkContext,
            offsetGen.getKafkaParams(),
            offsetRanges,
            LocationStrategies.PreferConsistent())
        .filter(x -> !StringUtils.isNullOrEmpty((String) x.value()))
        .map(x -> x.value().toString());
    return postProcess(jsonStringRDD);
  }

  private JavaRDD<String> postProcess(JavaRDD<String> jsonStringRDD) {
    String postProcessorClassName = this.props.getString(KafkaOffsetGen.Config.JSON_KAFKA_PROCESSOR_CLASS_OPT.key(), null);
    // no processor, do nothing
    if (StringUtils.isNullOrEmpty(postProcessorClassName)) {
      return jsonStringRDD;
    }

    JsonKafkaSourcePostProcessor processor;
    try {
      processor = UtilHelpers.createJsonKafkaSourcePostProcessor(postProcessorClassName, this.props);
    } catch (IOException e) {
      throw new HoodieSourcePostProcessException("Could not init " + postProcessorClassName, e);
    }

    return processor.process(jsonStringRDD);
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (this.props.getBoolean(KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.key(),
        KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.defaultValue())) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}
