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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import java.util.Optional;
import kafka.serializer.StringDecoder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

/**
 * Read json kafka data
 */
public class JsonKafkaSource extends JsonSource {

  private static Logger log = LogManager.getLogger(JsonKafkaSource.class);

  private final KafkaOffsetGen offsetGen;

  public JsonKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(properties, sparkContext, sparkSession, schemaProvider);
    offsetGen = new KafkaOffsetGen(properties);
  }

  @Override
  protected InputBatch<JavaRDD<String>> fetchNewData(Optional<String> lastCheckpointStr,
      long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    if (totalNewMsgs <= 0) {
      return new InputBatch<>(Optional.empty(),
          lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : "");
    }
    log.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    JavaRDD<String> newDataRDD = toRDD(offsetRanges);
    return new InputBatch<>(Optional.of(newDataRDD), CheckpointUtils.offsetsToStr(offsetRanges));
  }

  private JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {
    return KafkaUtils.createRDD(sparkContext, String.class, String.class, StringDecoder.class, StringDecoder.class,
        offsetGen.getKafkaParams(), offsetRanges).values();
  }
}
