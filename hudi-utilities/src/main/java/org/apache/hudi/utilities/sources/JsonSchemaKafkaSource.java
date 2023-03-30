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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;

public class JsonSchemaKafkaSource extends JsonKafkaSource {
  private static final Logger LOG = LogManager.getLogger(JsonKafkaSource.class);

  public JsonSchemaKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession, SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    super(properties, sparkContext, sparkSession, schemaProvider, metrics);
    properties.put("value.deserializer", "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
  }

  @Override
  JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {
    JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD = KafkaUtils.createRDD(sparkContext,
            offsetGen.getKafkaParams(),
            offsetRanges,
            LocationStrategies.PreferConsistent())
        .filter(x -> x.value() != null);
    return postProcess(maybeAppendKafkaOffsets(kafkaRDD));
  }

  @Override
  protected JavaRDD<String> maybeAppendKafkaOffsets(JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD) {
    ObjectMapper om = new ObjectMapper();
    if (this.shouldAddOffsets) {
      return kafkaRDD.mapPartitions(partitionIterator -> {
        List<String> stringList = new LinkedList<>();
        partitionIterator.forEachRemaining(consumerRecord -> {
          String record = null;
          if (consumerRecord.value() instanceof LinkedHashMap) {
            LinkedHashMap<String, Object> val = (LinkedHashMap<String, Object>) consumerRecord.value();
            try {
              record = om.writeValueAsString(val);
            } catch (JsonProcessingException e) {
              LOG.error(e);
              throw new HoodieException(e);
            }
          } else {
            throw new IllegalArgumentException("consumerRecord value is of invalid type " + consumerRecord.value().getClass());
          }
          try {
            ObjectNode jsonNode = (ObjectNode) om.readTree(record);
            jsonNode.put(KAFKA_SOURCE_OFFSET_COLUMN, consumerRecord.offset());
            jsonNode.put(KAFKA_SOURCE_PARTITION_COLUMN, consumerRecord.partition());
            jsonNode.put(KAFKA_SOURCE_TIMESTAMP_COLUMN, consumerRecord.timestamp());
            stringList.add(om.writeValueAsString(jsonNode));
          } catch (Throwable e) {
            stringList.add(record);
          }
        });
        return stringList.iterator();
      });
    }
    return kafkaRDD.map(consumerRecord -> {
      if (consumerRecord.value() instanceof LinkedHashMap) {
        LinkedHashMap<String, Object> val = (LinkedHashMap<String, Object>) consumerRecord.value();
        return new ObjectMapper().writeValueAsString(val);
      }
      throw new IllegalArgumentException("consumerRecord value is of invalid type " + consumerRecord.value().getClass());
    });
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
}
