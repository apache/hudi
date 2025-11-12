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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.JsonKafkaPostProcessorConfig;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_KEY_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN;
import static org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN;

/**
 * Read json kafka data.
 */
public class JsonKafkaSource extends KafkaSource<JavaRDD<String>> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonKafkaSource.class);

  /**
   * Configs specific to {@link JsonKafkaSource}.
   */
  public static class Config  {
    public static final ConfigProperty<String> KAFKA_JSON_VALUE_DESERIALIZER_CLASS = ConfigProperty
        .key("hoodie.deltastreamer.source.kafka.json.value.deserializer.class")
        .defaultValue(StringDeserializer.class.getName())
        .sinceVersion("1.1.0")
        .withDocumentation("Kafka Json Payload Deserializer Class");
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public JsonKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                         SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    this(properties, sparkContext, sparkSession, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public JsonKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(properties, sparkContext, sparkSession, SourceType.JSON, metrics,
        new DefaultStreamContext(UtilHelpers.getSchemaProviderForKafkaSource(streamContext.getSchemaProvider(), properties, sparkContext), streamContext.getSourceProfileSupplier()));
    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, ConfigUtils.getStringWithAltKeys(props, Config.KAFKA_JSON_VALUE_DESERIALIZER_CLASS, true));
    this.offsetGen = new KafkaOffsetGen(props);
  }

  @Override
  protected JavaRDD<String> toBatch(OffsetRange[] offsetRanges) {
    String deserializerClass = props.getString(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP);
    JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD = createKafkaRDD(this.props, sparkContext, offsetGen, offsetRanges)
        .filter(x -> filterForNullValues(x.value(), deserializerClass));
    return postProcess(maybeAppendKafkaOffsets(kafkaRDD, deserializerClass));
  }

  protected JavaRDD<String> maybeAppendKafkaOffsets(JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD, String deserializerClass) {
    if (shouldAddOffsets) {
      return kafkaRDD.mapPartitions(partitionIterator -> {
        TaskContext taskContext = TaskContext.get();
        LOG.info("Converting Kafka source objects to strings with stageId : {}, stage attempt no: {}, taskId : {}, task attempt no : {}, task attempt id : {} ",
            taskContext.stageId(), taskContext.stageAttemptNumber(), taskContext.partitionId(), taskContext.attemptNumber(),
            taskContext.taskAttemptId());
        return new CloseableMappingIterator<>(ClosableIterator.wrap(partitionIterator), consumerRecord -> {
          String recordKey;
          String record;
          try {
            record = getValueAsString(consumerRecord.value(), deserializerClass);
            recordKey = StringUtils.objToString(consumerRecord.key());
          } catch (JsonProcessingException e) {
            throw new HoodieException(e);
          }
          try {
            ObjectNode jsonNode = (ObjectNode) OBJECT_MAPPER.readTree(record);
            jsonNode.put(KAFKA_SOURCE_OFFSET_COLUMN, consumerRecord.offset());
            jsonNode.put(KAFKA_SOURCE_PARTITION_COLUMN, consumerRecord.partition());
            jsonNode.put(KAFKA_SOURCE_TIMESTAMP_COLUMN, consumerRecord.timestamp());
            if (recordKey != null) {
              jsonNode.put(KAFKA_SOURCE_KEY_COLUMN, recordKey);
            }
            return OBJECT_MAPPER.writeValueAsString(jsonNode);
          } catch (Throwable e) {
            return record;
          }
        });
      });
    } else {
      return kafkaRDD.map(consumerRecord -> {
        try {
          return getValueAsString(consumerRecord.value(), deserializerClass);
        } catch (JsonProcessingException e) {
          throw new HoodieException(e);
        }
      });
    }
  }

  private JavaRDD<String> postProcess(JavaRDD<String> jsonStringRDD) {
    String postProcessorClassName = getStringWithAltKeys(
        this.props, JsonKafkaPostProcessorConfig.JSON_KAFKA_PROCESSOR_CLASS, true);
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

  private static Boolean filterForNullValues(Object value, String valueDeserializerClass) {
    if (value == null) {
      return false;
    }
    if (valueDeserializerClass.equals(StringDeserializer.class.getName())) {
      return StringUtils.nonEmpty((String) value);
    }
    return true;
  }

  private static String getValueAsString(Object value, String valueDeserializerClass) throws JsonProcessingException {
    if (StringDeserializer.class.getName().equals(valueDeserializerClass)) {
      return (String) value;
    }
    return OBJECT_MAPPER.writeValueAsString(value);
  }
}
