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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.config.ProtoClassBasedSchemaProviderConfig;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * Reads protobuf serialized Kafka data, based on a provided class name.
 */
public class ProtoKafkaSource extends KafkaSource<JavaRDD<Message>> {
  private final Option<String> className;
  private final String deserializerName;

  public ProtoKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public ProtoKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(properties, sparkContext, sparkSession, SourceType.PROTO, metrics,
        new DefaultStreamContext(UtilHelpers.getSchemaProviderForKafkaSource(streamContext.getSchemaProvider(), properties, sparkContext), streamContext.getSourceProfileSupplier()));
    this.deserializerName = ConfigUtils.getStringWithAltKeys(props, KafkaSourceConfig.KAFKA_PROTO_VALUE_DESERIALIZER_CLASS, true);
    if (!deserializerName.equals(ByteArrayDeserializer.class.getName()) && !deserializerName.equals(KafkaProtobufDeserializer.class.getName())) {
      throw new HoodieReadFromSourceException("Only ByteArrayDeserializer and KafkaProtobufDeserializer are supported for ProtoKafkaSource");
    }
    if (deserializerName.equals(ByteArrayDeserializer.class.getName())) {
      checkRequiredConfigProperties(props, Collections.singletonList(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME));
      className = Option.of(getStringWithAltKeys(props, ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME));
    } else {
      className = Option.empty();
    }
    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, deserializerName);
    this.offsetGen = new KafkaOffsetGen(props);
    if (this.shouldAddOffsets) {
      throw new HoodieReadFromSourceException("Appending kafka offsets to ProtoKafkaSource is not supported");
    }
  }

  @Override
  protected JavaRDD<Message> toBatch(OffsetRange[] offsetRanges) {
    if (deserializerName.equals(ByteArrayDeserializer.class.getName())) {
      ValidationUtils.checkArgument(
          className.isPresent(),
          ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key() + " config must be present.");
      ProtoDeserializer deserializer = new ProtoDeserializer(className.get());
      JavaRDD<ConsumerRecord<String, byte[]>> kafkaRDD = createKafkaRDD(this.props, sparkContext, offsetGen, offsetRanges);
      return kafkaRDD.map(obj -> deserializer.parse(obj.value()));
    } else {
      JavaRDD<ConsumerRecord<String, Message>> kafkaRDD = createKafkaRDD(this.props, sparkContext, offsetGen, offsetRanges);
      return kafkaRDD.map(ConsumerRecord::value);
    }
  }

  @Override
  protected boolean allowSourcePersist() {
    // Persisting proto messages where protobuf class is unknown, is expensive because of the overhead.
    // Eg: Persisting DynamicMessage using kryo requires attaching descriptor info for each message.
    return persistRdd && deserializerName.equals(ByteArrayDeserializer.class.getName());
  }

  private static class ProtoDeserializer implements Serializable {
    private final String className;
    private transient Class protoClass;
    private transient Method parseMethod;

    public ProtoDeserializer(String className) {
      this.className = className;
    }

    public Message parse(byte[] bytes) {
      try {
        return (Message) getParseMethod().invoke(getClass(), bytes);
      } catch (IllegalAccessException | InvocationTargetException ex) {
        throw new HoodieReadFromSourceException("Failed to parse proto message from kafka", ex);
      }
    }

    private Class getProtoClass() {
      if (protoClass == null) {
        protoClass = ReflectionUtils.getClass(className);
      }
      return protoClass;
    }

    private Method getParseMethod() {
      if (parseMethod == null) {
        try {
          parseMethod = getProtoClass().getMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException ex) {
          throw new HoodieReadFromSourceException("Unable to get proto parsing method from specified class: " + className, ex);
        }
      }
      return parseMethod;
    }
  }
}
