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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.ProtoClassBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Reads protobuf serialized Kafka data, based on a provided class name.
 */
public class ProtoKafkaSource extends KafkaSource<Message> {

  private static final Logger LOG = LogManager.getLogger(ProtoKafkaSource.class);
  // these are native kafka's config. do not change the config names.
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  private final String className;

  public ProtoKafkaSource(TypedProperties props, JavaSparkContext sparkContext,
                          SparkSession sparkSession, SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider, SourceType.PROTO, metrics);
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(
        ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_CLASS_NAME));
    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class);
    props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, ByteArrayDeserializer.class);
    className = props.getString(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_CLASS_NAME);
  }

  @Override
  JavaRDD<Message> toRDD(OffsetRange[] offsetRanges) {
    ProtoDeserializer deserializer = new ProtoDeserializer(className);
    return KafkaUtils.<String, byte[]>createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
        LocationStrategies.PreferConsistent()).map(obj -> deserializer.parse(obj.value()));
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
        throw new HoodieException("Failed to parse proto message from kafka", ex);
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
          throw new HoodieException("Unable to get proto parsing method from specified class: " + className, ex);
        }
      }
      return parseMethod;
    }
  }
}
