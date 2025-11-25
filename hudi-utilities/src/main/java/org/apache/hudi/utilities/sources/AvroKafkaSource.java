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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deser.KafkaAvroSchemaDeserializer;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaSourceUtil;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS;

/**
 * Reads avro serialized Kafka data, based on the confluent schema-registry.
 */
public class AvroKafkaSource extends KafkaSource<JavaRDD<GenericRecord>> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroKafkaSource.class);
  // These are settings used to pass things to KafkaAvroDeserializer
  public static final String KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX =
      STREAMER_CONFIG_PREFIX + "source.kafka.value.deserializer.";
  @Deprecated
  public static final String OLD_KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX =
      DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.value.deserializer.";
  @Deprecated
  public static final String KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA =
      OLD_KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX + "schema";
  private final String deserializerClassName;

  //other schema provider may have kafka offsets
  protected final SchemaProvider originalSchemaProvider;

  public AvroKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                         SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public AvroKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession, HoodieIngestionMetrics metrics, StreamContext streamContext) {
    super(properties, sparkContext, sparkSession, SourceType.AVRO, metrics,
        new DefaultStreamContext(UtilHelpers.getSchemaProviderForKafkaSource(streamContext.getSchemaProvider(), properties, sparkContext), streamContext.getSourceProfileSupplier()));
    this.originalSchemaProvider = streamContext.getSchemaProvider();

    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    deserializerClassName = getStringWithAltKeys(props, KAFKA_AVRO_VALUE_DESERIALIZER_CLASS, true);

    try {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName).getName());
    } catch (ClassNotFoundException e) {
      String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
      LOG.error(error);
      throw new HoodieReadFromSourceException(error, e);
    }

    if (deserializerClassName.equals(KafkaAvroSchemaDeserializer.class.getName())) {
      KafkaSourceUtil.configureSchemaDeserializer(schemaProvider, props);
    }
    offsetGen = new KafkaOffsetGen(props);
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    if (deserializerClassName.equals(KafkaAvroSchemaDeserializer.class.getName())) {
      KafkaSourceUtil.configureSchemaDeserializer(schemaProvider, props);
      offsetGen = new KafkaOffsetGen(props);
    }
    return super.readFromCheckpoint(lastCheckpoint, sourceLimit);
  }

  @Override
  protected JavaRDD<GenericRecord> toBatch(OffsetRange[] offsetRanges) {
    JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD;
    if (deserializerClassName.equals(ByteArrayDeserializer.class.getName())) {
      if (schemaProvider == null) {
        throw new HoodieReadFromSourceException("Please provide a valid schema provider class when use ByteArrayDeserializer!");
      }

      //Don't want kafka offsets here so we use originalSchemaProvider
      AvroConvertor convertor = new AvroConvertor(originalSchemaProvider.getSourceSchema().toAvroSchema());
      JavaRDD<ConsumerRecord<String, byte[]>> kafkaRDDByteArray = createKafkaRDD(this.props, sparkContext, offsetGen, offsetRanges);
      kafkaRDD = kafkaRDDByteArray.filter(obj -> obj.value() != null)
          .map(obj -> new ConsumerRecord<>(obj.topic(), obj.partition(), obj.offset(), obj.key(), convertor.fromAvroBinary(obj.value())));
    } else {
      kafkaRDD = createKafkaRDD(this.props, sparkContext, offsetGen, offsetRanges);
    }

    return maybeAppendKafkaOffsets(kafkaRDD.filter(consemerRec -> consemerRec.value() != null));
  }

  protected JavaRDD<GenericRecord> maybeAppendKafkaOffsets(JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD) {
    if (this.shouldAddOffsets) {
      AvroConvertor convertor = new AvroConvertor(schemaProvider.getSourceSchema().toAvroSchema());
      return kafkaRDD.map(convertor::withKafkaFieldsAppended);
    } else {
      return kafkaRDD.map(consumerRecord -> (GenericRecord) consumerRecord.value());
    }
  }
}
