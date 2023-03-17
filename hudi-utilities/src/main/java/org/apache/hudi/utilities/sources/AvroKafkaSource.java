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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deser.KafkaAvroSchemaDeserializer;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * Reads avro serialized Kafka data, based on the confluent schema-registry.
 */
public class AvroKafkaSource extends KafkaSource<GenericRecord> {

  private static final Logger LOG = LogManager.getLogger(AvroKafkaSource.class);
  // These are settings used to pass things to KafkaAvroDeserializer
  public static final String KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX = "hoodie.deltastreamer.source.kafka.value.deserializer.";
  public static final String KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA = KAFKA_AVRO_VALUE_DESERIALIZER_PROPERTY_PREFIX + "schema";
  private final String deserializerClassName;

  //other schema provider may have kafka offsets
  protected final SchemaProvider originalSchemaProvider;

  /**
   * In the constructor of AvroKafkaSource, the schemaProvider parameter is passed in from outside the class, which represents the source schema provider that is used to read the schema of the incoming data from Kafka.
   * The method UtilHelpers.getSchemaProviderForKafkaSource is then called IN THE CONSTRUCTOR to obtain the actual SchemaProvider instance that is used for the source.
   *    This method takes the input schema provider and a set of properties, and returns a SchemaProvider instance based on these inputs.
   *    The SchemaProvider instance that is returned is responsible for providing the schema of the data being read from Kafka.
   * The SchemaProvider can be built either from the data itself or from the schema registry. In the case of this class, it depends on the configuration specified in the input properties passed to the constructor of AvroKafkaSource.
   * The actual SchemaProvider instance returned by UtilHelpers.getSchemaProviderForKafkaSource will either be built from the schema registry or from the data itself, based on the value of the hoodie.deltastreamer.source.schema.registry.url
   * If this property is set, then the schema will be fetched from the schema registry, otherwise, the schema will be inferred from the data itself.
   * This SchemaProvider instance is then used throughout the AvroKafkaSource class to perform various operations on the incoming Kafka data.
   */



  public AvroKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                         SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession,
        UtilHelpers.getSchemaProviderForKafkaSource(schemaProvider, props, sparkContext),
        SourceType.AVRO, metrics);
    this.originalSchemaProvider = schemaProvider;
    /*
      Invalidate local cache of the SourceSchema.
      SHOULD(?) do nothing if not using SchemaRegistryProvider?
     */
    clearCaches(); // Is this how I just call the parent method of clear caches?


    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    deserializerClassName = props.getString(DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().key(),
        DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().defaultValue());

    try {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName).getName());
      if (deserializerClassName.equals(KafkaAvroSchemaDeserializer.class.getName())) {
        if (schemaProvider == null) {
          throw new HoodieIOException("SchemaProvider has to be set to use KafkaAvroSchemaDeserializer");
        }
        props.put(KAFKA_AVRO_VALUE_DESERIALIZER_SCHEMA, schemaProvider.getSourceSchema().toString());
      }
    } catch (ClassNotFoundException e) {
      String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
      LOG.error(error);
      throw new HoodieException(error, e);
    }
    // AvroKafkaSource construction uses the public KafkaOffsetGen constructor Method inside the class of the same name.
    // The schema is put into the props of the KafkaOffsetGen
    this.offsetGen = new KafkaOffsetGen(props);
  }

  @Override
  JavaRDD<GenericRecord> toRDD(OffsetRange[] offsetRanges) {
    JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD;
    if (deserializerClassName.equals(ByteArrayDeserializer.class.getName())) {
      if (schemaProvider == null) {
        throw new HoodieException("Please provide a valid schema provider class when use ByteArrayDeserializer!");
      }

      //Don't want kafka offsets here so we use originalSchemaProvider
      AvroConvertor convertor = new AvroConvertor(originalSchemaProvider.getSourceSchema());
      kafkaRDD = KafkaUtils.<String, byte[]>createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
          LocationStrategies.PreferConsistent()).map(obj ->
          new ConsumerRecord<>(obj.topic(), obj.partition(), obj.offset(),
              obj.key(), convertor.fromAvroBinary(obj.value())));
    } else {
      kafkaRDD = KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
          LocationStrategies.PreferConsistent());
    }
    return maybeAppendKafkaOffsets(kafkaRDD);
  }

  protected JavaRDD<GenericRecord> maybeAppendKafkaOffsets(JavaRDD<ConsumerRecord<Object, Object>> kafkaRDD) {
    if (this.shouldAddOffsets) {
      AvroConvertor convertor = new AvroConvertor(schemaProvider.getSourceSchema());
      return kafkaRDD.map(convertor::withKafkaFieldsAppended);
    } else {
      return kafkaRDD.map(consumerRecord -> (GenericRecord) consumerRecord.value());
    }
  }

  //@Override
//  public void clearCaches() {
//    SchemaProvider.clearCaches();
//  }
}

/**
 This class is responsible for reading avro serialized Kafka data, based on the confluent schema-registry.
 This class extends the KafkaSource class and provides implementations for the abstract methods declared in it.

 The class has several instance variables, including a Logger object for logging messages, a deserializerClassName string that specifies the Kafka value deserializer class name,
 and an originalSchemaProvider object of type SchemaProvider that provides the schema for the Kafka data.

 There are also several constants defined in the class that are used as settings to pass things to KafkaAvroDeserializer.
 The class has a constructor that takes TypedProperties, JavaSparkContext, SparkSession, SchemaProvider, and HoodieDeltaStreamerMetrics objects as parameters.
 The constructor calls the super constructor of the KafkaSource class and initializes some instance variables.
 It also sets some properties in the TypedProperties object based on the deserializerClassName specified in the input parameters.

 The class overrides the toRDD method of the KafkaSource class.
    This method takes an array of OffsetRange objects as input and returns a JavaRDD of GenericRecord objects.
    The method creates a JavaRDD of ConsumerRecord objects by reading data from Kafka using the KafkaUtils.createRDD method.
    If the deserializerClassName is ByteArrayDeserializer, then the method converts the binary avro data to a GenericRecord object using the AvroConvertor class.
    Finally, the method calls the maybeAppendKafkaOffsets method to append Kafka metadata fields to the GenericRecord object.

 The class also defines a protected maybeAppendKafkaOffsets method that takes a JavaRDD of ConsumerRecord objects as input and returns a JavaRDD of GenericRecord objects.
 This method appends Kafka metadata fields to the GenericRecord objects if the shouldAddOffsets instance variable is true.
    Otherwise, it simply returns the GenericRecord objects as is.
 Overall, the AvroKafkaSource class provides a way to read avro serialized Kafka data, based on the confluent schema-registry, and convert it into GenericRecord objects.


 /**
 originalSchemaProvider is a member variable of the AvroKafkaSource class that is being set in the constructor of the same class.
 When an instance of AvroKafkaSource is created, the constructor takes in a SchemaProvider object as an argument named schemaProvider, along with other parameters.
 The constructor then calls a static method UtilHelpers.getSchemaProviderForKafkaSource(schemaProvider, props, sparkContext)
 to get a new SchemaProvider object that is appropriate for a Kafka source, based on the provided schemaProvider object and the other properties and context.
 This new SchemaProvider object is then passed along with other parameters to the superclass constructor KafkaSource which sets up the necessary properties and configuration for the KafkaSource object.
 Finally, the original schemaProvider object that was passed into the constructor is assigned to the originalSchemaProvider member variable of the AvroKafkaSource object, so that it can be accessed later if needed.
 */