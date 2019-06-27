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

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import java.util.Optional;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

/**
 * Reads avro serialized Kafka data, based on the confluent schema-registry
 */
public class AvroKafkaSource extends AvroSource {

  private static Logger log = LogManager.getLogger(AvroKafkaSource.class);

  private final KafkaOffsetGen offsetGen;

  public AvroKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    offsetGen = new KafkaOffsetGen(props);
    log.info("Setting SchemaRegistry provider in SchemaProviderBasedKafkaAvroDecoder");
    SchemaProviderBasedKafkaAvroDecoder.setSchemaProvider(schemaProvider);
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Optional<String> lastCheckpointStr,
      long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    if (totalNewMsgs <= 0) {
      return new InputBatch<>(Optional.empty(),
          lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : "");
    } else {
      log.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    }
    JavaRDD<GenericRecord> newDataRDD = toRDD(offsetRanges);
    return new InputBatch<>(Optional.of(newDataRDD),
        KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  private JavaRDD<GenericRecord> toRDD(OffsetRange[] offsetRanges) {
    JavaRDD<GenericRecord> recordRDD = KafkaUtils
        .createRDD(sparkContext, String.class, Object.class, StringDecoder.class,
            SchemaProviderBasedKafkaAvroDecoder.class,
            offsetGen.getKafkaParams(), offsetRanges).values().map(obj -> (GenericRecord) obj);
    return recordRDD;
  }


  /**
   * KafkaAvroDecoder does not guarantee that it will return generic records deserialized with latest schema.
   * This causes deserialization errors during writing time when input batch has records conforming to old schema
   * versions and Hudi tries to deserialize the bytes with latest schema.
   * Hence, this class which would use SchemaProvider to create GenericRecords with latest schema from schema provider.
   */
  public static class SchemaProviderBasedKafkaAvroDecoder extends KafkaAvroDecoder {

    private static SchemaProvider schemaProvider;

    public static SchemaProvider getSchemaProvider() {
      return schemaProvider;
    }

    public static void setSchemaProvider(SchemaProvider schemaProvider) {
      SchemaProviderBasedKafkaAvroDecoder.schemaProvider = schemaProvider;
    }

    public SchemaProviderBasedKafkaAvroDecoder(SchemaRegistryClient schemaRegistry) {
      super(schemaRegistry);
      Preconditions.checkArgument(schemaProvider != null, "SchemaProvider is not set");
    }

    public SchemaProviderBasedKafkaAvroDecoder(SchemaRegistryClient schemaRegistry, VerifiableProperties props) {
      super(schemaRegistry, props);
      Preconditions.checkArgument(schemaProvider != null, "SchemaProvider is not set");
    }

    public SchemaProviderBasedKafkaAvroDecoder(VerifiableProperties props) {
      super(props);
      Preconditions.checkArgument(schemaProvider != null, "SchemaProvider is not set");
    }

    @Override
    public Object fromBytes(byte[] bytes) {
      return this.deserialize(bytes, schemaProvider.getSourceSchema());
    }

    @Override
    public Object fromBytes(byte[] bytes, Schema readerSchema) {
      return this.deserialize(bytes, readerSchema);
    }
  }
}
