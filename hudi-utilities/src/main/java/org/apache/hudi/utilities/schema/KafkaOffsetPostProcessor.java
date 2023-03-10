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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Used internally to add Kafka offsets. You should probably not set
 * hoodie.deltastreamer.schemaprovider.schema_post_processor to this class
 * */
public class KafkaOffsetPostProcessor extends SchemaPostProcessor {

  public static class Config {
    public static final ConfigProperty<String> KAFKA_APPEND_OFFSETS = ConfigProperty
        .key("hoodie.deltastreamer.source.kafka.append.offsets")
        .defaultValue("false")
        .withDocumentation("When enabled, appends kafka offset info like source offset(_hoodie_kafka_source_offset), "
            + "partition (_hoodie_kafka_source_partition) and timestamp (_hoodie_kafka_source_timestamp) to the records. "
            + "By default its disabled and no kafka offsets are added");

    public static boolean shouldAddOffsets(TypedProperties props) {
      return  props.getBoolean(KafkaOffsetPostProcessor.Config.KAFKA_APPEND_OFFSETS.key(), Boolean.parseBoolean(KafkaOffsetPostProcessor.Config.KAFKA_APPEND_OFFSETS.defaultValue()));
    }
  }

  private static final Logger LOG = LogManager.getLogger(KafkaOffsetPostProcessor.class);

  public static final String KAFKA_SOURCE_OFFSET_COLUMN = "_hoodie_kafka_source_offset";
  public static final String KAFKA_SOURCE_PARTITION_COLUMN = "_hoodie_kafka_source_partition";
  public static final String KAFKA_SOURCE_TIMESTAMP_COLUMN = "_hoodie_kafka_source_timestamp";

  public KafkaOffsetPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public Schema processSchema(Schema schema) {
    // this method adds kafka offset fields namely source offset, partition and timestamp to the schema of the batch.
    List<Schema.Field> fieldList = schema.getFields();
    List<Schema.Field> newFieldList = fieldList.stream()
        .map(f -> new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal())).collect(Collectors.toList());
    newFieldList.add(new Schema.Field(KAFKA_SOURCE_OFFSET_COLUMN, Schema.create(Schema.Type.LONG), "offset column", 0));
    newFieldList.add(new Schema.Field(KAFKA_SOURCE_PARTITION_COLUMN, Schema.create(Schema.Type.INT), "partition column", 0));
    newFieldList.add(new Schema.Field(KAFKA_SOURCE_TIMESTAMP_COLUMN, Schema.create(Schema.Type.LONG), "timestamp column", 0));
    Schema newSchema = Schema.createRecord(schema.getName() + "_processed", schema.getDoc(), schema.getNamespace(), false, newFieldList);
    return newSchema;
  }
}