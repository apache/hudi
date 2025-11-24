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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;

import org.apache.avro.JsonProperties;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

/**
 * Used internally to add Kafka offsets. You should probably not set
 * hoodie.streamer.schemaprovider.schema_post_processor to this class
 */
public class KafkaOffsetPostProcessor extends SchemaPostProcessor {

  public static class Config {
    @Deprecated
    public static final ConfigProperty<String> KAFKA_APPEND_OFFSETS =
        HoodieStreamerConfig.KAFKA_APPEND_OFFSETS;

    public static boolean shouldAddOffsets(TypedProperties props) {
      return getBooleanWithAltKeys(props, HoodieStreamerConfig.KAFKA_APPEND_OFFSETS);
    }
  }

  public static final String KAFKA_SOURCE_OFFSET_COLUMN = "_hoodie_kafka_source_offset";
  public static final String KAFKA_SOURCE_PARTITION_COLUMN = "_hoodie_kafka_source_partition";
  public static final String KAFKA_SOURCE_TIMESTAMP_COLUMN = "_hoodie_kafka_source_timestamp";
  public static final String KAFKA_SOURCE_KEY_COLUMN = "_hoodie_kafka_source_key";

  public KafkaOffsetPostProcessor(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
  }

  @Override
  public HoodieSchema processSchema(HoodieSchema schema) {
    // this method adds kafka offset fields namely source offset, partition, timestamp and kafka message key to the schema of the batch.
    List<HoodieSchemaField> fieldList = schema.getFields();
    Set<String> fieldNames = fieldList.stream().map(HoodieSchemaField::name).collect(Collectors.toSet());
    // if the source schema already contains the kafka offset fields, then return the schema as is.
    if (fieldNames.containsAll(Arrays.asList(KAFKA_SOURCE_OFFSET_COLUMN, KAFKA_SOURCE_PARTITION_COLUMN, KAFKA_SOURCE_TIMESTAMP_COLUMN, KAFKA_SOURCE_KEY_COLUMN))) {
      return schema;
    }
    try {
      List<HoodieSchemaField> newFieldList = fieldList.stream()
          .map(HoodieSchemaUtils::createNewSchemaField).collect(Collectors.toList());
      // handle case where source schema provider may have already set 1 or more of these fields
      if (!fieldNames.contains(KAFKA_SOURCE_OFFSET_COLUMN)) {
        newFieldList.add(HoodieSchemaField.of(KAFKA_SOURCE_OFFSET_COLUMN, HoodieSchema.create(HoodieSchemaType.LONG), "offset column", 0));
      }
      if (!fieldNames.contains(KAFKA_SOURCE_PARTITION_COLUMN)) {
        newFieldList.add(HoodieSchemaField.of(KAFKA_SOURCE_PARTITION_COLUMN, HoodieSchema.create(HoodieSchemaType.INT), "partition column", 0));
      }
      if (!fieldNames.contains(KAFKA_SOURCE_TIMESTAMP_COLUMN)) {
        newFieldList.add(HoodieSchemaField.of(KAFKA_SOURCE_TIMESTAMP_COLUMN, HoodieSchema.create(HoodieSchemaType.LONG), "timestamp column", 0));
      }
      if (!fieldNames.contains(KAFKA_SOURCE_KEY_COLUMN)) {
        newFieldList.add(HoodieSchemaField.of(KAFKA_SOURCE_KEY_COLUMN, HoodieSchema.createNullableSchema(HoodieSchema.create(HoodieSchemaType.STRING)),
            "kafka key column", JsonProperties.NULL_VALUE));
      }
      return HoodieSchema.createRecord(schema.getName() + "_processed", schema.getDoc().orElse(null), schema.getNamespace().orElse(null), false, newFieldList);
    } catch (Exception e) {
      throw new HoodieSchemaException("Kafka offset post processor failed with schema: " + schema, e);
    }

  }
}