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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AvroKafkaSourceHelpers {

  public static final String INJECT_KAFKA_META_FIELDS = "hoodie.deltastreamer.source.inject_kafka_fields";

  public static final String KAFKA_PARTITION_META_FIELD = "_hudi_kafka_partition";
  public static final String KAFKA_OFFSET_META_FIELD = "_hudi_kafka_offset";
  public static final String KAFKA_TOPIC_META_FIELD = "_hudi_kafka_topic";
  public static final String KAFKA_KEY_META_FIELD = "_hudi_kafka_key";
  private static final String KAFKA_META_FIELDS_PATTERN = "<KAFKA_FIELDS>";

  private static final String ALL_KAFKA_META_FIELDS = String.join(
      ",",
      AvroKafkaSourceHelpers.KAFKA_PARTITION_META_FIELD,
      AvroKafkaSourceHelpers.KAFKA_OFFSET_META_FIELD,
      AvroKafkaSourceHelpers.KAFKA_TOPIC_META_FIELD,
      AvroKafkaSourceHelpers.KAFKA_KEY_META_FIELD);

  public static String transform(String sql) {
    return sql.replaceAll(KAFKA_META_FIELDS_PATTERN, ALL_KAFKA_META_FIELDS);
  }

  public static GenericRecord addKafkaFields(ConsumerRecord<Object, Object> obj) {
    GenericRecord record = (GenericRecord) obj.value();
    record.put(AvroKafkaSourceHelpers.KAFKA_OFFSET_META_FIELD, obj.offset());
    record.put(AvroKafkaSourceHelpers.KAFKA_PARTITION_META_FIELD, obj.partition());
    record.put(AvroKafkaSourceHelpers.KAFKA_TOPIC_META_FIELD, obj.topic());
    record.put(AvroKafkaSourceHelpers.KAFKA_KEY_META_FIELD, obj.key());
    return record;
  }

  private static boolean isKafkaMetadataField(String fieldName) {
    return AvroKafkaSourceHelpers.KAFKA_PARTITION_META_FIELD.equals(fieldName)
        || AvroKafkaSourceHelpers.KAFKA_OFFSET_META_FIELD.equals(fieldName)
        || AvroKafkaSourceHelpers.KAFKA_TOPIC_META_FIELD.equals(fieldName)
        || AvroKafkaSourceHelpers.KAFKA_KEY_META_FIELD.equals(fieldName);
  }

  /**
   * Adds the Kafka metadata fields to the given schema, so later on the appropriate data can be injected.
   */
  public static Schema addKafkaMetadataFields(Schema schema) {

    final List<Schema.Field> parentFields = new ArrayList<>();
    final List<Schema.Field> schemaFields = schema.getFields();

    for (Schema.Field field : schemaFields) {
      if (!isKafkaMetadataField(field.name())) {
        Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
        for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
          newField.addProp(prop.getKey(), prop.getValue());
        }
        parentFields.add(newField);
      }
    }

    final Schema.Field partitionField =
        new Schema.Field(
            AvroKafkaSourceHelpers.KAFKA_PARTITION_META_FIELD,
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))),
            "",
            Schema.NULL_VALUE);
    final Schema.Field offsetField =
        new Schema.Field(
            AvroKafkaSourceHelpers.KAFKA_OFFSET_META_FIELD,
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG))),
            "",
            Schema.NULL_VALUE);
    final Schema.Field topicField =
        new Schema.Field(
            AvroKafkaSourceHelpers.KAFKA_TOPIC_META_FIELD,
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))),
            "",
            Schema.NULL_VALUE);
    final Schema.Field keyField =
        new Schema.Field(
            AvroKafkaSourceHelpers.KAFKA_KEY_META_FIELD,
            Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))),
            "",
            Schema.NULL_VALUE);

    parentFields.add(partitionField);
    parentFields.add(offsetField);
    parentFields.add(topicField);
    parentFields.add(keyField);

    final Schema mergedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    mergedSchema.setFields(parentFields);
    return mergedSchema;
  }
}
