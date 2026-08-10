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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestKafkaOffsetPostProcessor {
  private static final List<String>
      EXPECTED_FIELD_NAMES = Arrays.asList("existing_field", "_hoodie_kafka_source_offset", "_hoodie_kafka_source_partition", "_hoodie_kafka_source_timestamp", "_hoodie_kafka_source_key");

  @ParameterizedTest
  @MethodSource("cases")
  void testProcessSchema(HoodieSchema inputSchema) {
    KafkaOffsetPostProcessor kafkaOffsetPostProcessor = new KafkaOffsetPostProcessor(null, null);
    HoodieSchema actual = kafkaOffsetPostProcessor.processSchema(inputSchema);
    List<String> actualFieldNames = actual.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList());
    assertEquals(EXPECTED_FIELD_NAMES, actualFieldNames);
  }

  private static Stream<Arguments> cases() {
    String offsetField = "{\"name\": \"_hoodie_kafka_source_offset\", \"type\": \"long\", \"doc\": \"offset column\", \"default\": 0}";
    String partitionField = "{\"name\": \"_hoodie_kafka_source_partition\", \"type\": \"int\", \"doc\": \"partition column\", \"default\": 0}";
    String timestampField = "{\"name\": \"_hoodie_kafka_source_timestamp\", \"type\": \"long\", \"doc\": \"timestamp column\", \"default\": 0}";
    String keyField = "{\"name\": \"_hoodie_kafka_source_key\", \"type\": [\"null\", \"string\"], \"doc\": \"kafka key column\", \"default\": null}";
    return Stream.of(
        Arguments.of(HoodieSchema.parse("{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"existing_field\", \"type\": \"string\"}]}")),
        Arguments.of(HoodieSchema.parse("{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"existing_field\", \"type\": \"string\"}, "
                + offsetField + "]}")),
        Arguments.of(HoodieSchema.parse("{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"existing_field\", \"type\": \"string\"}, "
                + offsetField + ", " + partitionField + "]}")),
        Arguments.of(
            HoodieSchema.parse("{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"existing_field\", \"type\": \"string\"}, "
                + offsetField + ", " + partitionField + ", " + timestampField + "]}")),
        Arguments.of(
            HoodieSchema.parse("{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"existing_field\", \"type\": \"string\"}, "
                + offsetField + ", " + partitionField + ", " + timestampField + ", " + keyField + "]}"))
    );
  }
}
