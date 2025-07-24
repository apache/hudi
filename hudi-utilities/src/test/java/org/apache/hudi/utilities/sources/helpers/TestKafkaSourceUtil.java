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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.hash.HashID;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import com.google.crypto.tink.subtle.Base64;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_VALUE_DESERIALIZER_SCHEMA;
import static org.apache.hudi.utilities.sources.helpers.KafkaSourceUtil.GROUP_ID_MAX_BYTES_LENGTH;
import static org.apache.hudi.utilities.sources.helpers.KafkaSourceUtil.NATIVE_KAFKA_CONSUMER_GROUP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestKafkaSourceUtil {

  @Mock
  SchemaProvider schemaProvider;

  @Test
  void testConfigureSchemaDeserializer() {
    TypedProperties props = new TypedProperties();
    // should throw exception when schema provider is null.
    assertThrows(HoodieReadFromSourceException.class, () -> KafkaSourceUtil.configureSchemaDeserializer(schemaProvider, props));

    String avroSchemaJson =
        "{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},"
            + "{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"isEmployed\",\"type\":\"boolean\"}]}";
    Schema schema = new Schema.Parser().parse(avroSchemaJson);
    when(schemaProvider.getSourceSchema()).thenReturn(schema);
    KafkaSourceUtil.configureSchemaDeserializer(schemaProvider, props);
    assertTrue(props.containsKey(NATIVE_KAFKA_CONSUMER_GROUP_ID));
    assertTrue(props.getString(NATIVE_KAFKA_CONSUMER_GROUP_ID, "").length() <= GROUP_ID_MAX_BYTES_LENGTH);
    assertEquals(props.getString(KAFKA_VALUE_DESERIALIZER_SCHEMA.key()), avroSchemaJson);
    String schemaHash = Base64.encode(HashID.hash(avroSchemaJson, HashID.Size.BITS_128));
    assertEquals(props.getString(NATIVE_KAFKA_CONSUMER_GROUP_ID, ""), schemaHash);
  }

  @Test
  public void testFilterKafkaParameters() {
    Map<String, Object> kafkaParams = new HashMap<>();

    kafkaParams.put("custom1.config.streamer", "offer");
    kafkaParams.put("boostrap.servers", "dns:port");
    kafkaParams.put("custom2.config.capture", "s3://folder1");
    kafkaParams.put("custom1config.sourceprofile.refresh.mode", "ENABLED");
    // Case 1: No prefixes are configured.
    assertEquals(kafkaParams, KafkaSourceUtil.filterKafkaParameters(kafkaParams, ""));
    // Case 2: Ensure only the appropriate configs are filtered out.
    Map<String, Object> filteredParams = KafkaSourceUtil.filterKafkaParameters(kafkaParams, "custom1,custom2");
    Map<String, Object> expectedParams = new HashMap<>();
    expectedParams.put("boostrap.servers", "dns:port");
    assertEquals(expectedParams, filteredParams);
    // Case 3: There are no configs with the given prefixes.
    assertEquals(kafkaParams, KafkaSourceUtil.filterKafkaParameters(kafkaParams, "custom3,custom4"));
  }
}
