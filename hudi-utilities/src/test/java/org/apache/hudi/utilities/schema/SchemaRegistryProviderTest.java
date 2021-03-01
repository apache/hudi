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

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.helpers.AvroKafkaSourceHelpers;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryProviderTest {

  final static String AVRO_SCHEMA = "{\n" +
      "     \"type\": \"record\",\n" +
      "     \"namespace\": \"com.example\",\n" +
      "     \"name\": \"FullName\",\n" +
      "     \"fields\": [\n" +
      "       { \"name\": \"first\", \"type\": \"string\" },\n" +
      "       { \"name\": \"last\", \"type\": \"string\" }\n" +
      "     ]\n" +
      "}";

  TypedProperties initProps() {
    TypedProperties tp = new TypedProperties();
    tp.put("hoodie.deltastreamer.schemaprovider.registry.url", "sourceUrl");
    tp.put("hoodie.deltastreamer.schemaprovider.registry.targetUrl", "targetUrl");
    tp.put("hoodie.deltastreamer.schemaprovider.registry.cache_enabled", "false");
    tp.put(AvroKafkaSourceHelpers.INJECT_KAFKA_META_FIELDS, "false");
    return tp;
  }

  @Test
  void getSourceSchemaNoCacheNoKafka() throws IOException {
    TypedProperties tp = initProps();
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));
    doReturn(AVRO_SCHEMA).when(srp).fetchSchemaFromRegistry("sourceUrl");
    Schema sc1 = srp.getSourceSchema();
    Schema sc2 = srp.getSourceSchema();
    verify(srp, times(2)).fetchSchemaFromRegistry("sourceUrl");
    assertEquals(sc1, sc2);
  }

  @Test
  void getTargetSchemaNoCacheNoKafka() throws IOException {
    TypedProperties tp = initProps();
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));
    doReturn(AVRO_SCHEMA).when(srp).fetchSchemaFromRegistry("targetUrl");
    Schema sc1 = srp.getTargetSchema();
    Schema sc2 = srp.getTargetSchema();
    verify(srp, times(2)).fetchSchemaFromRegistry("targetUrl");
    assertEquals(sc1, sc2);
  }

  @Test
  void getSourceSchemaWithCacheNoKafka() throws IOException {
    TypedProperties tp = initProps();
    tp.put("hoodie.deltastreamer.schemaprovider.registry.cache_enabled", "true");
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));
    doReturn(AVRO_SCHEMA).when(srp).fetchSchemaFromRegistry("sourceUrl");
    Schema sc1 = srp.getSourceSchema();
    Schema sc2 = srp.getSourceSchema();
    verify(srp, times(1)).fetchSchemaFromRegistry("sourceUrl");
    assertEquals(sc1, sc2);
  }

  @Test
  void getTargetSchemaWithCacheNoKafka() throws IOException {
    TypedProperties tp = initProps();
    tp.put("hoodie.deltastreamer.schemaprovider.registry.cache_enabled", "true");
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));
    doReturn(AVRO_SCHEMA).when(srp).fetchSchemaFromRegistry("targetUrl");
    Schema sc1 = srp.getTargetSchema();
    Schema sc2 = srp.getTargetSchema();
    verify(srp, times(1)).fetchSchemaFromRegistry("targetUrl");
    assertEquals(sc1, sc2);
  }

  void assertFieldExistsInSchema(Schema sc, String fieldName) {
    for (Schema.Field f : sc.getFields()) {
      if (f.name().equals(fieldName)) {
        return;
      }
    }
    fail(String.format("No '%s' field found in schema %s", fieldName, sc));
  }

  @Test
  void getSourceSchemaWithCacheAndKafka() throws IOException {
    TypedProperties tp = initProps();
    tp.put("hoodie.deltastreamer.schemaprovider.registry.cache_enabled", "true");
    tp.put(AvroKafkaSourceHelpers.INJECT_KAFKA_META_FIELDS, "true");
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));
    doReturn(AVRO_SCHEMA).when(srp).fetchSchemaFromRegistry("sourceUrl");
    Schema sc1 = srp.getSourceSchema();
    Schema sc2 = srp.getSourceSchema();
    assertEquals(sc1, sc2);
    System.out.println(sc1);
    verify(srp, times(1)).fetchSchemaFromRegistry("sourceUrl");
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_KEY_META_FIELD);
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_OFFSET_META_FIELD);
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_PARTITION_META_FIELD);
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_TOPIC_META_FIELD);
  }

  @Test
  void getTargetSchemaWithCacheAndKafka() throws IOException {
    TypedProperties tp = initProps();
    tp.put("hoodie.deltastreamer.schemaprovider.registry.cache_enabled", "true");
    tp.put(AvroKafkaSourceHelpers.INJECT_KAFKA_META_FIELDS, "true");
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));
    doReturn(AVRO_SCHEMA).when(srp).fetchSchemaFromRegistry("targetUrl");
    Schema sc1 = srp.getTargetSchema();
    Schema sc2 = srp.getTargetSchema();
    assertEquals(sc1, sc2);
    System.out.println(sc1);
    verify(srp, times(1)).fetchSchemaFromRegistry("targetUrl");
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_KEY_META_FIELD);
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_OFFSET_META_FIELD);
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_PARTITION_META_FIELD);
    assertFieldExistsInSchema(sc1, AvroKafkaSourceHelpers.KAFKA_TOPIC_META_FIELD);
  }

  @Test
  void getNullTargetSchema() throws IOException {
    TypedProperties tp = initProps();
    tp.put("hoodie.deltastreamer.schemaprovider.registry.targetUrl", "null");
    SchemaRegistryProvider srp = spy(new SchemaRegistryProvider(tp, null));

    Schema sc = srp.getTargetSchema();
    assertNull(sc);
  }
}
