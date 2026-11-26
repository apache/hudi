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

package org.apache.hudi.connect.writers;

import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.schema.SchemaProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link KafkaConnectWriterProvider} loads the sink's default schema provider, and that it supplies the
 * schema through {@link SchemaProvider#getSourceHoodieSchema()}, the only getter the sink calls.
 */
class TestKafkaConnectDefaultSchemaProvider {

  private static final String SOURCE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"SourceRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  void testDefaultSchemaProviderReadsSourceSchemaFile(@TempDir Path tempDir) throws IOException {
    Path schemaFile = tempDir.resolve("source.avsc");
    Files.write(schemaFile, SOURCE_SCHEMA.getBytes(StandardCharsets.UTF_8));
    KafkaConnectConfigs configs = KafkaConnectConfigs.newBuilder()
        .withProperties(Collections.singletonMap(
            "hoodie.streamer.schemaprovider.source.schema.file", schemaFile.toString()))
        .build();

    assertEquals(FilebasedSchemaProvider.class.getName(), configs.getSchemaProviderClass());
    SchemaProvider provider = KafkaConnectWriterProvider.createSchemaProvider(configs);

    assertEquals("SourceRecord", provider.getSourceHoodieSchema().getName());
  }

  @Test
  void testEmptySchemaProviderClassIsRejected() {
    KafkaConnectConfigs configs = KafkaConnectConfigs.newBuilder()
        .withProperties(Collections.singletonMap(KafkaConnectConfigs.SCHEMA_PROVIDER_CLASS.key(), ""))
        .build();

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> KafkaConnectWriterProvider.createSchemaProvider(configs));
    assertTrue(e.getMessage().contains(KafkaConnectConfigs.SCHEMA_PROVIDER_CLASS.key()), e.getMessage());
  }
}
