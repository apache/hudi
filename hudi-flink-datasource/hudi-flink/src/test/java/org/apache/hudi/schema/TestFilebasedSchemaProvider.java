/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.hudi.schema.SchemaProviderTestSchemas.LEGACY_SCHEMA;
import static org.apache.hudi.schema.SchemaProviderTestSchemas.SOURCE_SCHEMA;
import static org.apache.hudi.schema.SchemaProviderTestSchemas.TARGET_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link FilebasedSchemaProvider}.
 */
class TestFilebasedSchemaProvider {

  private static final String SOURCE_SCHEMA_KEY = "hoodie.streamer.schemaprovider.source.schema.file";
  private static final String TARGET_SCHEMA_KEY = "hoodie.streamer.schemaprovider.target.schema.file";

  @TempDir
  Path tempDir;

  @Test
  void testConfigurationConstructorReturnsSourceSchema() throws IOException {
    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(sourceOnlyConf());

    assertSchemaNames(provider, "SourceRecord", "SourceRecord");
  }

  @Test
  @SuppressWarnings("deprecation")
  void testTypedPropertiesConstructorReturnsSeparateTargetSchema() throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_SCHEMA_KEY, writeSchema("source.avsc", SOURCE_SCHEMA).toString());
    props.setProperty(TARGET_SCHEMA_KEY, writeSchema("target.avsc", TARGET_SCHEMA).toString());

    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(props);

    assertSchemaNames(provider, "SourceRecord", "TargetRecord");
  }

  @Test
  @SuppressWarnings("deprecation")
  void testTypedPropertiesConstructorWithoutTargetUsesSourceSchema() throws IOException {
    // Source file only: the default configuration of the Kafka Connect sink.
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_SCHEMA_KEY, writeSchema("source.avsc", SOURCE_SCHEMA).toString());

    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(props);

    assertSchemaNames(provider, "SourceRecord", "SourceRecord");
  }

  @Test
  @SuppressWarnings("deprecation")
  void testLegacySubclassSourceOverrideIsHonoredByAllGetters() throws IOException {
    Schema legacySchema = new Schema.Parser().parse(LEGACY_SCHEMA);

    // A legacy subclass that only overrides the deprecated Avro source getter.
    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(sourceOnlyConf()) {
      @Override
      public Schema getSourceSchema() {
        return legacySchema;
      }
    };

    // The HoodieSchema getters and, with no target file, the target getters all go through the override.
    assertEquals("LegacyRecord", provider.getSourceHoodieSchema().getName());
    assertEquals("LegacyRecord", provider.getTargetHoodieSchema().getName());
    assertEquals("LegacyRecord", provider.getTargetSchema().getName());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testLegacySubclassTargetOverrideIsHonoredByHoodieSchemaGetter() throws IOException {
    Schema legacySchema = new Schema.Parser().parse(LEGACY_SCHEMA);

    // A legacy subclass that only overrides the deprecated Avro target getter.
    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(sourceOnlyConf()) {
      @Override
      public Schema getTargetSchema() {
        return legacySchema;
      }
    };

    assertEquals("LegacyRecord", provider.getTargetHoodieSchema().getName());
  }

  @Test
  void testReadFailureIsWrapped() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, tempDir.resolve("missing.avsc").toString());

    assertThrows(HoodieIOException.class, () -> new FilebasedSchemaProvider(conf));
  }

  @SuppressWarnings("deprecation")
  private static void assertSchemaNames(SchemaProvider provider, String sourceName, String targetName) {
    assertEquals(sourceName, provider.getSourceHoodieSchema().getName());
    assertEquals(sourceName, provider.getSourceSchema().getName());
    assertEquals(targetName, provider.getTargetHoodieSchema().getName());
    assertEquals(targetName, provider.getTargetSchema().getName());
  }

  private Configuration sourceOnlyConf() throws IOException {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, writeSchema("source.avsc", SOURCE_SCHEMA).toString());
    return conf;
  }

  private Path writeSchema(String fileName, String schema) throws IOException {
    Path path = tempDir.resolve(fileName);
    Files.write(path, schema.getBytes(StandardCharsets.UTF_8));
    return path;
  }
}
