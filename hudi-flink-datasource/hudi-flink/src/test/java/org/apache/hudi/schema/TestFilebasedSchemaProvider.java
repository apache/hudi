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

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link FilebasedSchemaProvider}.
 */
class TestFilebasedSchemaProvider {

  private static final String SOURCE_SCHEMA_KEY = "hoodie.streamer.schemaprovider.source.schema.file";
  private static final String TARGET_SCHEMA_KEY = "hoodie.streamer.schemaprovider.target.schema.file";
  private static final String SOURCE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"SourceRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
  private static final String TARGET_SCHEMA =
      "{\"type\":\"record\",\"name\":\"TargetRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\",\"default\":0}]}";

  @TempDir
  Path tempDir;

  @Test
  void testConfigurationConstructorReturnsSourceSchema() throws IOException {
    Path sourcePath = writeSchema("source.avsc", SOURCE_SCHEMA);
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, sourcePath.toString());

    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(conf);

    assertEquals("SourceRecord", provider.getSourceSchema().getName());
    assertEquals("SourceRecord", provider.getTargetSchema().getName());
  }

  @Test
  void testTypedPropertiesConstructorReturnsSeparateTargetSchema() throws IOException {
    Path sourcePath = writeSchema("source.avsc", SOURCE_SCHEMA);
    Path targetPath = writeSchema("target.avsc", TARGET_SCHEMA);
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_SCHEMA_KEY, sourcePath.toString());
    props.setProperty(TARGET_SCHEMA_KEY, targetPath.toString());

    FilebasedSchemaProvider provider = new FilebasedSchemaProvider(props);

    assertEquals("SourceRecord", provider.getSourceSchema().getName());
    assertEquals("TargetRecord", provider.getTargetSchema().getName());
  }

  @Test
  void testReadFailureIsWrapped() {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH, tempDir.resolve("missing.avsc").toString());

    assertThrows(HoodieIOException.class, () -> new FilebasedSchemaProvider(conf));
  }

  private Path writeSchema(String fileName, String schema) throws IOException {
    Path path = tempDir.resolve(fileName);
    Files.write(path, schema.getBytes(StandardCharsets.UTF_8));
    return path;
  }
}
