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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests the target schema of {@link HiveSchemaProvider} without a Hive metastore: the Hive-backed tests in
 * {@code TestHiveSchemaProvider} are disabled (#17258), so the schemas are passed in directly.
 */
class TestHiveSchemaProviderTargetSchema {

  private static final HoodieSchema SOURCE_SCHEMA = HoodieSchema.fromAvroSchema(new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"source\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"));
  private static final HoodieSchema TARGET_SCHEMA = HoodieSchema.fromAvroSchema(new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"target\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"ts\",\"type\":\"long\",\"default\":0}]}"));

  @Test
  @SuppressWarnings("deprecation")
  void testTargetSchemaFallsBackToSourceWithoutTargetTable() {
    HiveSchemaProvider provider = new HiveSchemaProvider(new TypedProperties(), SOURCE_SCHEMA, null);

    assertSame(SOURCE_SCHEMA, provider.getTargetHoodieSchema());
    assertEquals(SOURCE_SCHEMA.toAvroSchema(), provider.getTargetSchema());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testTargetSchemaIsTargetTableSchema() {
    HiveSchemaProvider provider = new HiveSchemaProvider(new TypedProperties(), SOURCE_SCHEMA, TARGET_SCHEMA);

    assertSame(SOURCE_SCHEMA, provider.getSourceHoodieSchema());
    assertSame(TARGET_SCHEMA, provider.getTargetHoodieSchema());
    assertEquals(TARGET_SCHEMA.toAvroSchema(), provider.getTargetSchema());
  }

  @Test
  void testPostProcessorWrapperGetsSourceAsTargetWithoutTargetTable() {
    // With a schema post-processor configured, the Hudi Streamer wraps the provider like this; a null Hive
    // target used to fail here with an NPE.
    SchemaProvider wrapped = new SchemaProviderWithPostProcessor(
        new HiveSchemaProvider(new TypedProperties(), SOURCE_SCHEMA, null),
        Option.of(new IdentityPostProcessor()));

    assertEquals(SOURCE_SCHEMA, wrapped.getTargetHoodieSchema());
  }

  private static class IdentityPostProcessor extends SchemaPostProcessor {
    IdentityPostProcessor() {
      super(new TypedProperties(), null);
    }

    @Override
    public HoodieSchema processSchema(HoodieSchema schema) {
      return schema;
    }
  }
}
