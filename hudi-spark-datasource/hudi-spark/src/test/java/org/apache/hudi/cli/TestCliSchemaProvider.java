/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cli;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests the CLI bootstrap {@link SchemaProvider} ({@code org.apache.hudi.cli}), which is distinct from the Hudi
 * Streamer one ({@code org.apache.hudi.utilities.schema.SchemaProvider}). Its only in-repo consumer is
 * {@link BootstrapExecutorUtils}, which calls the legacy {@link SchemaProvider#getTargetSchema()}; the
 * {@link HoodieSchema} accessors and the target fallback are public API for out-of-tree providers and are pinned
 * here directly.
 */
public class TestCliSchemaProvider {

  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");

  @Test
  @SuppressWarnings("deprecation")
  void testLegacyProviderConvertsSourceAndTargetSchemas() {
    SchemaProvider provider = new SchemaProvider(new TypedProperties()) {
      @Override
      public Schema getSourceSchema() {
        return AVRO_SCHEMA;
      }
    };

    // The target schema defaults to the source schema, and both are wrapped into HoodieSchema.
    assertSame(AVRO_SCHEMA, provider.getTargetSchema());
    assertEquals(HoodieSchema.fromAvroSchema(AVRO_SCHEMA), provider.getSourceHoodieSchema());
    assertEquals(HoodieSchema.fromAvroSchema(AVRO_SCHEMA), provider.getTargetHoodieSchema());
  }

  @Test
  void testModernProviderFallsBackToSourceHoodieSchemaForTarget() {
    HoodieSchema schema = HoodieSchema.fromAvroSchema(AVRO_SCHEMA);
    SchemaProvider provider = new SchemaProvider(new TypedProperties()) {
      @Override
      public Schema getSourceSchema() {
        throw new UnsupportedOperationException("legacy accessor not implemented");
      }

      @Override
      public HoodieSchema getSourceHoodieSchema() {
        return schema;
      }
    };

    // The default getTargetSchema() delegates to the throwing getSourceSchema(), so the target falls back to
    // the overridden source HoodieSchema.
    assertSame(schema, provider.getTargetHoodieSchema());
  }

  @Test
  void testNullSourceSchemaYieldsNullHoodieSchemas() {
    SchemaProvider provider = new SchemaProvider(new TypedProperties()) {
      @Override
      public Schema getSourceSchema() {
        return null;
      }
    };

    assertNull(provider.getSourceHoodieSchema());
    assertNull(provider.getTargetHoodieSchema());
  }
}
