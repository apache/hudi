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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests default behavior of the abstract {@link SchemaProvider} used by Hudi Streamer.
 */
public class TestSchemaProvider {

  private static final String SCHEMA_STR =
      "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";

  private static Schema parseSchema() {
    return new Schema.Parser().parse(SCHEMA_STR);
  }

  /** Provider that only implements the required source schema. */
  private static class SourceOnlyProvider extends SchemaProvider {
    private final Schema schema;

    SourceOnlyProvider(Schema schema) {
      super(new TypedProperties());
      this.schema = schema;
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }
  }

  /** Provider whose legacy target schema is unavailable, exercising the fallback path. */
  private static class TargetUnsupportedProvider extends SchemaProvider {
    private final Schema schema;

    TargetUnsupportedProvider(Schema schema) {
      super(new TypedProperties());
      this.schema = schema;
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }

    @Override
    public Schema getTargetSchema() {
      throw new UnsupportedOperationException("no target schema");
    }
  }

  @Test
  void testTargetSchemaDefaultsToSource() {
    Schema schema = parseSchema();
    SchemaProvider provider = new SourceOnlyProvider(schema);
    // By default the target schema is the source schema.
    assertSame(schema, provider.getTargetSchema());
  }

  @Test
  void testHoodieSchemaWrapsAvroSchema() {
    Schema schema = parseSchema();
    SchemaProvider provider = new SourceOnlyProvider(schema);
    HoodieSchema sourceHoodieSchema = provider.getSourceHoodieSchema();
    assertNotNull(sourceHoodieSchema);
    assertEquals(schema, sourceHoodieSchema.getAvroSchema());
    // Target Hoodie schema defaults to the source when target schema is not overridden.
    assertEquals(schema, provider.getTargetHoodieSchema().getAvroSchema());
  }

  @Test
  void testNullSchemaYieldsNullHoodieSchema() {
    SchemaProvider provider = new SourceOnlyProvider(null);
    assertNull(provider.getSourceHoodieSchema());
    assertNull(provider.getTargetHoodieSchema());
  }

  @Test
  void testTargetHoodieSchemaFallsBackToSourceOnUnsupported() {
    Schema schema = parseSchema();
    SchemaProvider provider = new TargetUnsupportedProvider(schema);
    // getTargetSchema() throws, so getTargetHoodieSchema() falls back to the source schema.
    assertEquals(schema, provider.getTargetHoodieSchema().getAvroSchema());
  }
}
