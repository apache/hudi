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

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestSchemaProviderCompatibility {

  private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

  @Test
  @SuppressWarnings("deprecation")
  void testLegacyProviderConvertsSourceAndTargetSchemas() {
    SchemaProvider provider = new SchemaProvider() {
      @Override
      public Schema getSourceSchema() {
        return AVRO_SCHEMA;
      }
    };

    assertEquals(HoodieSchema.fromAvroSchema(AVRO_SCHEMA), provider.getSourceHoodieSchema());
    assertEquals(HoodieSchema.fromAvroSchema(AVRO_SCHEMA), provider.getTargetHoodieSchema());
    assertSame(AVRO_SCHEMA, provider.getTargetSchema());
  }

  @Test
  void testModernProviderFallsBackToSourceHoodieSchemaForTarget() {
    HoodieSchema schema = HoodieSchema.fromAvroSchema(AVRO_SCHEMA);
    SchemaProvider provider = new SchemaProvider() {
      @Override
      public HoodieSchema getSourceHoodieSchema() {
        return schema;
      }
    };

    assertSame(schema, provider.getTargetHoodieSchema());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testNullLegacySchemaAndUnsupportedDefault() {
    SchemaProvider nullProvider = new SchemaProvider() {
      @Override
      public Schema getSourceSchema() {
        return null;
      }
    };
    SchemaProvider defaultProvider = new SchemaProvider() { };

    assertNull(nullProvider.getSourceHoodieSchema());
    assertNull(nullProvider.getTargetHoodieSchema());
    assertThrows(UnsupportedOperationException.class, defaultProvider::getSourceSchema);
  }
}
