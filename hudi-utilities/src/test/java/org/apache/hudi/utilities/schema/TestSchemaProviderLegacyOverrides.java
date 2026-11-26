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

import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests that the built-in schema providers, which implement the HoodieSchema getters and keep the deprecated Avro
 * getters as bridges, still honor subclasses that override only a deprecated Avro getter, and that the bridges
 * keep a null schema null.
 */
class TestSchemaProviderLegacyOverrides {

  private static final HoodieSchema SOURCE = schema("source");
  private static final HoodieSchema TARGET = schema("target");
  private static final HoodieSchema LEGACY = schema("legacy");

  @Test
  @SuppressWarnings("deprecation")
  void testLegacySourceOverrideIsHonoredByHoodieSchemaGetters() {
    SchemaProvider provider = new SimpleSchemaProvider(null, SOURCE, new TypedProperties()) {
      @Override
      public Schema getSourceSchema() {
        return LEGACY.toAvroSchema();
      }
    };

    assertEquals("legacy", provider.getSourceHoodieSchema().getName());
    // SimpleSchemaProvider has no target of its own, so the target is the (overridden) source.
    assertEquals("legacy", provider.getTargetHoodieSchema().getName());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testLegacyOverrideCallingSuperDoesNotRecurse() {
    // A legacy override that decorates the built-in schema through super: super must return the provider's own
    // schema rather than calling back into the override.
    SchemaProvider provider = new HiveSchemaProvider(new TypedProperties(), SOURCE, TARGET) {
      @Override
      public Schema getSourceSchema() {
        return renamed(super.getSourceSchema(), "decorated_source");
      }

      @Override
      public Schema getTargetSchema() {
        return renamed(super.getTargetSchema(), "decorated_target");
      }
    };

    assertEquals("decorated_source", provider.getSourceHoodieSchema().getName());
    assertEquals("decorated_target", provider.getTargetHoodieSchema().getName());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testLegacySourceOverrideIsTheTargetWithoutTargetTable() {
    SchemaProvider provider = new HiveSchemaProvider(new TypedProperties(), SOURCE, null) {
      @Override
      public Schema getSourceSchema() {
        return LEGACY.toAvroSchema();
      }
    };

    assertEquals("legacy", provider.getTargetHoodieSchema().getName());
    assertEquals("legacy", provider.getTargetSchema().getName());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testHoodieSchemaOverrideIsHonoredByAvroGetters() {
    SchemaProvider provider = new HiveSchemaProvider(new TypedProperties(), SOURCE, TARGET) {
      @Override
      public HoodieSchema getSourceHoodieSchema() {
        return LEGACY;
      }
    };

    assertEquals("legacy", provider.getSourceSchema().getName());
    assertEquals("target", provider.getTargetSchema().getName());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testNullTargetStaysNullThroughAvroGetter() {
    SchemaProvider provider = new NullTargetSchemaRegistryProvider(registryProps(), null);

    assertNull(provider.getTargetHoodieSchema());
    assertNull(provider.getTargetSchema());
  }

  @Test
  void testPostProcessorKeepsNullTargetNull() {
    SchemaProvider wrapped = new SchemaProviderWithPostProcessor(
        new NullTargetSchemaRegistryProvider(registryProps(), null), Option.of(new IdentityPostProcessor()));

    assertNull(wrapped.getTargetHoodieSchema());
  }

  private static TypedProperties registryProps() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.schemaprovider.registry.url", "http://localhost/subjects/test/versions/latest");
    return props;
  }

  private static HoodieSchema schema(String name) {
    return HoodieSchema.fromAvroSchema(new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"));
  }

  private static Schema renamed(Schema schema, String name) {
    return Schema.createRecord(name, schema.getDoc(), schema.getNamespace(), schema.isError(),
        schema.getFields().stream().map(f -> new Schema.Field(f, f.schema())).collect(Collectors.toList()));
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
