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
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static org.apache.hudi.schema.SchemaProviderTestSchemas.LEGACY_SCHEMA;
import static org.apache.hudi.schema.SchemaProviderTestSchemas.SOURCE_SCHEMA;
import static org.apache.hudi.schema.SchemaProviderTestSchemas.TARGET_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link SchemaRegistryProvider}.
 */
class TestSchemaRegistryProvider {

  private static final String SOURCE_URL_KEY = "hoodie.streamer.schemaprovider.registry.url";
  private static final String TARGET_URL_KEY = "hoodie.streamer.schemaprovider.registry.targetUrl";

  @Test
  @SuppressWarnings("deprecation")
  void testReturnsSourceAndTargetSchemasFromRegistryResponses() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://source:secret@localhost/source");
    props.setProperty(TARGET_URL_KEY, "http://localhost/target");
    StubSchemaRegistryProvider provider = new StubSchemaRegistryProvider(props);

    assertEquals("SourceRecord", provider.getSourceHoodieSchema().getName());
    assertEquals("source:secret", provider.authorizationCredentials);
    assertEquals("TargetRecord", provider.getTargetHoodieSchema().getName());
    // The deprecated Avro accessors bridge to the same schemas.
    assertEquals("SourceRecord", provider.getSourceSchema().getName());
    assertEquals("TargetRecord", provider.getTargetSchema().getName());
  }

  @Test
  void testAuthorizationHeaderIsBase64Encoded() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://localhost/source");
    StubSchemaRegistryProvider provider = new StubSchemaRegistryProvider(props);
    HttpURLConnection connection = mock(HttpURLConnection.class);

    provider.setAuthorizationHeader("source:secret", connection);

    verify(connection).setRequestProperty("Authorization", "Basic c291cmNlOnNlY3JldA==");
  }

  @Test
  void testTargetDefaultsToSourceRegistry() {
    // Overriding the source getter tells the two routes apart: the target getter fetches the source url itself
    // rather than going through getSourceSchema().
    SchemaRegistryProvider provider = sourceOverridingProvider();

    assertEquals("SourceRecord", provider.getTargetHoodieSchema().getName());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testRegistryReadFailureIsWrapped() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://localhost/failure");
    StubSchemaRegistryProvider provider = new StubSchemaRegistryProvider(props);

    assertThrows(HoodieIOException.class, provider::getSourceHoodieSchema);
    assertThrows(HoodieIOException.class, provider::getTargetHoodieSchema);
    assertThrows(HoodieIOException.class, provider::getSourceSchema);
    assertThrows(HoodieIOException.class, provider::getTargetSchema);
  }

  @Test
  @SuppressWarnings("deprecation")
  void testLegacySubclassOverridesAreHonoredByHoodieSchemaGetters() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://localhost/source");
    Schema legacySchema = new Schema.Parser().parse(LEGACY_SCHEMA);

    // Legacy subclasses that only override the deprecated Avro getters.
    SchemaRegistryProvider sourceOverride = sourceOverridingProvider();
    SchemaRegistryProvider targetOverride = new StubSchemaRegistryProvider(props) {
      @Override
      public Schema getTargetSchema() {
        return legacySchema;
      }
    };

    assertEquals("LegacyRecord", sourceOverride.getSourceHoodieSchema().getName());
    assertEquals("LegacyRecord", targetOverride.getTargetHoodieSchema().getName());
  }

  /**
   * A legacy subclass, reading the source registry url, that only overrides the deprecated Avro source getter
   * to return {@code LEGACY_SCHEMA}.
   */
  @SuppressWarnings("deprecation")
  private static SchemaRegistryProvider sourceOverridingProvider() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://localhost/source");
    Schema legacySchema = new Schema.Parser().parse(LEGACY_SCHEMA);
    return new StubSchemaRegistryProvider(props) {
      @Override
      public Schema getSourceSchema() {
        return legacySchema;
      }
    };
  }

  private static class StubSchemaRegistryProvider extends SchemaRegistryProvider {
    private String authorizationCredentials;

    StubSchemaRegistryProvider(TypedProperties props) {
      super(props);
    }

    @Override
    protected void setAuthorizationHeader(String creds, HttpURLConnection connection) {
      super.setAuthorizationHeader(creds, connection);
      authorizationCredentials = creds;
    }

    @Override
    protected InputStream getStream(HttpURLConnection connection) throws IOException {
      String path = connection.getURL().getPath();
      if ("/failure".equals(path)) {
        throw new IOException("schema registry unavailable");
      }
      String schema = "/target".equals(path) ? TARGET_SCHEMA : SOURCE_SCHEMA;
      String response = "{\"schema\":" + quote(schema) + "}";
      return new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8));
    }

    private static String quote(String value) {
      return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
  }
}
