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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link SchemaRegistryProvider}.
 */
class TestSchemaRegistryProvider {

  private static final String SOURCE_URL_KEY = "hoodie.streamer.schemaprovider.registry.url";
  private static final String TARGET_URL_KEY = "hoodie.streamer.schemaprovider.registry.targetUrl";
  private static final String SOURCE_SCHEMA =
      "{\"type\":\"record\",\"name\":\"SourceRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
  private static final String TARGET_SCHEMA =
      "{\"type\":\"record\",\"name\":\"TargetRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\",\"default\":0}]}";

  @Test
  void testReturnsSourceAndTargetSchemasFromRegistryResponses() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://source:secret@localhost/source");
    props.setProperty(TARGET_URL_KEY, "http://localhost/target");
    StubSchemaRegistryProvider provider = new StubSchemaRegistryProvider(props);

    assertEquals("SourceRecord", provider.getSourceSchema().getName());
    assertEquals("source:secret", provider.authorizationCredentials);
    assertEquals("TargetRecord", provider.getTargetSchema().getName());
    assertEquals("TargetRecord", provider.getTargetHoodieSchema().getName());
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
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://localhost/source");
    StubSchemaRegistryProvider provider = new StubSchemaRegistryProvider(props);

    Schema targetSchema = provider.getTargetSchema();

    assertEquals("SourceRecord", targetSchema.getName());
    assertNotNull(provider.getTargetHoodieSchema());
  }

  @Test
  void testRegistryReadFailureIsWrapped() {
    TypedProperties props = new TypedProperties();
    props.setProperty(SOURCE_URL_KEY, "http://localhost/failure");
    StubSchemaRegistryProvider provider = new StubSchemaRegistryProvider(props);

    assertThrows(HoodieIOException.class, provider::getSourceSchema);
    assertThrows(HoodieIOException.class, provider::getTargetSchema);
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
