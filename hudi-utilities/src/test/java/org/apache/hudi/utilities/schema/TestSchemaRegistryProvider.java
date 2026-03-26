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
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.util.Option;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestSchemaRegistryProvider {

  private final String basicAuth = "foo:bar";

  private final String json = "{\"schema\":\"{\\\"type\\\": \\\"record\\\", \\\"namespace\\\": \\\"example\\\", "
      + "\\\"name\\\": \\\"FullName\\\",\\\"fields\\\": [{ \\\"name\\\": \\\"first\\\", \\\"type\\\": "
      + "\\\"string\\\" }]}\"}";

  private TypedProperties getProps() {
    return new TypedProperties() {{
        put("hoodie.deltastreamer.schemaprovider.registry.baseUrl", "http://" + basicAuth + "@localhost");
        put("hoodie.deltastreamer.schemaprovider.registry.urlSuffix", "-value");
        put("hoodie.deltastreamer.schemaprovider.registry.url", "http://foo:bar@localhost");
        put("hoodie.deltastreamer.source.kafka.topic", "foo");
      }
    };
  }

  private Schema getExpectedSchema(String response) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readTree(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));
    return (new Schema.Parser()).parse(node.get("schema").asText());
  }

  private SchemaRegistryProvider getUnderTest(TypedProperties props) throws IOException {
    InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    SchemaRegistryProvider spyUnderTest = Mockito.spy(new SchemaRegistryProvider(props, null));
    Mockito.doReturn(is).when(spyUnderTest).getStream(Mockito.any());
    return spyUnderTest;
  }

  @Test
  public void testGetSourceSchemaShouldRequestSchemaWithCreds() throws IOException {
    SchemaRegistryProvider spyUnderTest = getUnderTest(getProps());
    Schema actual = spyUnderTest.getSourceSchema();
    assertNotNull(actual);
    assertEquals(actual, getExpectedSchema(json));
    verify(spyUnderTest, times(1)).setAuthorizationHeader(eq(basicAuth),
        Mockito.any(HttpURLConnection.class));
  }

  @Test
  public void testGetTargetSchemaShouldRequestSchemaWithCreds() throws IOException {
    SchemaRegistryProvider spyUnderTest = getUnderTest(getProps());
    Schema actual = spyUnderTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(actual, getExpectedSchema(json));
    verify(spyUnderTest, times(1)).setAuthorizationHeader(eq(basicAuth),
        Mockito.any(HttpURLConnection.class));
  }

  @Test
  public void testGetSourceSchemaShouldRequestSchemaWithoutCreds() throws IOException {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost");
    SchemaRegistryProvider spyUnderTest = getUnderTest(props);
    Schema actual = spyUnderTest.getSourceSchema();
    assertNotNull(actual);
    assertEquals(actual, getExpectedSchema(json));
    verify(spyUnderTest, times(0)).setAuthorizationHeader(Mockito.any(), Mockito.any());
  }

  @Test
  public void testPublicConstructorForwardsConfigsToCachedSchemaRegistryClient() {
    TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url",
        "http://localhost/subjects/test/versions/latest");
    props.put("bearer.auth.credentials.source", "CUSTOM");
    props.put("bearer.auth.custom.provider.class",
        "com.example.GcpBearerAuthCredentialProvider");

    List<Map<String, Object>> capturedConfigs = new ArrayList<>();
    ParsedSchema mockParsedSchema = mock(ParsedSchema.class);
    when(mockParsedSchema.canonicalString()).thenReturn(RAW_SCHEMA);

    try (MockedConstruction<RestService> ignoredRest =
             Mockito.mockConstruction(RestService.class);
         MockedConstruction<CachedSchemaRegistryClient> ignored =
             Mockito.mockConstruction(CachedSchemaRegistryClient.class, (mock, context) -> {
               capturedConfigs.add((Map<String, Object>) context.arguments().get(3));
               when(mock.getLatestSchemaMetadata(any())).thenReturn(new SchemaMetadata(1, 1, RAW_SCHEMA));
               when(mock.parseSchema(any(), any(), any())).thenReturn(java.util.Optional.of(mockParsedSchema));
             })) {

      SchemaRegistryProvider provider = new SchemaRegistryProvider(props, null);
      provider.fetchSchemaFromRegistry("http://localhost/subjects/test/versions/latest");

      assertEquals(1, capturedConfigs.size());
      assertEquals("CUSTOM", capturedConfigs.get(0).get("bearer.auth.credentials.source"));
      assertEquals("com.example.GcpBearerAuthCredentialProvider",
          capturedConfigs.get(0).get("bearer.auth.custom.provider.class"));
    }
  }

  @Test
  public void testGetTargetSchemaShouldRequestSchemaWithoutCreds() throws IOException {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost");
    SchemaRegistryProvider spyUnderTest = getUnderTest(props);
    Schema actual = spyUnderTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(actual, getExpectedSchema(json));
    verify(spyUnderTest, times(0)).setAuthorizationHeader(Mockito.any(), Mockito.any());
  }
}