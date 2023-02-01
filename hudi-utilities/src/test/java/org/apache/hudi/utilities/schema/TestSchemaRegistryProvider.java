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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestSchemaRegistryProvider {

  private static final String BASIC_AUTH = "foo:bar";

  private static final String REGISTRY_RESPONSE = "{\"schema\":\"{\\\"type\\\": \\\"record\\\", \\\"namespace\\\": \\\"example\\\", "
      + "\\\"name\\\": \\\"FullName\\\",\\\"fields\\\": [{ \\\"name\\\": \\\"first\\\", \\\"type\\\": "
      + "\\\"string\\\" }]}\"}";
  private static final String CONVERTED_SCHEMA = "{\"type\": \"record\", \"namespace\": \"com.example.hoodie\", "
      + "\"name\": \"FullName\",\"fields\": [{ \"name\": \"first\", \"type\": "
      + "\"string\" }]}";

  private static Schema getExpectedSchema() {
    return new Schema.Parser().parse(CONVERTED_SCHEMA);
  }

  private static TypedProperties getProps() {
    return new TypedProperties() {
      {
        put("hoodie.deltastreamer.schemaprovider.registry.baseUrl", "http://" + BASIC_AUTH + "@localhost");
        put("hoodie.deltastreamer.schemaprovider.registry.urlSuffix", "-value");
        put("hoodie.deltastreamer.schemaprovider.registry.url", "http://foo:bar@localhost");
        put("hoodie.deltastreamer.schemaprovider.registry.schemaconverter", DummySchemaConverter.class.getName());
        put("hoodie.deltastreamer.source.kafka.topic", "foo");
      }
    };
  }

  private static SchemaRegistryProvider getUnderTest(TypedProperties props) throws IOException {
    InputStream is = new ByteArrayInputStream(REGISTRY_RESPONSE.getBytes(StandardCharsets.UTF_8));
    SchemaRegistryProvider spyUnderTest = Mockito.spy(new SchemaRegistryProvider(props, null));
    Mockito.doReturn(is).when(spyUnderTest).getStream(Mockito.any());
    return spyUnderTest;
  }

  @Test
  public void testGetSourceSchemaShouldRequestSchemaWithCreds() throws IOException {
    SchemaRegistryProvider spyUnderTest = getUnderTest(getProps());
    Schema actual = spyUnderTest.getSourceSchema();
    assertNotNull(actual);
    assertEquals(getExpectedSchema(), actual);
    verify(spyUnderTest, times(1)).setAuthorizationHeader(eq(BASIC_AUTH),
        Mockito.any(HttpURLConnection.class));
  }

  @Test
  public void testGetTargetSchemaShouldRequestSchemaWithCreds() throws IOException {
    SchemaRegistryProvider spyUnderTest = getUnderTest(getProps());
    Schema actual = spyUnderTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(getExpectedSchema(), actual);
    verify(spyUnderTest, times(1)).setAuthorizationHeader(eq(BASIC_AUTH),
        Mockito.any(HttpURLConnection.class));
  }

  @Test
  public void testGetSourceSchemaShouldRequestSchemaWithoutCreds() throws IOException {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost");
    SchemaRegistryProvider spyUnderTest = getUnderTest(props);
    Schema actual = spyUnderTest.getSourceSchema();
    assertNotNull(actual);
    assertEquals(getExpectedSchema(), actual);
    verify(spyUnderTest, times(0)).setAuthorizationHeader(Mockito.any(), Mockito.any());
  }

  @Test
  public void testGetTargetSchemaShouldRequestSchemaWithoutCreds() throws IOException {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost");
    SchemaRegistryProvider spyUnderTest = getUnderTest(props);
    Schema actual = spyUnderTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(getExpectedSchema(), actual);
    verify(spyUnderTest, times(0)).setAuthorizationHeader(Mockito.any(), Mockito.any());
  }

  public static class DummySchemaConverter implements SchemaRegistryProvider.SchemaConverter {

    @Override
    public String convert(String schema) throws IOException {
      return ((ObjectNode) new ObjectMapper()
          .readTree(schema))
          .set("namespace", TextNode.valueOf("com.example.hoodie"))
          .toString();
    }
  }
}
