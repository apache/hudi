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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestSchemaRegistryProvider {

  private static final String BASIC_AUTH = "foo:bar";
  private static final String RAW_SCHEMA = "{\"type\": \"record\", \"namespace\": \"example\", "
      + "\"name\": \"FullName\",\"fields\": [{ \"name\": \"first\", \"type\": "
      + "\"string\" }]}";
  private static final String CONVERTED_SCHEMA = "{\"type\": \"record\", \"namespace\": \"com.example.hoodie\", "
      + "\"name\": \"FullName\",\"fields\": [{ \"name\": \"first\", \"type\": "
      + "\"string\" }]}";

  private static HoodieSchema getExpectedSchema() {
    return HoodieSchema.parse(RAW_SCHEMA);
  }

  private static HoodieSchema getExpectedConvertedSchema() {
    return HoodieSchema.parse(CONVERTED_SCHEMA);
  }

  private static TypedProperties getProps() {
    return new TypedProperties() {
      {
        put("hoodie.deltastreamer.schemaprovider.registry.baseUrl", "http://" + BASIC_AUTH + "@localhost");
        put("hoodie.deltastreamer.schemaprovider.registry.urlSuffix", "-value");
        put("hoodie.deltastreamer.schemaprovider.registry.url", "http://foo:bar@localhost/subjects/test/versions/latest");
        put("hoodie.deltastreamer.source.kafka.topic", "foo");
      }
    };
  }

  private final SchemaRegistryProvider.SchemaConverter mockSchemaConverter = mock(SchemaRegistryProvider.SchemaConverter.class);
  private final RestService mockRestService = mock(RestService.class);
  private final SchemaRegistryClient mockRegistryClient = mock(SchemaRegistryClient.class);

  private SchemaRegistryProvider getUnderTest(TypedProperties props, int version, boolean useConverter) throws Exception {
    SerializableFunctionUnchecked<String, RestService> mockRestServiceFactory = mock(SerializableFunctionUnchecked.class);
    when(mockRestServiceFactory.apply("http://localhost/")).thenReturn(mockRestService);
    SerializableFunctionUnchecked<RestService, SchemaRegistryClient> mockRegistryClientFactory = mock(SerializableFunctionUnchecked.class);
    when(mockRegistryClientFactory.apply(mockRestService)).thenReturn(mockRegistryClient);
    SchemaRegistryProvider underTest = new SchemaRegistryProvider(props, null, useConverter ? Option.of(mockSchemaConverter) : Option.empty(),
        mockRestServiceFactory, mockRegistryClientFactory);
    SchemaMetadata metadata = new SchemaMetadata(1, 1, RAW_SCHEMA);
    if (version == -1) {
      when(mockRegistryClient.getLatestSchemaMetadata("test")).thenReturn(metadata);
    } else {
      when(mockRegistryClient.getSchemaMetadata("test", version)).thenReturn(metadata);
    }
    ParsedSchema mockParsedSchema = mock(ParsedSchema.class);
    when(mockRegistryClient.parseSchema("AVRO", RAW_SCHEMA, Collections.emptyList())).thenReturn(java.util.Optional.of(mockParsedSchema));
    if (useConverter) {
      when(mockSchemaConverter.convert(mockParsedSchema)).thenReturn(CONVERTED_SCHEMA);
    } else {
      when(mockParsedSchema.canonicalString()).thenReturn(RAW_SCHEMA);
    }
    return underTest;
  }

  @Test
  public void testGetSourceSchemaShouldRequestSchemaWithCreds() throws Exception {
    SchemaRegistryProvider underTest = getUnderTest(getProps(), -1, true);
    HoodieSchema actual = underTest.getSourceSchema();
    assertNotNull(actual);
    assertEquals(getExpectedConvertedSchema(), actual);
    verify(mockRestService).setHttpHeaders(Collections.singletonMap("Authorization", "Basic " + Base64.getEncoder().encodeToString(BASIC_AUTH.getBytes())));
  }

  @Test
  public void testGetTargetSchemaShouldRequestSchemaWithCreds() throws Exception {
    SchemaRegistryProvider underTest = getUnderTest(getProps(), -1, true);
    HoodieSchema actual = underTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(getExpectedConvertedSchema(), actual);
    verify(mockRestService).setHttpHeaders(Collections.singletonMap("Authorization", "Basic " + Base64.getEncoder().encodeToString(BASIC_AUTH.getBytes())));
  }

  @Test
  public void testGetSourceSchemaShouldRequestSchemaWithoutCreds() throws Exception {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost/subjects/test/versions/latest");
    SchemaRegistryProvider underTest = getUnderTest(props, -1, true);
    HoodieSchema actual = underTest.getSourceSchema();
    assertNotNull(actual);
    assertEquals(getExpectedConvertedSchema(), actual);
    verify(mockRestService, never()).setHttpHeaders(any());
  }

  @Test
  public void testGetTargetSchemaShouldRequestSchemaWithoutCreds() throws Exception {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost/subjects/test/versions/latest");
    SchemaRegistryProvider underTest = getUnderTest(props, -1, true);
    HoodieSchema actual = underTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(getExpectedConvertedSchema(), actual);
    verify(mockRestService, never()).setHttpHeaders(any());
  }

  @Test
  public void testGetTargetSchemaWithoutConverter() throws Exception {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost/subjects/test/versions/latest");
    SchemaRegistryProvider underTest = getUnderTest(props, -1, false);
    HoodieSchema actual = underTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(getExpectedSchema(), actual);
    verify(mockRestService, never()).setHttpHeaders(any());
  }

  @Test
  public void testUrlWithSpecificSchemaVerson() throws Exception {
    TypedProperties props = getProps();
    props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost/subjects/test/versions/3");
    SchemaRegistryProvider underTest = getUnderTest(props, 3, false);
    HoodieSchema actual = underTest.getTargetSchema();
    assertNotNull(actual);
    assertEquals(getExpectedSchema(), actual);
    verify(mockRestService, never()).setHttpHeaders(any());
  }

  @Test
  public void testFallbackWhenIllegalAccessErrorThrown() throws Exception {
    TypedProperties props = getProps();
    // Create an instance with useConverter=false (so canonicalString() is used)
    SchemaRegistryProvider provider = getUnderTest(props, -1, false);
    // Simulate failure by making canonicalString() throw IllegalAccessError.
    ParsedSchema failingParsedSchema = mock(ParsedSchema.class);
    when(failingParsedSchema.canonicalString()).thenThrow(new IllegalAccessError("tried to access field org.apache.avro.Schema.FACTORY from class org.apache.avro.Schemas"));
    // Override the existing parseSchema behavior on the mock registry client so that it returns our failingParsedSchema
    when(mockRegistryClient.parseSchema("AVRO", RAW_SCHEMA, Collections.emptyList()))
        .thenReturn(java.util.Optional.of(failingParsedSchema));
    // Stub the legacy fallback method to return a known fallback schema string.
    SchemaRegistryProvider spyProvider = spy(provider);
    final String FALLBACK_SCHEMA = "{\"type\": \"record\", \"namespace\": \"example.fallback\", \"name\": \"Fallback\", \"fields\": []}";
    doReturn(FALLBACK_SCHEMA).when(spyProvider).fetchSchemaUsingLegacyMethod(anyString());
    // Invoke the method; the IllegalAccessError should be caught and fallback used.
    HoodieSchema schema = spyProvider.getSourceSchema();
    // Verify that the fallback schema is returned.
    assertEquals(HoodieSchema.parse(FALLBACK_SCHEMA), schema);
  }
}
