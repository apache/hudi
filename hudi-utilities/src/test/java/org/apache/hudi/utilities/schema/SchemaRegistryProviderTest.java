package org.apache.hudi.utilities.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SchemaRegistryProviderTest {

    String basicAuth = "foo:bar";

    private final String json = "{\"schema\":\"{\\\"type\\\": \\\"record\\\", \\\"namespace\\\": \\\"example\\\", " +
            "\\\"name\\\": \\\"FullName\\\",\\\"fields\\\": [{ \\\"name\\\": \\\"first\\\", \\\"type\\\": " +
            "\\\"string\\\" }]}\"}";

    private TypedProperties getProps() {
        return new TypedProperties() {{
            put("hoodie.deltastreamer.schemaprovider.registry.baseUrl", "http://" + basicAuth + "@localhost");
            put("hoodie.deltastreamer.schemaprovider.registry.urlSuffix", "-value");
            put("hoodie.deltastreamer.schemaprovider.registry.url", "http://foo:bar@localhost");
            put("hoodie.deltastreamer.source.kafka.topic", "foo");
        }};
    }

    Schema getExpectedSchema(String response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));
        return (new Schema.Parser()).parse(node.get("schema").asText());
    }

    @Test
    public void testGetSourceSchemaShouldRequestSchemaWithCreds() throws IOException {
        InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        SchemaRegistryProvider underTest = new SchemaRegistryProvider(getProps(), null);
        SchemaRegistryProvider spyUnderTest = Mockito.spy(underTest);
        Mockito.doReturn(is).when(spyUnderTest).getStream(Mockito.any());
        Schema actual = spyUnderTest.getSourceSchema();
        assertNotNull(actual);
        assertEquals(actual, getExpectedSchema(json));
        verify(spyUnderTest, times(1)).setAuthorizationHeader(eq(basicAuth),
                Mockito.any(HttpURLConnection.class));
    }

    @Test
    public void testGetTargetSchemaShouldRequestSchemaWithCreds() throws IOException {
        InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        SchemaRegistryProvider underTest = new SchemaRegistryProvider(getProps(), null);
        SchemaRegistryProvider spyUnderTest = Mockito.spy(underTest);
        Mockito.doReturn(is).when(spyUnderTest).getStream(Mockito.any());
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
        InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        SchemaRegistryProvider underTest = new SchemaRegistryProvider(props, null);
        SchemaRegistryProvider spyUnderTest = Mockito.spy(underTest);
        Mockito.doReturn(is).when(spyUnderTest).getStream(Mockito.any());
        Schema actual = spyUnderTest.getSourceSchema();
        assertNotNull(actual);
        assertEquals(actual, getExpectedSchema(json));
        verify(spyUnderTest, times(0)).setAuthorizationHeader(Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetTargetSchemaShouldRequestSchemaWithoutCreds() throws IOException {
        TypedProperties props = getProps();
        props.put("hoodie.deltastreamer.schemaprovider.registry.url", "http://localhost");
        InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        SchemaRegistryProvider underTest = new SchemaRegistryProvider(props, null);
        SchemaRegistryProvider spyUnderTest = Mockito.spy(underTest);
        Mockito.doReturn(is).when(spyUnderTest).getStream(Mockito.any());
        Schema actual = spyUnderTest.getTargetSchema();
        assertNotNull(actual);
        assertEquals(actual, getExpectedSchema(json));
        verify(spyUnderTest, times(0)).setAuthorizationHeader(Mockito.any(), Mockito.any());
    }
}