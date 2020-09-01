package org.apache.hudi.common.properties;

import org.apache.hudi.common.config.TypedProperties;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTypedProperties {
    @Test
    public void testNewTypedProperties() {
        Properties properties = new Properties();
        properties.put("key1", "value1");

        TypedProperties typedProperties = new TypedProperties(properties);
        assertEquals("value1", typedProperties.getString("key1"));
    }
}
