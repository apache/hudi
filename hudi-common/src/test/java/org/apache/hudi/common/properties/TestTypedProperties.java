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

package org.apache.hudi.common.properties;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TypedProperties}.
 */
public class TestTypedProperties {
  @Test
  public void testGetString() {
    Properties properties = new Properties();
    properties.put("key1", "value1");

    TypedProperties typedProperties = TypedProperties.copy(properties);
    assertEquals("value1", typedProperties.getString("key1"));
    assertEquals("value1", typedProperties.getString("key1", "default"));
    assertEquals("default", typedProperties.getString("key2", "default"));
  }

  @Test
  public void testGetInteger() {
    Properties properties = new Properties();
    properties.put("key1", "123");

    TypedProperties typedProperties = TypedProperties.copy(properties);
    assertEquals(123, typedProperties.getInteger("key1"));
    assertEquals(123, typedProperties.getInteger("key1", 456));
    assertEquals(456, typedProperties.getInteger("key2", 456));

  }

  @Test
  public void testGetDouble() {
    Properties properties = new Properties();
    properties.put("key1", "123.4");

    TypedProperties typedProperties = TypedProperties.copy(properties);
    assertEquals(123.4, typedProperties.getDouble("key1"));
    assertEquals(123.4, typedProperties.getDouble("key1", 0.001D));
    assertEquals(0.001D, typedProperties.getDouble("key2", 0.001D));
  }

  @Test
  public void testGetLong() {
    Properties properties = new Properties();
    properties.put("key1", "1354354354");

    TypedProperties typedProperties = TypedProperties.copy(properties);
    assertEquals(1354354354, typedProperties.getLong("key1"));
    assertEquals(1354354354, typedProperties.getLong("key1", 8578494434L));
    assertEquals(8578494434L, typedProperties.getLong("key2", 8578494434L));
  }

  @Test
  public void testGetBoolean() {
    Properties properties = new Properties();
    properties.put("key1", "true");

    TypedProperties typedProperties = TypedProperties.copy(properties);
    assertTrue(typedProperties.getBoolean("key1"));
    assertTrue(typedProperties.getBoolean("key1", false));
    assertFalse(typedProperties.getBoolean("key2", false));
    // test getBoolean with non-string value for key2
    properties.put("key2", true);
    typedProperties = TypedProperties.copy(properties);
    assertTrue(typedProperties.getBoolean("key1", false));
    assertTrue(typedProperties.getBoolean("key2", false));
    // put non-string value in TypedProperties
    typedProperties.put("key3", true);
    assertTrue(typedProperties.getBoolean("key3", false));
  }

  @Test
  public void testTypedPropertiesWithNonStringValue() {
    Properties properties = new Properties();
    properties.put("key1", "1");
    properties.put("key2", 2);

    TypedProperties props = TypedProperties.copy(properties);
    assertEquals(1, props.getInteger("key1"));
    assertEquals(2, props.getInteger("key2"));
    // put non-string value in TypedProperties
    props.put("key2", 3);
    assertEquals(3, props.getInteger("key2"));
  }
}
