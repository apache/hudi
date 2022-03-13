/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.properties;

import org.apache.hudi.common.config.OrderedProperties;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOrderedProperties {

  @Test
  public void testPutPropertiesOrder() {
    Properties properties = new OrderedProperties();
    properties.put("key0", "true");
    properties.put("key1", "false");
    properties.put("key2", "true");
    properties.put("key3", "false");
    properties.put("key4", "true");
    properties.put("key5", "true");
    properties.put("key6", "false");
    properties.put("key7", "true");
    properties.put("key8", "false");
    properties.put("key9", "true");

    OrderedProperties typedProperties = new OrderedProperties(properties);
    assertTypeProperties(typedProperties, 0);
  }

  @Test
  void testPutAllPropertiesOrder() {
    Properties firstProp = new OrderedProperties();
    firstProp.put("key0", "true");
    firstProp.put("key1", "false");
    firstProp.put("key2", "true");

    OrderedProperties firstProperties = new OrderedProperties(firstProp);
    assertTypeProperties(firstProperties, 0);

    OrderedProperties secondProperties = new OrderedProperties();
    secondProperties.put("key3", "true");
    secondProperties.put("key4", "false");
    secondProperties.put("key5", "true");
    assertTypeProperties(secondProperties, 3);

    OrderedProperties thirdProperties = new OrderedProperties();
    thirdProperties.putAll(firstProp);
    thirdProperties.putAll(secondProperties);

    assertEquals(3, firstProp.stringPropertyNames().size());
    assertEquals(3, secondProperties.stringPropertyNames().size());
    assertEquals(6, thirdProperties.stringPropertyNames().size());
  }

  private void assertTypeProperties(OrderedProperties typedProperties, int start) {
    String[] props = typedProperties.stringPropertyNames().toArray(new String[0]);
    for (int i = start; i < props.length; i++) {
      assertEquals(String.format("key%d", i), props[i]);
    }
  }
}
