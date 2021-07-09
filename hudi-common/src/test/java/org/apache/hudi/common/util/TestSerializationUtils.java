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

package org.apache.hudi.common.util;

import org.apache.avro.util.Utf8;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests serialization utils.
 */
public class TestSerializationUtils {

  @Test
  public void testSerDeser() throws IOException {
    // It should handle null object references.
    verifyObject(null);
    // Object with nulls.
    verifyObject(new NonSerializableClass(null));
    // Object with valid values & no default constructor.
    verifyObject(new NonSerializableClass("testValue"));
    // Object with multiple constructor
    verifyObject(new NonSerializableClass("testValue1", "testValue2"));
    // Object which is of non-serializable class.
    verifyObject(new Utf8("test-key"));
    // Verify serialization of list.
    verifyObject(new LinkedList<>(Arrays.asList(2, 3, 5)));
  }
  
  @Test
  public void testHoodieKeV1Compatibility() throws IOException {
    HoodieKey[] hoodieKeys = new HoodieKey[2];
    hoodieKeys[0] = new HoodieKey("key1", "part1");
    hoodieKeys[1] = new HoodieKey("key2", "part2");
    verifyHoodieKeyCompatibility(hoodieKeys, "hoodie-key-v1.bin", 1);
  }

  @Test
  public void testHoodieKeyV2Compatibility() throws IOException {
    HoodieKey[] hoodieKeys = new HoodieKey[3];
    hoodieKeys[0] = new HoodieKey("key1", "part1");
    hoodieKeys[1] = new HoodieKey("key2", "part2", new ArrayList<>(Arrays.asList("indexKey2")));
    hoodieKeys[2] = new HoodieKey("key3", "part3", new ArrayList<>(Arrays.asList("indexKey3")));
    verifyHoodieKeyCompatibility(hoodieKeys, "hoodie-key-v2.bin", 2);
    // Add this check to the latest version
    verifyHoodieKeyCompatibility(hoodieKeys, "hoodie-key-v2.bin");
  }
  
  private void verifyHoodieKeyCompatibility(HoodieKey[] hoodieKeys, String file)
      throws IOException {
    verifyHoodieKeyCompatibility(hoodieKeys, file, HoodieLogBlock.version);
  }

  private void verifyHoodieKeyCompatibility(HoodieKey[] hoodieKeys, String file, int version)
      throws IOException {
    byte[] serializedHoodieKeys = FileIOUtils.readAsByteArray(
        getClass().getClassLoader().getResourceAsStream(file));
    HoodieKey[] deserialized = SerializationUtils.deserialize(serializedHoodieKeys, version);
    for (int i = 0; i < hoodieKeys.length; i++) {
      assertEquals(hoodieKeys[i], deserialized[i]);
    }
  }

  private <T> void verifyObject(T expectedValue) throws IOException {
    byte[] serializedObject = SerializationUtils.serialize(expectedValue);
    assertNotNull(serializedObject);
    assertTrue(serializedObject.length > 0);

    final T deserializedValue = SerializationUtils.<T>deserialize(serializedObject);
    if (expectedValue == null) {
      assertNull(deserializedValue);
    } else {
      assertEquals(expectedValue, deserializedValue);
    }
  }

  private static class NonSerializableClass {
    private String id;
    private String name;

    NonSerializableClass(String id) {
      this(id, "");
    }

    NonSerializableClass(String id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof NonSerializableClass)) {
        return false;
      }
      final NonSerializableClass other = (NonSerializableClass) obj;
      return Objects.equals(this.id, other.id) && Objects.equals(this.name, other.name);
    }
  }
}
