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
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
