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

package org.apache.hudi.avro;

import org.apache.hudi.common.util.AvroJavaTypeConverter;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAvroJavaTypeConverter {
  private final AvroJavaTypeConverter converter = new AvroJavaTypeConverter();

  @Test
  void testCastToStringWithNullValue() {
    assertNull(converter.castToString(null));
  }

  @Test
  void testCastToStringWithByteBuffer() {
    String testString = "Hello, World!";
    ByteBuffer buffer = ByteBuffer.wrap(testString.getBytes());
    assertEquals(testString, converter.castToString(buffer));
  }

  @Test
  void testCastToStringWithEmptyByteBuffer() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[0]);
    assertEquals("", converter.castToString(buffer));
  }

  @Test
  void testCastToStringWithStringValue() {
    String testString = "test string";
    assertEquals(testString, converter.castToString(testString));
  }

  @Test
  void testCastToStringWithIntegerValue() {
    Integer value = 123;
    assertEquals("123", converter.castToString(value));
  }

  @Test
  void testCastToStringWithBooleanValue() {
    Boolean value = true;
    assertEquals("true", converter.castToString(value));
  }

  @Test
  void testCastToStringWithDoubleValue() {
    Double value = 3.14;
    assertEquals("3.14", converter.castToString(value));
  }

  @Test
  void testCastToBooleanWithBooleanTrue() {
    assertTrue(converter.castToBoolean(true));
  }

  @Test
  void testCastToBooleanWithBooleanFalse() {
    assertFalse(converter.castToBoolean(false));
  }

  @Test
  void testCastToBooleanWithInvalidType() {
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      converter.castToBoolean("true");
    });
    assertTrue(exception.getMessage().contains("cannot be cast to boolean"));
  }
}
