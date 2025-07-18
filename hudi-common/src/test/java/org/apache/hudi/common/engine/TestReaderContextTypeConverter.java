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

package org.apache.hudi.common.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestReaderContextTypeConverter {
  private final ReaderContextTypeConverter typeHandler = new ReaderContextTypeConverter();

  @Test
  void testCastToBoolean_withBooleanTrue() {
    assertTrue(typeHandler.castToBoolean(true));
  }

  @Test
  void testCastToBoolean_withBooleanFalse() {
    assertFalse(typeHandler.castToBoolean(false));
  }

  @Test
  void testCastToBoolean_withInvalidType() {
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      typeHandler.castToBoolean("true");
    });
    assertTrue(exception.getMessage().contains("cannot be cast to boolean"));
  }

  @Test
  void testCastToString_withNonNullValue() {
    assertEquals("123", typeHandler.castToString(123));
    assertEquals("true", typeHandler.castToString(true));
  }

  @Test
  void testCastToString_withNullValue() {
    assertNull(typeHandler.castToString(null));
  }
}
