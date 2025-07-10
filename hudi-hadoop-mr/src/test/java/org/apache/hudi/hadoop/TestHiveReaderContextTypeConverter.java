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

package org.apache.hudi.hadoop;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHiveReaderContextTypeConverter {
  private final HiveReaderContextTypeConverter handler = new HiveReaderContextTypeConverter();

  @Test
  void testCastToBooleanWithValidBooleanWritableTrue() {
    BooleanWritable input = new BooleanWritable(true);
    assertTrue(handler.castToBoolean(input));
  }

  @Test
  void testCastToBooleanWithValidBooleanWritableFalse() {
    BooleanWritable input = new BooleanWritable(false);
    assertFalse(handler.castToBoolean(input));
  }

  @Test
  void testCastToBooleanWithInvalidType() {
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      handler.castToBoolean("not a BooleanWritable");
    });
    assertTrue(exception.getMessage().contains("Expected BooleanWritable but got"));
  }

  @Test
  void testCastToStringWithValidText() {
    Text input = new Text("test string");
    assertEquals("test string", handler.castToString(input));
  }

  @Test
  void testCastToStringWithInvalidType() {
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      handler.castToString(new BooleanWritable(true));
    });
    assertTrue(exception.getMessage().contains("Expected BooleanWritable but got"));
  }
}
