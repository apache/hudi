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

package org.apache.hudi.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestValueType {

  @Test
  public void testValueTypeNumbering() {
    // DO NOT MODIFY THE ORDERING OF THE TYPE NUMBERING
    assertEquals(0, ValueType.V1.ordinal());
    assertEquals(1, ValueType.NULL.ordinal());
    assertEquals(2, ValueType.BOOLEAN.ordinal());
    assertEquals(3, ValueType.INT.ordinal());
    assertEquals(4, ValueType.LONG.ordinal());
    assertEquals(5, ValueType.FLOAT.ordinal());
    assertEquals(6, ValueType.DOUBLE.ordinal());
    assertEquals(7, ValueType.STRING.ordinal());
    assertEquals(8, ValueType.BYTES.ordinal());
    assertEquals(9, ValueType.FIXED.ordinal());
    assertEquals(10, ValueType.DECIMAL.ordinal());
    assertEquals(11, ValueType.UUID.ordinal());
    assertEquals(12, ValueType.DATE.ordinal());
    assertEquals(13, ValueType.TIME_MILLIS.ordinal());
    assertEquals(14, ValueType.TIME_MICROS.ordinal());
    assertEquals(15, ValueType.TIMESTAMP_MILLIS.ordinal());
    assertEquals(16, ValueType.TIMESTAMP_MICROS.ordinal());
    assertEquals(17, ValueType.TIMESTAMP_NANOS.ordinal());
    assertEquals(18, ValueType.LOCAL_TIMESTAMP_MILLIS.ordinal());
    assertEquals(19, ValueType.LOCAL_TIMESTAMP_MICROS.ordinal());
    assertEquals(20, ValueType.LOCAL_TIMESTAMP_NANOS.ordinal());
    // IF YOU ADD A NEW TYPE, ADD IT TO THE END AND INCREMENT THE COUNT
    // AND ALSO ASSERT IT HERE SO THAT SOMEONE DOESN'T MESS WITH IT
    // IN THE FUTURE
    assertEquals(21, ValueType.values().length);
  }
}
