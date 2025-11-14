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

package org.apache.hudi.schema;

import org.apache.avro.JsonProperties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link HoodieJsonProperties}.
 */
public class TestHoodieJsonProperties {

  @Test
  public void testNullValue() {
    // HoodieJsonProperties.NULL_VALUE should be the same as JsonProperties.NULL_VALUE
    assertSame(JsonProperties.NULL_VALUE, HoodieJsonProperties.NULL_VALUE);
    assertEquals(JsonProperties.NULL_VALUE, HoodieJsonProperties.NULL_VALUE);
  }

  @Test
  public void testUtilityClass() throws Exception {
    // Should not be able to instantiate HoodieJsonProperties
    Constructor<HoodieJsonProperties> constructor = HoodieJsonProperties.class.getDeclaredConstructor();
    constructor.setAccessible(true);

    InvocationTargetException exception = assertThrows(InvocationTargetException.class, () -> {
      constructor.newInstance();
    });

    // The actual exception should be UnsupportedOperationException
    assertEquals(UnsupportedOperationException.class, exception.getCause().getClass());
  }
}
