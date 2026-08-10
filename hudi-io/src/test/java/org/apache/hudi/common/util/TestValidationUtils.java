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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link ValidationUtils}: checkArgument throws {@link IllegalArgumentException} and
 * checkState throws {@link IllegalStateException} across all overloads.
 */
public class TestValidationUtils {

  @Test
  public void testCheckArgumentThrowsIllegalArgumentException() {
    assertDoesNotThrow(() -> ValidationUtils.checkArgument(true, "msg"));
    assertThrows(IllegalArgumentException.class, () -> ValidationUtils.checkArgument(false));
    assertEquals("msg", assertThrows(IllegalArgumentException.class,
        () -> ValidationUtils.checkArgument(false, "msg")).getMessage());
    assertEquals("supplied", assertThrows(IllegalArgumentException.class,
        () -> ValidationUtils.checkArgument(false, () -> "supplied")).getMessage());
  }

  @Test
  public void testCheckStateThrowsIllegalStateException() {
    assertDoesNotThrow(() -> ValidationUtils.checkState(true, "msg"));
    assertThrows(IllegalStateException.class, () -> ValidationUtils.checkState(false));
    assertEquals("msg", assertThrows(IllegalStateException.class,
        () -> ValidationUtils.checkState(false, "msg")).getMessage());
    // supplier overload historically threw IllegalArgumentException by copy-paste mistake
    assertEquals("supplied", assertThrows(IllegalStateException.class,
        () -> ValidationUtils.checkState(false, () -> "supplied")).getMessage());
  }
}
