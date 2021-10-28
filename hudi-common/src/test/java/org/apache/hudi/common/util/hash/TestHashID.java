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

package org.apache.hudi.common.util.hash;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHashID {

  /**
   * Test HashID of all sizes for ByteArray type input message.
   */
  @ParameterizedTest
  @EnumSource(HashID.Size.class)
  public void testHashForByteInput(HashID.Size size) {
    final int count = 8;
    Random random = new Random();
    for (int i = 0; i < count; i++) {
      final String message = random.ints(50, 120)
          .filter(j -> (j <= 57 || j >= 65) && (j <= 90 || j >= 97))
          .limit((32 + (i * 4)))
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
      final byte[] originalData = message.getBytes(StandardCharsets.UTF_8);
      final byte[] hashBytes = HashID.hash(originalData, size);
      assertEquals(hashBytes.length, size.byteSize());
    }
  }

  /**
   * Test HashID of all sizes for String type input message.
   */
  @ParameterizedTest
  @EnumSource(HashID.Size.class)
  public void testHashForStringInput(HashID.Size size) {
    final int count = 8;
    Random random = new Random();
    for (int i = 0; i < count; i++) {
      final String message = random.ints(50, 120)
          .filter(j -> (j <= 57 || j >= 65) && (j <= 90 || j >= 97))
          .limit((32 + (i * 4)))
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
      final byte[] hashBytes = HashID.hash(message, size);
      assertEquals(hashBytes.length, size.byteSize());
    }
  }
}
