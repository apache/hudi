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

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestVectorIndexBootstrapUtils {

  @Test
  void splitsInterleavedBitsIntoPostingPlanes() {
    // Two lower bits per dimension: 01, 10, 11, 00 (least-significant bit first).
    byte[] interleaved = {(byte) 0x39};

    byte[] planes = VectorIndexBootstrapUtils.splitExPlanes(interleaved, 2, 4, 8);

    byte[] expected = new byte[16];
    expected[0] = 0x05;
    expected[8] = 0x06;
    assertArrayEquals(expected, planes);
  }

  @Test
  void returnsNoPlanesForBinaryEncoding() {
    assertArrayEquals(new byte[0],
        VectorIndexBootstrapUtils.splitExPlanes(null, 0, 4, 8));
  }

  @Test
  void rejectsPackedCodeWithWrongSize() {
    assertThrows(IllegalArgumentException.class,
        () -> VectorIndexBootstrapUtils.splitExPlanes(new byte[2], 2, 4, 8));
  }
}
