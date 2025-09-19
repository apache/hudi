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

package org.apache.hudi.io.hfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class TestHfileBlock {
  @Test
  void testNumChecksumChunksZeroBytes() {
    Assertions.assertEquals(0, HFileBlock.numChecksumChunks(0L, 512));
  }

  @Test
  void testNumChecksumChunksExactDivision() {
    Assertions.assertEquals(2, HFileBlock.numChecksumChunks(1024L, 512));
  }

  @Test
  void testNumChecksumChunksWithRemainder() {
    Assertions.assertEquals(3, HFileBlock.numChecksumChunks(1200L, 512));
  }

  @Test
  void testNumChecksumChunksSingleChunk() {
    Assertions.assertEquals(1, HFileBlock.numChecksumChunks(200L, 512));
  }

  @Test
  void testNumChecksumChunksOverflowThrows() {
    long numBytes = ((long) Integer.MAX_VALUE / HFileBlock.CHECKSUM_SIZE + 1)
        * 1024; // force too many chunks
    assertThrows(IllegalArgumentException.class,
        () -> HFileBlock.numChecksumChunks(numBytes, 1024));
  }
}
