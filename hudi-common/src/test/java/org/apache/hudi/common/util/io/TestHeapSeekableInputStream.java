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

package org.apache.hudi.common.util.io;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HeapSeekableInputStream}.
 */
public class TestHeapSeekableInputStream {
  @Test
  public void testGetPos() throws IOException {
    byte[] sourceBytes = { 0xD, 0xE, 0xA, 0xD, 0xD, 0xA, 0xE, 0xD };
    ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(sourceBytes);
    HeapSeekableInputStream seekableInputStream = new HeapSeekableInputStream(inputStream);

    int firstRead = seekableInputStream.read();
    assertEquals(0xD, firstRead);
    assertEquals(1L, seekableInputStream.getPos());
  }

  @Test
  public void testSeek() throws IOException {
    byte[] sourceBytes = { 0xD, 0xE, 0xA, 0xD, 0xD, 0xA, 0xE, 0xD };
    ByteBufferBackedInputStream inputStream = new ByteBufferBackedInputStream(sourceBytes);
    HeapSeekableInputStream seekableInputStream = new HeapSeekableInputStream(inputStream);

    seekableInputStream.seek(5);
    int firstRead = seekableInputStream.read();
    assertEquals(0xA, firstRead);
    assertEquals(6L, seekableInputStream.getPos());
  }
}
