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

package org.apache.hudi.common.fs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the SizeAwareDataInputStream class.
 */
public class TestSizeAwareDataInputStream {

  private SizeAwareDataInputStream sizeAwareDataInputStream;
  private ByteArrayInputStream byteArrayInputStream;

  /**
   * Initializes the input stream with a predefined byte array.
   */
  @BeforeEach
  void setUp() {
    byte[] data = {0, 0, 0, 1, 2, 3, 4, 5};
    byteArrayInputStream = new ByteArrayInputStream(data);
    sizeAwareDataInputStream = new SizeAwareDataInputStream(new DataInputStream(byteArrayInputStream));
  }

  /**
   * Tests the readInt method to ensure it reads an integer correctly and updates the byte count.
   */
  @Test
  void testReadInt() throws IOException {
    int value = sizeAwareDataInputStream.readInt();
    assertEquals(1, value);
    assertEquals(4, sizeAwareDataInputStream.getNumberOfBytesRead());
  }

  /**
   * Tests the readFully method to ensure it reads bytes into a buffer and updates the byte count.
   */
  @Test
  void testReadFully() throws IOException {
    byte[] buffer = new byte[4];
    sizeAwareDataInputStream.readFully(buffer);
    assertArrayEquals(new byte[]{0, 0, 0, 1}, buffer);
    assertEquals(4, sizeAwareDataInputStream.getNumberOfBytesRead());
  }

  /**
   * Tests the readFully method with offset to ensure it reads bytes into a specific part of the buffer and updates the byte count.
   */
  @Test
  void testReadFullyWithOffset() throws IOException {
    byte[] buffer = new byte[6];
    sizeAwareDataInputStream.readFully(buffer, 2, 4);
    assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 1}, buffer);
    assertEquals(4, sizeAwareDataInputStream.getNumberOfBytesRead());
  }

  /**
   * Tests the skipBytes method to ensure it skips the correct number of bytes and updates the byte count.
   */
  @Test
  void testSkipBytes() throws IOException {
    int skipped = sizeAwareDataInputStream.skipBytes(2);
    assertEquals(2, skipped);
    assertEquals(2, sizeAwareDataInputStream.getNumberOfBytesRead());
  }

  /**
   * Tests the skipBytes method when attempting to skip more bytes than available, ensuring it only skips the available bytes.
   */
  @Test
  void testSkipBytesBeyondAvailable() throws IOException {
    sizeAwareDataInputStream.readInt();
    // Try to skip more than available
    int skipped = sizeAwareDataInputStream.skipBytes(10);
    assertEquals(4, skipped);
    assertEquals(8, sizeAwareDataInputStream.getNumberOfBytesRead());
  }

  /**
   * Tests exception handling to ensure that the byte count remains consistent after an EOFException.
   */
  @Test
  void testExceptionHandling() {
    byte[] buffer = new byte[10];
    assertThrows(EOFException.class, () -> sizeAwareDataInputStream.readFully(buffer));
    assertEquals(0, sizeAwareDataInputStream.getNumberOfBytesRead(), "Number of bytes read should remain consistent after exception.");
  }
}
