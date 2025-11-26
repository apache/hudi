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

package org.apache.hudi.io;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link ByteBufferBackedInputStream}.
 */
public class TestByteBufferBackedInputStream {

  @Test
  public void testConstructor() {
    byte[] bytes = { 0xD, 0xE, 0xA, 0xD, 0xD, 0xE, 0xE, 0xD };
    ByteBuffer byteBuf = ByteBuffer.wrap(bytes, 0, 1);
    ByteBuffer byteBufClone = byteBuf.duplicate();

    // ByteBuffer ctor
    ByteBufferBackedInputStream first = new ByteBufferBackedInputStream(byteBuf);

    assertEquals(first.read(), 0xD);
    assertThrows(IllegalArgumentException.class, first::read);
    // Make sure that the original buffer stays intact
    assertEquals(byteBufClone, byteBuf);

    // byte[] ctor
    ByteBufferBackedInputStream second = new ByteBufferBackedInputStream(bytes);

    assertEquals(second.read(), 0xD);

    // byte[] ctor (w/ offset)
    ByteBufferBackedInputStream third = new ByteBufferBackedInputStream(bytes, 1, 1);

    assertEquals(third.read(), 0xE);
    assertThrows(IllegalArgumentException.class, third::read);
  }

  @Test
  public void testRead() {
    byte[] sourceBytes = { 0xD, 0xE, 0xA, 0xD, 0xD, 0xE, 0xE, 0xD };

    ByteBufferBackedInputStream stream = new ByteBufferBackedInputStream(sourceBytes);

    int firstByte = stream.read();
    assertEquals(firstByte, 0xD);

    byte[] readBytes = new byte[4];
    int read = stream.read(readBytes, 1, 3);

    assertEquals(3, read);
    assertArrayEquals(new byte[]{0, 0xE, 0xA, 0xD}, readBytes);
    assertEquals(4, stream.getPosition());
  }

  @Test
  public void testSeek() {
    byte[] sourceBytes = { 0xD, 0xE, 0xA, 0xD, 0xD, 0xA, 0xE, 0xD };

    ByteBufferBackedInputStream stream = new ByteBufferBackedInputStream(sourceBytes, 1, 7);

    // Seek to 2 byte in the stream (3 in the original buffer)
    stream.seek(1);
    int firstRead = stream.read();
    assertEquals(0xA, firstRead);

    // Seek to 5 byte in the stream (6 in the original buffer)
    stream.seek(5);
    int secondRead = stream.read();
    assertEquals(0xE, secondRead);

    // Try to seek past the stream boundary
    assertThrows(IllegalArgumentException.class, () -> stream.seek(8));
  }

  @Test
  public void testCopyFrom() {
    byte[] sourceBytes = { 0xD, 0xE, 0xA, 0xD, 0xD, 0xA, 0xE, 0xD };

    ByteBufferBackedInputStream stream = new ByteBufferBackedInputStream(sourceBytes);

    int firstByte = stream.read();
    assertEquals(firstByte, 0xD);

    // Copy 5 byes from the stream (while keeping stream's state intact)
    byte[] targetBytes = new byte[5];
    stream.copyFrom(2, targetBytes, 0, targetBytes.length);

    assertArrayEquals(new byte[] { 0xA, 0xD, 0xD, 0xA, 0xE }, targetBytes);

    // Continue reading the stream from where we left of (before copying)
    int secondByte = stream.read();
    assertEquals(secondByte, 0xE);
  }
}
