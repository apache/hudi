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

package org.apache.hudi.parquet.io;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestByteArraySeekableInputStream {

  @Test
  public void testReadAndSeek() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data)) {
      assertEquals(0, stream.getPos());
      assertEquals(0, stream.read());
      assertEquals(1, stream.getPos());

      stream.seek(5);
      assertEquals(5, stream.getPos());
      assertEquals(5, stream.read());

      byte[] buffer = new byte[2];
      stream.readFully(buffer);
      assertArrayEquals(new byte[] {6, 7}, buffer);
      assertEquals(8, stream.getPos());

      stream.seek(0);
      assertEquals(0, stream.getPos());
      assertEquals(0, stream.read());
    }
  }

  @Test
  public void testReadWithOffset() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    // Offset 2, length 4.
    // This means: buffer[0..3] contains data from file positions 2..5
    // So data[0]=file_pos_2, data[1]=file_pos_3, data[2]=file_pos_4, data[3]=file_pos_5
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 2, 4)) {
      // Initially at offset position (file position 2, which is buffer[0])
      assertEquals(2, stream.getPos());

      assertEquals(0, stream.read()); // Reads data[0] which contains value 0 from file_pos_2
      assertEquals(3, stream.getPos()); // Now at file position 3

      stream.seek(4); // Seek to file position 4 (data[2])
      assertEquals(2, stream.read()); // Reads data[2] which contains value 2 from file_pos_4
      assertEquals(5, stream.getPos()); // Now at file position 5

      // Seek bounds check
      // newPos < offset (2)
      assertThrows(IOException.class, () -> stream.seek(1));
      // newPos > offset + length (2 + 4 = 6)
      assertThrows(IOException.class, () -> stream.seek(7));

      // Seek to end (offset + length = 6) is allowed
      stream.seek(6);
      assertEquals(-1, stream.read());
    }
  }

  @Test
  public void testReadFully() throws IOException {
    byte[] data = new byte[] {10, 20, 30, 40, 50};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data)) {
      byte[] buffer = new byte[3];
      stream.readFully(buffer);
      assertArrayEquals(new byte[] {10, 20, 30}, buffer);

      stream.readFully(buffer, 0, 2);
      assertEquals(40, buffer[0]);
      assertEquals(50, buffer[1]);
      assertEquals(30, buffer[2]); // untouched

      assertThrows(EOFException.class, () -> stream.readFully(new byte[1]));
    }
  }

  @Test
  public void testReadByteBuffer() throws IOException {
    byte[] data = new byte[] {1, 2, 3, 4, 5};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data)) {
      ByteBuffer buf = ByteBuffer.allocate(3);
      stream.readFully(buf);
      buf.flip();
      assertEquals(1, buf.get());
      assertEquals(2, buf.get());
      assertEquals(3, buf.get());

      buf.clear();
      int read = stream.read(buf);
      assertEquals(2, read);
      buf.flip();
      assertEquals(4, buf.get());
      assertEquals(5, buf.get());

      read = stream.read(buf); // EOF
      assertEquals(-1, read);
    }
  }

  @Test
  public void testReadByteArrayPartial() throws IOException {
    byte[] data = new byte[] {1, 2, 3};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data)) {
      byte[] buf = new byte[5];
      int read = stream.read(buf, 0, 5);
      assertEquals(3, read);
      assertEquals(1, buf[0]);
      assertEquals(2, buf[1]);
      assertEquals(3, buf[2]);

      read = stream.read(buf, 0, 1);
      assertEquals(-1, read);
    }
  }

  @Test
  public void testOffsetMapping_FilePositionMapsToBufferIndex() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    // Create stream with offset=2, length=4
    // This represents file positions 2..5, mapped to buffer[0..3]
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 2, 4)) {
      assertEquals(2, stream.getPos(), "Initial position should be at offset");
    }
  }

  @Test
  public void testBoundaryCheck_BeforeOffset_ShouldThrow() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 2, 4)) {
      assertThrows(IOException.class, () -> stream.seek(1), "Should throw for seek before offset");
    }
  }

  @Test
  public void testBoundaryCheck_PastEnd_ShouldThrow() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 2, 4)) {
      assertThrows(IOException.class, () -> stream.seek(7), "Should throw for seek past end");
    }
  }

  @Test
  public void testSeek_ToExactEnd_IsAllowed() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 2, 4)) {
      stream.seek(6);  // Exactly offset + length
      assertEquals(6, stream.getPos(), "Seek to end allowed");
    }
  }

  @Test
  public void testRead_AtEnd_ReturnsEOF() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 2, 4)) {
      stream.seek(6);  // Seek to end
      int result = stream.read();
      assertEquals(-1, result, "Should return -1 at EOF");
    }
  }

  @Test
  public void testEmptyStream_ZeroLength() throws IOException {
    byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    try (ByteArraySeekableInputStream stream = new ByteArraySeekableInputStream(data, 5, 0)) {
      assertEquals(5, stream.getPos());
      int result = stream.read();
      assertEquals(-1, result, "Should return EOF immediately for zero-length stream");
    }
  }
}
