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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link BufferedRandomAccessFile}, the buffered wrapper used to
 * back the log-file read path (via BitCaskDiskMap / LazyFileIterable). The buffer
 * capacity is clamped to a 64K minimum, so the payloads here are deliberately
 * larger than that to force reads and seeks that cross buffer boundaries.
 *
 * <p>All fixtures are seeded via {@link Files#write} rather than the class's own
 * write path since Hudi opens this class only in read mode ("r") in production.
 */
public class TestBufferedRandomAccessFile {

  // Larger than the internal 64K minimum buffer so that access crosses buffer boundaries.
  private static final int PAYLOAD_SIZE = (1 << 16) * 3 + 517;

  private static byte[] deterministicBytes(int size) {
    byte[] bytes = new byte[size];
    new Random(42).nextBytes(bytes);
    return bytes;
  }

  private static File seedFile(Path dir, String name, byte[] contents) throws Exception {
    File file = new File(dir.toFile(), name);
    Files.write(file.toPath(), contents);
    return file;
  }

  @Test
  public void testReadFullyAcrossBufferBoundaries(@TempDir Path tempDir) throws Exception {
    byte[] expected = deterministicBytes(PAYLOAD_SIZE);
    File file = seedFile(tempDir, "read.bin", expected);

    byte[] readBack = new byte[PAYLOAD_SIZE];
    try (BufferedRandomAccessFile raf = new BufferedRandomAccessFile(file, "r")) {
      // readFully loops over read(...) so partial reads at buffer boundaries are handled.
      raf.readFully(readBack);
      assertEquals(PAYLOAD_SIZE, raf.getFilePointer(), "Read should consume the whole file");
      assertEquals(-1, raf.read(), "Reading past EOF should return -1");
    }
    assertArrayEquals(expected, readBack, "Bytes read back should match bytes written");
  }

  @Test
  public void testSeekRandomAccessCrossingBoundaries(@TempDir Path tempDir) throws Exception {
    byte[] expected = deterministicBytes(PAYLOAD_SIZE);
    File file = seedFile(tempDir, "seek.bin", expected);

    try (BufferedRandomAccessFile raf = new BufferedRandomAccessFile(file, "r")) {
      // Probe positions that fall in the first, second, third and last logical buffer blocks,
      // out of order, so both forward and backward seeks are exercised.
      int[] probes = {0, (1 << 16) + 7, (1 << 16) * 2 + 100, 5, PAYLOAD_SIZE - 1};
      for (int pos : probes) {
        raf.seek(pos);
        assertEquals(pos, raf.getFilePointer(), "getFilePointer should reflect the seek target");
        int actual = raf.read();
        assertEquals(expected[pos] & 0xFF, actual, "Byte at position " + pos + " should match");
      }

      // A ranged read that starts mid-buffer and spans a boundary must return the correct slice.
      int start = (1 << 16) - 10;
      int len = 40;
      raf.seek(start);
      byte[] slice = new byte[len];
      raf.readFully(slice);
      byte[] expectedSlice = new byte[len];
      System.arraycopy(expected, start, expectedSlice, 0, len);
      assertArrayEquals(expectedSlice, slice, "Ranged read across a buffer boundary should match");
    }
  }

  @Test
  public void testReadIntoOffsetAndLength(@TempDir Path tempDir) throws Exception {
    byte[] expected = deterministicBytes(1024);
    File file = seedFile(tempDir, "offset.bin", expected);

    try (BufferedRandomAccessFile raf = new BufferedRandomAccessFile(file, "r")) {
      byte[] target = new byte[1024 + 8];
      // Leave a 4-byte prefix untouched and read the payload into the middle of the array.
      raf.readFully(target, 4, 1024);
      byte[] payload = new byte[1024];
      System.arraycopy(target, 4, payload, 0, 1024);
      assertArrayEquals(expected, payload, "readFully into an offset should place bytes at that offset");
      assertEquals(0, target[0], "Bytes before the offset must remain untouched");
      assertEquals(0, target[3], "Byte just before the offset must remain untouched");
      assertEquals(0, target[1028], "Byte just after the range must remain untouched");
      assertEquals(0, target[1031], "Last byte after the range must remain untouched");
    }
  }

  @Test
  public void testLengthAndEofAfterFullRead(@TempDir Path tempDir) throws Exception {
    byte[] expected = deterministicBytes(PAYLOAD_SIZE);
    File file = seedFile(tempDir, "length.bin", expected);

    try (BufferedRandomAccessFile raf = new BufferedRandomAccessFile(file, "r")) {
      assertEquals(PAYLOAD_SIZE, raf.length(), "length should report the on-disk size");
      raf.seek(PAYLOAD_SIZE);
      assertEquals(-1, raf.read(), "Reading at EOF should return -1");
      assertEquals(PAYLOAD_SIZE, raf.getFilePointer(), "File pointer should stay at EOF");
    }
  }
}
