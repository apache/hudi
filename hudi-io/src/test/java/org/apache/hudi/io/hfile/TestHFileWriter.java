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

import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.SeekableDataInputStream;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hudi.io.hfile.HFileBlockType.DATA;
import static org.apache.hudi.io.hfile.HFileBlockType.TRAILER;
import static org.apache.hudi.io.hfile.HFileInfo.AVG_KEY_LEN;
import static org.apache.hudi.io.hfile.HFileInfo.AVG_VALUE_LEN;
import static org.apache.hudi.io.hfile.HFileInfo.KEY_VALUE_VERSION;
import static org.apache.hudi.io.hfile.HFileInfo.LAST_KEY;
import static org.apache.hudi.io.hfile.HFileInfo.MAX_MVCC_TS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class TestHFileWriter {
  private static final String TEST_FILE = "test.hfile";
  private static final HFileContext CONTEXT = HFileContext.builder().build();

  @AfterEach
  public void tearDown() throws IOException {
    Files.deleteIfExists(Paths.get(TEST_FILE));
  }

  @Test
  void testOverflow() throws Exception {
    // 1. Write data.
    writeTestFile();
    // 2. Validate file size.
    validateHFileSize();
    // 3. Validate file structure.
    validateHFileStructure();
    // 4. Validate consistency with HFileReader.
    validateConsistencyWithHFileReader();
    log.info("All validations passed!");
  }

  @Test
  void testSameKeyLocation() throws IOException {
    // 165 bytes for data part limit.
    HFileContext context = new HFileContext.Builder().blockSize(165).build();
    String testFile = TEST_FILE;
    // CREATE 4 BLOCKs:
    // Block 1: 100 records, whose keys are the same: "key00".
    // Block 2: 5   records, whose first key is "key01"
    // Block 3: 5   records, whose first key is "key06"
    // Block 4: 1   record,  whose first key is "key11",
    //              whose length is larger than the block size.
    try (DataOutputStream outputStream =
             new DataOutputStream(Files.newOutputStream(Paths.get(testFile)));
        HFileWriter writer = new HFileWriterImpl(context, outputStream)) {
      // All entries for key00 are stored in the first block.
      for (int i = 0; i < 100; i++) {
        writer.append("key00", String.format("value%02d", i).getBytes());
      }
      // Otherwise, 5 records in each other blocks.
      for (int i = 1; i < 11; i++) {
        writer.append(
            String.format("key%02d", i),
            String.format("value%02d", i).getBytes());
      }
      // Adding a record whose size is larger than block size.
      String longValue = generateRandomStringStream(200);
      writer.append("key11", longValue.getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Validate.
    try (FileChannel channel = FileChannel.open(Paths.get(testFile), StandardOpenOption.READ)) {
      ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
      SeekableDataInputStream inputStream =
          new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(buf));
      HFileReaderImpl reader = new HFileReaderImpl(inputStream, channel.size());
      reader.initializeMetadata();
      // Totally 111 records.
      assertEquals(111, reader.getNumKeyValueEntries());
      HFileTrailer trailer = reader.getTrailer();
      // Totally 4 blocks.
      assertEquals(4, trailer.getDataIndexCount());
      int i = 0;
      for (Map.Entry<Key, BlockIndexEntry> entry : reader.getDataBlockIndexMap().entrySet()) {
        if (i == 0) {
          // first block: 100 records * 33 bytes + 37 bytes for header and checksum = 3337.
          assertEquals(3337, entry.getValue().getSize());
          assertEquals("key00", entry.getKey().getContentInString());
          i++;
        } else {
          if (i == 1 || i == 6) {
            // second and third blocks: 5 records * 33 bytes + 37 bytes for header and checksum = 202.
            assertEquals(202, entry.getValue().getSize());
            assertEquals(String.format("key%02d", i), entry.getKey().getContentInString());
            i += 5;
          } else {
            // fourth block: 1 records * 226 bytes + 37 bytes for header and checksum = 263.
            assertEquals(263, entry.getValue().getSize());
            assertEquals(String.format("key%02d", i), entry.getKey().getContentInString());
            i++;
          }
        }
      }
    }
  }

  @Test
  void testUniqueKeyLocation() throws IOException {
    // 50 bytes for data part limit.
    HFileContext context = new HFileContext.Builder().blockSize(100).build();
    String testFile = TEST_FILE;
    try (DataOutputStream outputStream =
             new DataOutputStream(Files.newOutputStream(Paths.get(testFile)));
         HFileWriter writer = new HFileWriterImpl(context, outputStream)) {
      for (int i = 0; i < 50; i++) {
        writer.append(
            String.format("key%02d", i), String.format("value%02d", i).getBytes());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Validate.
    try (FileChannel channel = FileChannel.open(Paths.get(testFile), StandardOpenOption.READ)) {
      ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
      SeekableDataInputStream inputStream =
          new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(buf));
      HFileReaderImpl reader = new HFileReaderImpl(inputStream, channel.size());
      reader.initializeMetadata();
      assertEquals(50, reader.getNumKeyValueEntries());
      HFileTrailer trailer = reader.getTrailer();
      assertEquals(17, trailer.getDataIndexCount());
      reader.seekTo();
      for (int i = 0; i < 50; i++) {
        KeyValue kv = reader.getKeyValue().get();
        assertArrayEquals(
            String.format("key%02d", i).getBytes(),
            kv.getKey().getContentInString().getBytes());
        assertArrayEquals(
            String.format("value%02d", i).getBytes(),
            Arrays.copyOfRange(
                kv.getBytes(),
                kv.getValueOffset(),
                kv.getValueOffset() + kv.getValueLength())
        );
        reader.next();
      }
    }
  }

  private static void writeTestFile() throws Exception {
    try (
        DataOutputStream outputStream =
             new DataOutputStream(Files.newOutputStream(Paths.get(TEST_FILE)));
        HFileWriter writer = new HFileWriterImpl(CONTEXT, outputStream)) {
      writer.append("key1", "value1".getBytes());
      writer.append("key2", "value2".getBytes());
      writer.append("key3", "value3".getBytes());
    }
  }

  private static void validateHFileSize() throws IOException {
    Path path = Paths.get(TEST_FILE);
    long actualSize = Files.size(path);
    long expectedSize = 4537;
    assertEquals(expectedSize, actualSize);
  }

  private static void validateHFileStructure() throws IOException {
    ByteBuffer fileBuffer = mapFileToBuffer();

    // 1. Validate Trailer
    validateTrailer(fileBuffer);

    // 2. Validate Data block.
    validateDataBlocks(fileBuffer);
  }

  private static void validateConsistencyWithHFileReader() throws IOException {
    ByteBuffer content = mapFileToBuffer();
    try (HFileReader reader = new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(
            new ByteBufferBackedInputStream(content)), content.limit())) {
      reader.initializeMetadata();
      assertEquals(3, reader.getNumKeyValueEntries());
      assertTrue(reader.getMetaInfo(LAST_KEY).isPresent());
      assertEquals(4, reader.getMetaInfo(AVG_KEY_LEN).get().length);
      assertEquals(4, reader.getMetaInfo(AVG_VALUE_LEN).get().length);
      assertEquals(8, reader.getMetaInfo(MAX_MVCC_TS_KEY).get().length);
      assertEquals(1,
          ByteBuffer.wrap(reader.getMetaInfo(KEY_VALUE_VERSION).get()).getInt());
    }
  }

  private static ByteBuffer mapFileToBuffer() throws IOException {
    try (FileChannel channel = FileChannel.open(Paths.get(TEST_FILE), StandardOpenOption.READ)) {
      return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }
  }

  private static void validateTrailer(ByteBuffer buf) {
    int trailerStart = Math.max(0, buf.limit() - 4096);
    buf.position(trailerStart);

    // Verify magic
    byte[] trailerMagic = new byte[8];
    buf.get(trailerMagic);
    assertArrayEquals(TRAILER.getMagic(), trailerMagic);

    // Verify version (last 4 bytes of trailer)
    buf.position(trailerStart + 4096 - 4);
    byte[] versionBytes = new byte[4];
    buf.get(versionBytes);
    int version = ByteBuffer.wrap(versionBytes).getInt();
    assertEquals(3, version);
  }

  private static void validateDataBlocks(ByteBuffer buf) {
    // Point to the first data block.
    buf.position(0);

    // Validate magic.
    byte[] dataBlockMagic = new byte[8];
    buf.get(dataBlockMagic);
    assertArrayEquals(DATA.getMagic(), dataBlockMagic);

    // Skip header.
    buf.position(buf.position() + 25);

    // Validate data.
    validateKeyValue(buf, "key1", "value1");
    validateKeyValue(buf, "key2", "value2");
    validateKeyValue(buf, "key3", "value3");
  }

  private static void validateKeyValue(ByteBuffer buf, String expectedKey, String expectedValue) {
    int keyLen = buf.getInt();
    int valLen = buf.getInt();

    byte[] key = new byte[keyLen];
    buf.get(key);
    byte[] keyContent = Arrays.copyOfRange(key, 2, key.length - 10);
    assertArrayEquals(expectedKey.getBytes(StandardCharsets.UTF_8), keyContent);

    byte[] value = new byte[valLen];
    buf.get(value);
    assertArrayEquals(expectedValue.getBytes(StandardCharsets.UTF_8), value);

    buf.get(); // Skip MVCC timestamp
  }

  private static void assertArrayEquals(byte[] expected, byte[] actual) {
    if (!Arrays.equals(expected, actual)) {
      throw new AssertionError("Byte array mismatch");
    }
  }

  public static String generateRandomStringStream(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    Random random = new Random();
    return random.ints(length, 0, characters.length())
        .mapToObj(characters::charAt)
        .map(Object::toString)
        .collect(Collectors.joining());
  }
}
