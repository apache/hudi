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
import org.apache.hudi.io.hfile.protobuf.generated.HFileProtos;
import org.apache.hudi.io.util.IOUtils;

import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.hfile.HFileBlockType.DATA;
import static org.apache.hudi.io.hfile.HFileBlockType.FILE_INFO;
import static org.apache.hudi.io.hfile.HFileBlockType.META;
import static org.apache.hudi.io.hfile.HFileBlockType.ROOT_INDEX;
import static org.apache.hudi.io.hfile.HFileBlockType.TRAILER;
import static org.apache.hudi.io.hfile.HFileInfo.AVG_KEY_LEN;
import static org.apache.hudi.io.hfile.HFileInfo.AVG_VALUE_LEN;
import static org.apache.hudi.io.hfile.HFileInfo.KEY_VALUE_VERSION;
import static org.apache.hudi.io.hfile.HFileInfo.LAST_KEY;
import static org.apache.hudi.io.hfile.HFileInfo.MAX_MVCC_TS_KEY;
import static org.apache.hudi.io.hfile.HFileTrailer.TRAILER_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestHFileWriter.class);
  private static final String TEST_FILE = "test.hfile";
  private static final HFileContext CONTEXT = HFileContext.builder().build();
  // Golden bytes (NONE compression, fixed input) that lock the on-disk encoding of the data block
  // and the root block-index block. Update intentionally ONLY after reviewing HBase-reader
  // compatibility, since any change here is a storage-format change.
  private static final String GOLDEN_DATA_REGION_HEX =
      "44415441424c4b2a000000610000005dffffffffffffffff00000040000000007e000000100000000600046b65793100"
          + "7fffffffffffffff0476616c75653100000000100000000600046b657932007fffffffffffffff0476616c7565320000"
          + "0000100000000600046b657933007fffffffffffffff0476616c7565330000000000";
  private static final String GOLDEN_ROOT_INDEX_BLOCK_HEX =
      "494458524f4f5432000000210000001dffffffffffffffff00000040000000003e000000000000000000000082100004"
          + "6b657931007fffffffffffffff0400000000";
  // Golden for a single block holding keys of different lengths whose first key (116 chars) lands in
  // the multi-byte vint range (keyLength 128). Pinned for both the native and HBase writers.
  private static final String GOLDEN_VARLEN_DATA_REGION_HEX =
      "44415441424c4b2a000000d1000000cdffffffffffffffff0000004000000000ee000000800000000200746161616161"
          + "616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161"
          + "616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161"
          + "616161616161616161616161616161007fffffffffffffff04763000000000140000000200086262626262626262007f"
          + "ffffffffffffff047631000000001800000002000c636363636363636363636363007fffffffffffffff047632000000"
          + "0000";
  private static final String GOLDEN_VARLEN_ROOT_INDEX_BLOCK_HEX =
      "494458524f4f5432000000920000008effffffffffffffff0000004000000000af0000000000000000000000f28f8000"
          + "746161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161"
          + "616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161"
          + "616161616161616161616161616161616161616161007fffffffffffffff0400000000";

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
    LOG.info("All validations passed!");
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

      // Each data block's previous-block-offset header (8 bytes at offset 16) must chain back to
      // the prior block, and -1 for the first, so an HBase reader's seekBefore can step back across
      // blocks. The writer once set it to the block's own offset; this guards that regression.
      byte[] file = Files.readAllBytes(Paths.get(testFile));
      List<IndexEntry> dataIndex = parseIndexBlock(
          file, (int) trailer.getLoadOnOpenDataOffset(), trailer.getDataIndexCount());
      assertTrue(dataIndex.size() > 1, "test setup: expected multiple data blocks");
      for (int i = 0; i < dataIndex.size(); i++) {
        long prevOffset = readLongBE(file, (int) dataIndex.get(i).offset + 16);
        long expected = i == 0 ? -1L : dataIndex.get(i - 1).offset;
        assertEquals(expected, prevOffset, "data block " + i + " previous-block offset");
      }
    }
  }

  /**
   * Format lock for the native writer's on-disk bytes. Single-block inputs are checked against the
   * HBase writer (proving the writers agree on the encoding): short keys (single-byte index
   * keyLength) and the 116-char boundary (keyLength 128, where Protobuf and Hadoop VInt diverge) are
   * also pinned to a golden; 500- and 2000-char first keys (3-byte keyLength) are checked against
   * HBase without a golden. The golden cases also validate the trailer and file info bytes. A single
   * block is deliberate for cross-writer checks: across blocks HBase stores shortened index separator
   * keys, so only a single full-key entry is byte-comparable. A multi-block native file then covers
   * the meta block and the rest of the section byte layout and round-trips.
   */
  @Test
  void writerBlockBytesAreStableFormatLock() throws Exception {
    byte[][] vals = {bytes("v0"), bytes("v1"), bytes("v2")};
    assertSingleBlockBytesMatchGoldenAndHBase(
        new String[] {"key1", "key2", "key3"},
        new byte[][] {bytes("value1"), bytes("value2"), bytes("value3")},
        GOLDEN_DATA_REGION_HEX, GOLDEN_ROOT_INDEX_BLOCK_HEX);
    assertSingleBlockBytesMatchGoldenAndHBase(
        new String[] {rep('a', 116), rep('b', 8), rep('c', 12)}, vals,
        GOLDEN_VARLEN_DATA_REGION_HEX, GOLDEN_VARLEN_ROOT_INDEX_BLOCK_HEX);
    // Larger first keys (3-byte index keyLength) checked against the HBase writer without a golden,
    // which for multi-KB key bytes would add no signal.
    assertSingleBlockBytesMatchHBase(new String[] {rep('a', 500), rep('b', 8), rep('c', 12)}, vals, null, null);
    assertSingleBlockBytesMatchHBase(new String[] {rep('a', 2000), rep('b', 8), rep('c', 12)}, vals, null, null);
    assertAllSectionsValidOnMultiBlockFile();
  }

  @Test
  void getVarIntBytesRoundTripsThroughReadVarLong() {
    // HFileIndexBlock.getVarIntBytes output must decode back via the reader's IOUtils.readVarLong.
    int[] testValues = {0, 1, 127, 128, 146, 200, 255, 256, 300, 1000, 32080, 65535, 100000,
        2034958, 632492350, Integer.MAX_VALUE};
    for (int value : testValues) {
      byte[] encoded = HFileIndexBlock.getVarIntBytes(value);
      int size = IOUtils.decodeVarLongSizeOnDisk(encoded, 0);
      assertEquals(encoded.length, size, "Size mismatch for value " + value);
      long decoded = IOUtils.readVarLong(encoded, 0, size);
      assertEquals(value, decoded, "Round-trip mismatch for value " + value);
    }
  }

  @Test
  void getVarIntBytesMatchesKnownHadoopVectors() {
    // Cross-check HFileIndexBlock.getVarIntBytes against known Hadoop WritableUtils VInt encodings.
    assertEquals(1, HFileIndexBlock.getVarIntBytes(0).length);
    assertEquals(0, HFileIndexBlock.getVarIntBytes(0)[0]);
    assertEquals(1, HFileIndexBlock.getVarIntBytes(98).length);
    assertEquals(98, HFileIndexBlock.getVarIntBytes(98)[0]);

    // Value 208 requires 2 bytes.
    byte[] enc208 = HFileIndexBlock.getVarIntBytes(208);
    assertEquals(2, enc208.length);
    assertEquals(208, IOUtils.readVarLong(enc208, 0));

    // Value 32080 requires 3 bytes.
    byte[] enc32080 = HFileIndexBlock.getVarIntBytes(32080);
    assertEquals(3, enc32080.length);
    assertEquals(32080, IOUtils.readVarLong(enc32080, 0));
  }

  /**
   * Pins a single-block file's data block and root index block bytes to a golden the HBase writer
   * must also reproduce, and validates the remaining sections of the native file (trailer, file
   * info, and the empty meta index). The file has no meta block: a meta block would shift the data
   * region and HBase serializes it differently, breaking the byte-parity (the meta block is covered
   * by {@link #assertAllSectionsValidOnMultiBlockFile()}).
   */
  private static void assertSingleBlockBytesMatchGoldenAndHBase(
      String[] keys, byte[][] values, String dataGolden, String rootGolden) throws Exception {
    assertSingleBlockBytesMatchHBase(keys, values, dataGolden, rootGolden);
    byte[] nativeFile = Files.readAllBytes(Paths.get(TEST_FILE));
    HFileProtos.TrailerProto trailer = validateTrailerSection(nativeFile, keys.length, 0);
    int loadOnOpen = (int) trailer.getLoadOnOpenDataOffset();
    int metaIndexOffset = loadOnOpen + HFILEBLOCK_HEADER_SIZE + readIntBE(nativeFile, loadOnOpen + 8);
    assertBytesEqual("empty meta index magic", ROOT_INDEX.getMagic(),
        Arrays.copyOfRange(nativeFile, metaIndexOffset, metaIndexOffset + 8));
    validateFileInfoSection(
        nativeFile, metaIndexOffset, (int) trailer.getFileInfoOffset(), keys[keys.length - 1]);
  }

  /**
   * Asserts the native and HBase writers produce identical single-block data and root index bytes
   * (optionally also matching a pinned golden), and the root-index keyLength is Hadoop VInt. Used
   * without a golden for large keys, where a hardcoded golden of multi-KB key bytes adds no signal.
   */
  private static void assertSingleBlockBytesMatchHBase(
      String[] keys, byte[][] values, String dataGolden, String rootGolden) throws Exception {
    writeNativeFile(TEST_FILE, HFileContext.builder().build(), keys, values, null, null);
    byte[] nativeFile = Files.readAllBytes(Paths.get(TEST_FILE));
    String[] nativeBlocks = dataAndRootIndexBlockHex(nativeFile);
    String[] hbaseBlocks = dataAndRootIndexBlockHex(writeHBaseFile(keys, values));
    assertEquals(hbaseBlocks[0], nativeBlocks[0], "native vs HBase data block bytes");
    assertEquals(hbaseBlocks[1], nativeBlocks[1], "native vs HBase root index bytes");
    if (dataGolden != null) {
      LOG.info("DATA_REGION_HEX={}", nativeBlocks[0]);
      LOG.info("ROOT_INDEX_BLOCK_HEX={}", nativeBlocks[1]);
      assertEquals(dataGolden, nativeBlocks[0], "data block bytes changed (storage-format change)");
      assertEquals(rootGolden, nativeBlocks[1], "root index bytes changed (storage-format change)");
    }
    IndexEntry first =
        parseIndexBlock(nativeFile, indexOf(nativeFile, ROOT_INDEX.getMagic()), 1).get(0);
    assertVarIntIsHadoopNotProtobuf("root index keyLength", first);
  }

  /** Returns {@code [dataRegionHex, rootIndexBlockHex]} for an HFile's raw bytes. */
  private static String[] dataAndRootIndexBlockHex(byte[] data) {
    int idxRootOffset = indexOf(data, HFileBlockType.ROOT_INDEX.getMagic());
    assertTrue(idxRootOffset > 0, "root index block not found");
    // Root index block: 33-byte v3 block header + onDiskSizeWithoutHeader payload (at header + 8).
    int onDiskSizeWithoutHeader = readIntBE(data, idxRootOffset + 8);
    return new String[] {
        hex(Arrays.copyOfRange(data, 0, idxRootOffset)),
        hex(Arrays.copyOfRange(data, idxRootOffset, idxRootOffset + 33 + onDiskSizeWithoutHeader))
    };
  }

  /** Writes the given records with the HBase HFile writer, matching the native writer's settings. */
  private static byte[] writeHBaseFile(String[] keys, byte[][] values) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    org.apache.hadoop.fs.Path path =
        new org.apache.hadoop.fs.Path(Files.createTempFile("hbase_fixed_", ".hfile").toString());
    org.apache.hadoop.hbase.io.hfile.HFileContext context = new HFileContextBuilder()
        .withBlockSize(1024 * 1024)
        .withCompression(Compression.Algorithm.NONE)
        .withChecksumType(org.apache.hadoop.hbase.util.ChecksumType.NULL)
        .withCellComparator(CellComparatorImpl.COMPARATOR)
        .withIncludesMvcc(true)
        .build();
    try (HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, path).withFileContext(context).create()) {
      for (int i = 0; i < keys.length; i++) {
        writer.append(new org.apache.hadoop.hbase.KeyValue(
            keys[i].getBytes(StandardCharsets.UTF_8), new byte[0], new byte[0],
            HConstants.LATEST_TIMESTAMP, values[i]));
      }
    }
    return Files.readAllBytes(Paths.get(path.toString()));
  }

  /**
   * Validates the raw bytes of every HFile section on a multi-block native file with one meta block:
   * scanned data blocks, the non-scanned meta block, the load-on-open root data index / meta index /
   * file info, and the trailer, then reads every record back. Index keyLengths are asserted Hadoop
   * {@code WritableUtils} VInt; the file info and trailer length prefixes protobuf-delimited. Each
   * section is checked in its own helper so callers stay under the per-test assertion limit.
   */
  private static void assertAllSectionsValidOnMultiBlockFile() throws IOException {
    int numRecords = 8;
    String[] keys = new String[numRecords];
    byte[][] values = new byte[numRecords][];
    for (int i = 0; i < numRecords; i++) {
      keys[i] = makeKey(200, i);
      values[i] = bytes("v" + i);
    }
    String metaKey = makeKey(200, 9999);
    byte[] metaValue = bytes("bloom-filter-payload-bytes");
    // Small block size forces one record per block, so the root index has multiple entries.
    writeNativeFile(
        TEST_FILE, new HFileContext.Builder().blockSize(100).build(), keys, values, metaKey, metaValue);
    byte[] file = Files.readAllBytes(Paths.get(TEST_FILE));

    HFileProtos.TrailerProto trailer = validateTrailerSection(file, numRecords, 1);
    int loadOnOpen = (int) trailer.getLoadOnOpenDataOffset();
    validateRootDataIndexSection(file, loadOnOpen, trailer.getDataIndexCount(), keys[0]);
    int metaIndexOffset = loadOnOpen + HFILEBLOCK_HEADER_SIZE + readIntBE(file, loadOnOpen + 8);
    validateMetaIndexAndBlock(file, metaIndexOffset, metaKey, metaValue);
    validateFileInfoSection(file, metaIndexOffset, (int) trailer.getFileInfoOffset(), keys[numRecords - 1]);
    assertAllRecordsReadBack(numRecords, keys);
  }

  /** Validates the trailer magic, protobuf framing, fields, and HFile version; returns the proto. */
  private static HFileProtos.TrailerProto validateTrailerSection(
      byte[] file, int numRecords, int metaIndexCount) throws IOException {
    int trailerStart = file.length - TRAILER_SIZE;
    assertBytesEqual("trailer magic",
        TRAILER.getMagic(), Arrays.copyOfRange(file, trailerStart, trailerStart + 8));
    HFileProtos.TrailerProto trailer = HFileProtos.TrailerProto.parseDelimitedFrom(
        new ByteArrayInputStream(file, trailerStart + 8, TRAILER_SIZE - 8));
    assertProtobufDelimitedFraming("trailer", file, trailerStart + 8, trailer.getSerializedSize());
    assertEquals(numRecords, trailer.getEntryCount());
    assertEquals(metaIndexCount, trailer.getMetaIndexCount());
    assertEquals(1, trailer.getNumDataIndexLevels());
    assertEquals(2, trailer.getCompressionCodec(), "compression codec must be NONE (2)");
    assertEquals("org.apache.hudi.io.storage.HoodieHBaseKVComparator",
        trailer.getComparatorClassName());
    assertEquals(0, trailer.getFirstDataBlockOffset(), "scanned section starts at offset 0");
    // HFile version: last 4 bytes hold the major version (3).
    assertEquals(3, readIntBE(file, file.length - 4), "HFile major version");
    return trailer;
  }

  /** Validates the scanned-section start and every root data index entry. */
  private static void validateRootDataIndexSection(
      byte[] file, int loadOnOpen, int dataIndexCount, String firstKey) throws IOException {
    assertBytesEqual("first data block magic", DATA.getMagic(), Arrays.copyOfRange(file, 0, 8));
    assertBytesEqual("root index magic",
        ROOT_INDEX.getMagic(), Arrays.copyOfRange(file, loadOnOpen, loadOnOpen + 8));
    List<IndexEntry> dataIndex = parseIndexBlock(file, loadOnOpen, dataIndexCount);
    assertEquals(dataIndexCount, dataIndex.size(), "root index entry count");
    for (IndexEntry e : dataIndex) {
      // keyLength must be Hadoop WritableUtils VInt (never protobuf varint), and each entry must
      // point at a real DATA block whose on-disk size matches the entry's size.
      assertVarIntIsHadoopNotProtobuf("data index keyLength", e);
      assertBytesEqual("data block magic at index offset",
          DATA.getMagic(), Arrays.copyOfRange(file, (int) e.offset, (int) e.offset + 8));
      assertEquals(HFILEBLOCK_HEADER_SIZE + readIntBE(file, (int) e.offset + 8), e.size,
          "data index entry size must equal the on-disk size of the referenced block");
    }
    assertEquals(firstKey, keyContentString(dataIndex.get(0).key), "first index entry key content");
  }

  /** Validates the meta index entry and the meta block it points at. */
  private static void validateMetaIndexAndBlock(
      byte[] file, int metaIndexOffset, String metaKey, byte[] metaValue) throws IOException {
    assertBytesEqual("meta index magic",
        ROOT_INDEX.getMagic(), Arrays.copyOfRange(file, metaIndexOffset, metaIndexOffset + 8));
    IndexEntry metaEntry = parseIndexBlock(file, metaIndexOffset, 1).get(0);
    assertVarIntIsHadoopNotProtobuf("meta index keyLength", metaEntry);
    assertBytesEqual("meta index key bytes", metaKey.getBytes(StandardCharsets.UTF_8), metaEntry.key);
    assertBytesEqual("meta block magic",
        META.getMagic(), Arrays.copyOfRange(file, (int) metaEntry.offset, (int) metaEntry.offset + 8));
    int metaPayload = (int) metaEntry.offset + HFILEBLOCK_HEADER_SIZE;
    assertBytesEqual("meta block payload == meta value",
        metaValue, Arrays.copyOfRange(file, metaPayload, metaPayload + metaValue.length));
  }

  /** Validates the file info block offset, magic, PBUF framing, and decoded entries. */
  private static void validateFileInfoSection(
      byte[] file, int metaIndexOffset, int fileInfoOffset, String lastKey) throws IOException {
    int metaIndexBlockSize = HFILEBLOCK_HEADER_SIZE + readIntBE(file, metaIndexOffset + 8);
    assertEquals(metaIndexOffset + metaIndexBlockSize, fileInfoOffset,
        "trailer fileInfoOffset must point right after the meta index block");
    assertBytesEqual("file info magic",
        FILE_INFO.getMagic(), Arrays.copyOfRange(file, fileInfoOffset, fileInfoOffset + 8));
    int pbufStart = fileInfoOffset + HFILEBLOCK_HEADER_SIZE;
    assertBytesEqual("file info PBUF magic",
        "PBUF".getBytes(StandardCharsets.UTF_8), Arrays.copyOfRange(file, pbufStart, pbufStart + 4));
    HFileProtos.InfoProto info = HFileProtos.InfoProto.parseDelimitedFrom(
        new ByteArrayInputStream(file, pbufStart + 4, file.length - (pbufStart + 4)));
    // File info uses protobuf-delimited framing (correct here, unlike the index keyLength fields).
    assertProtobufDelimitedFraming("file info", file, pbufStart + 4, info.getSerializedSize());
    Map<String, byte[]> infoMap = new LinkedHashMap<>();
    for (HFileProtos.BytesBytesPair pair : info.getMapEntryList()) {
      infoMap.put(new String(pair.getFirst().toByteArray(), StandardCharsets.UTF_8),
          pair.getSecond().toByteArray());
    }
    for (UTF8StringKey required : new UTF8StringKey[] {
        LAST_KEY, KEY_VALUE_VERSION, MAX_MVCC_TS_KEY, AVG_KEY_LEN, AVG_VALUE_LEN}) {
      assertTrue(infoMap.containsKey(utf8(required)), "file info must contain " + utf8(required));
    }
    assertEquals(lastKey, keyContentString(infoMap.get(utf8(LAST_KEY))),
        "LASTKEY content must be the last appended key");
  }

  /** Reads every record back through the native reader. */
  private static void assertAllRecordsReadBack(int numRecords, String[] keys) throws IOException {
    try (FileChannel channel = FileChannel.open(Paths.get(TEST_FILE), StandardOpenOption.READ)) {
      ByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
      HFileReaderImpl reader = new HFileReaderImpl(
          new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(buf)), channel.size());
      reader.initializeMetadata();
      assertEquals(numRecords, reader.getNumKeyValueEntries());
      reader.seekTo();
      for (int i = 0; i < numRecords; i++) {
        assertEquals(keys[i], reader.getKeyValue().get().getKey().getContentInString());
        reader.next();
      }
    }
  }

  /** Writes the given records (and an optional single meta block) with the native HFile writer. */
  private static void writeNativeFile(String testFile, HFileContext context, String[] keys,
                                      byte[][] values, String metaKey, byte[] metaValue)
      throws IOException {
    try (DataOutputStream outputStream =
             new DataOutputStream(Files.newOutputStream(Paths.get(testFile)));
         HFileWriter writer = new HFileWriterImpl(context, outputStream)) {
      for (int i = 0; i < keys.length; i++) {
        writer.append(keys[i], values[i]);
      }
      if (metaKey != null) {
        writer.appendMetaInfo(metaKey, metaValue);
      }
    }
  }

  /** A parsed block index entry: block offset, on-disk size, raw keyLength vint bytes, and key. */
  private static final class IndexEntry {
    long offset;
    int size;
    byte[] keyLengthVarIntBytes;
    int keyLen;
    byte[] key;
  }

  /** Parses {@code numEntries} block index entries starting at the given index block offset. */
  private static List<IndexEntry> parseIndexBlock(byte[] file, int blockStart, int numEntries) {
    List<IndexEntry> entries = new ArrayList<>();
    int p = blockStart + HFILEBLOCK_HEADER_SIZE;
    for (int i = 0; i < numEntries; i++) {
      IndexEntry e = new IndexEntry();
      e.offset = readLongBE(file, p);
      e.size = readIntBE(file, p + 8);
      int varIntSize = IOUtils.decodeVarLongSizeOnDisk(file, p + 12);
      e.keyLen = (int) IOUtils.readVarLong(file, p + 12, varIntSize);
      e.keyLengthVarIntBytes = Arrays.copyOfRange(file, p + 12, p + 12 + varIntSize);
      e.key = Arrays.copyOfRange(file, p + 12 + varIntSize, p + 12 + varIntSize + e.keyLen);
      entries.add(e);
      p += 12 + varIntSize + e.keyLen;
    }
    return entries;
  }

  /**
   * Asserts that an index entry's keyLength bytes equal Hadoop {@code WritableUtils} VInt (the
   * encoding the reader and HBase decode), and for values >= 128, that they are NOT protobuf
   * varint (the pre-fix encoding that produced negative key lengths in HBase's reader).
   */
  private static void assertVarIntIsHadoopNotProtobuf(String message, IndexEntry e)
      throws IOException {
    assertBytesEqual(message + " must be Hadoop WritableUtils VInt",
        hadoopWritableUtilsVInt(e.keyLen), e.keyLengthVarIntBytes);
    if (e.keyLen >= 128) {
      assertNotEquals(hex(protobufVarInt(e.keyLen)), hex(e.keyLengthVarIntBytes),
          message + " must NOT be protobuf varint for values >= 128 (the original bug)");
    }
  }

  /** Asserts the on-disk length prefix at {@code offset} is a protobuf varint of {@code size}. */
  private static void assertProtobufDelimitedFraming(String section, byte[] file, int offset,
                                                     int serializedSize) throws IOException {
    byte[] expectedPrefix = protobufVarInt(serializedSize);
    assertBytesEqual(section + " must use protobuf-delimited framing",
        expectedPrefix, Arrays.copyOfRange(file, offset, offset + expectedPrefix.length));
  }

  /** Reference Hadoop {@code WritableUtils} VInt encoder (the encoding HBase's reader expects). */
  private static byte[] hadoopWritableUtilsVInt(long value) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    WritableUtils.writeVInt(out, (int) value);
    return Arrays.copyOf(out.getData(), out.getLength());
  }

  /** Reference protobuf varint encoder (the pre-fix, HBase-incompatible encoding for the index). */
  private static byte[] protobufVarInt(int value) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CodedOutputStream cos = CodedOutputStream.newInstance(baos);
    cos.writeUInt32NoTag(value);
    cos.flush();
    return baos.toByteArray();
  }

  /** Extracts the row (key content) bytes from a serialized HBase KeyValue key, as a String. */
  private static String keyContentString(byte[] keyValueKey) {
    int rowLength = ((keyValueKey[0] & 0xff) << 8) | (keyValueKey[1] & 0xff);
    return new String(keyValueKey, 2, rowLength, StandardCharsets.UTF_8);
  }

  /** Builds a length-{@code len} key that sorts by {@code i} and is unique. */
  private static String makeKey(int len, int i) {
    String suffix = String.format("%04d", i);
    StringBuilder sb = new StringBuilder(len);
    for (int k = 0; k < len - suffix.length(); k++) {
      sb.append('a');
    }
    return sb.append(suffix).toString();
  }

  private static String utf8(UTF8StringKey key) {
    return new String(key.getBytes(), StandardCharsets.UTF_8);
  }

  private static void assertBytesEqual(String message, byte[] expected, byte[] actual) {
    assertEquals(hex(expected), hex(actual), message);
  }

  private static String rep(char c, int n) {
    char[] a = new char[n];
    Arrays.fill(a, c);
    return new String(a);
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static long readLongBE(byte[] b, int off) {
    long v = 0;
    for (int i = 0; i < 8; i++) {
      v = (v << 8) | (b[off + i] & 0xffL);
    }
    return v;
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
    // Each root block-index entry carries the 10-byte HBase KeyValue suffix (column-family
    // length + timestamp + key type). This file has one index entry, so the size grows by 10.
    long expectedSize = 4547;
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

  private static int indexOf(byte[] haystack, byte[] needle) {
    outer:
    for (int i = 0; i + needle.length <= haystack.length; i++) {
      for (int j = 0; j < needle.length; j++) {
        if (haystack[i + j] != needle[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  private static int readIntBE(byte[] b, int off) {
    return ((b[off] & 0xff) << 24) | ((b[off + 1] & 0xff) << 16)
        | ((b[off + 2] & 0xff) << 8) | (b[off + 3] & 0xff);
  }

  private static String hex(byte[] b) {
    StringBuilder sb = new StringBuilder(b.length * 2);
    for (byte x : b) {
      sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
    }
    return sb.toString();
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
