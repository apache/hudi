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
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.hudi.io.hfile.HFileInfo.KEY_VALUE_VERSION;
import static org.apache.hudi.io.util.FileIOUtils.readAsByteArray;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.apache.hudi.io.util.IOUtils.toBytes;

class TestHFileReadCompatibility {
  // Test data - simple key-value pairs
  private static final List<TestRecord> TEST_RECORDS = Arrays.asList(
      new TestRecord("row1", "value1"),
      new TestRecord("row2", "value2"),
      new TestRecord("row3", "value3"),
      new TestRecord("row4", "value4"),
      new TestRecord("row5", "value5")
  );

  // Small block size + many records => many data blocks => block-boundary keys land in the root
  // index, which is exactly where the HBase point-lookup parsing happened.
  private static final int MULTI_BLOCK_RECORDS = 2000;
  private static final int SMALL_BLOCK_SIZE = 512;

  @ParameterizedTest
  @CsvSource({
      "/hfile/hudi-generated.hfile,/hfile/hbase-generated.hfile",
      "/hfile/hbase-generated.hfile,/hfile/hudi-generated.hfile"
  })
  void testHFileReadCompatibility(String hudiFilePath, String hbaseFilePath) throws Exception {
    try (HFileReader hudiReader = createHFileReaderFromResource(hudiFilePath);
        org.apache.hadoop.hbase.io.hfile.HFile.Reader hbaseReader =
            createHBaseHFileReaderFromResource(hbaseFilePath)) {
      // Validate number of entries.
      Assertions.assertEquals(5, hudiReader.getNumKeyValueEntries());
      Assertions.assertEquals(5, hbaseReader.getEntries());
      // Validate data block content.
      hudiReader.seekTo();
      HFileScanner scanner = hbaseReader.getScanner(true, true);
      scanner.seekTo();
      int i = 0;
      do {
        org.apache.hudi.io.hfile.KeyValue keyValue = hudiReader.getKeyValue().get();
        Cell cell = scanner.getCell();
        // Ensure Hudi record is correct.
        Assertions.assertEquals(TEST_RECORDS.get(i).key, keyValue.getKey().getContentInString());
        byte[] value = Arrays.copyOfRange(
            keyValue.getBytes(),
            keyValue.getValueOffset(),
            keyValue.getValueOffset() + keyValue.getValueLength());
        Assertions.assertArrayEquals(value, TEST_RECORDS.get(i).value.getBytes());
        // Ensure Hbase record is correct.
        byte[] key = Arrays.copyOfRange(
            cell.getRowArray(),
            cell.getRowOffset(),
            cell.getRowOffset() + cell.getRowLength());
        Assertions.assertArrayEquals(TEST_RECORDS.get(i).key.getBytes(), key);
        value = Arrays.copyOfRange(
            cell.getValueArray(),
            cell.getValueOffset(),
            cell.getValueOffset() + cell.getValueLength());
        Assertions.assertArrayEquals(value, TEST_RECORDS.get(i).value.getBytes());
        i++;
      } while (hudiReader.next() && scanner.next());

      // Compare some meta information.
      // LAST KEY.
      Assertions.assertTrue(hbaseReader.getHFileInfo().containsKey(HFileInfo.LAST_KEY.getBytes()));
      Assertions.assertTrue(hudiReader.getMetaInfo(HFileInfo.LAST_KEY).isPresent());
      // The last key value returned from hbase contains the extra fields,
      // e.g., column family, column qualifier, timestamp, key type, which is 10 more bytes.
      // Therefore, the last key value from hudi should be the prefix since hudi does not use these
      // extra fields.
      if (hudiReader.getMetaInfo(HFileInfo.LAST_KEY).get().length
          < hbaseReader.getHFileInfo().get(HFileInfo.LAST_KEY.getBytes()).length) {
        Assertions.assertTrue(isPrefix(
            hudiReader.getMetaInfo(HFileInfo.LAST_KEY).get(),
            hbaseReader.getHFileInfo().get(HFileInfo.LAST_KEY.getBytes())));
      } else {
        Assertions.assertTrue(isPrefix(
            hbaseReader.getHFileInfo().get(HFileInfo.LAST_KEY.getBytes()),
            hudiReader.getMetaInfo(HFileInfo.LAST_KEY).get()));
      }
      // Average key length.
      Assertions.assertTrue(hbaseReader.getHFileInfo().containsKey(HFileInfo.AVG_KEY_LEN.getBytes()));
      Assertions.assertTrue(hudiReader.getMetaInfo(HFileInfo.AVG_KEY_LEN).isPresent());
      // Average value length.
      Assertions.assertTrue(hbaseReader.getHFileInfo().containsKey(HFileInfo.AVG_VALUE_LEN.getBytes()));
      Assertions.assertTrue(hudiReader.getMetaInfo(HFileInfo.AVG_VALUE_LEN).isPresent());
      Assertions.assertTrue(
          hbaseReader.getHFileInfo().getAvgValueLen()
              >= readInt(hudiReader.getMetaInfo(HFileInfo.AVG_VALUE_LEN).get(), 0));
      // MVCC.
      Assertions.assertTrue(hbaseReader.getHFileInfo().shouldIncludeMemStoreTS());
      // Note that MemStoreTS is not set.
      Assertions.assertFalse(hbaseReader.getHFileInfo().isDecodeMemstoreTS());
      Assertions.assertTrue(hudiReader.getMetaInfo(KEY_VALUE_VERSION).isPresent());
      Assertions.assertTrue(hudiReader.getMetaInfo(HFileInfo.MAX_MVCC_TS_KEY).isPresent());
      Assertions.assertEquals(0L,
          IOUtils.readLong(
          hudiReader.getMetaInfo(HFileInfo.MAX_MVCC_TS_KEY).get(), 0));
    }
  }

  @Test
  void testHbaseReaderSucceedsWhenKeyValueVersionIsSetTo1() throws IOException {
    String fileName = "hudi-generated-for-keyvalue-versions";
    Path tempFile = new Path(Files.createTempFile(fileName, ".hfile").toString());
    // By default this value is set to 1. Here we explicitly set it to 1 for test purpose.
    writeHFileWithHudi(tempFile, 1);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    // Create HBase HFile.Reader from the temporary file
    HFile.Reader reader = HFile.createReader(fs, new Path(tempFile.toString()), conf);
    byte[] keyValueVersion = reader.getHFileInfo().get(KEY_VALUE_VERSION.getBytes());
    Assertions.assertEquals(1, IOUtils.readInt(keyValueVersion, 0));
    // Values from trailer still works.
    Assertions.assertEquals(5, reader.getEntries());
    // Scanning the file succeeds.
    HFileScanner scanner = reader.getScanner(true, true);
    scanner.seekTo();
    Assertions.assertDoesNotThrow(() -> {
      int i = 0;
      do {
        Cell cell = scanner.getCell();
        byte[] key = Arrays.copyOfRange(
            cell.getRowArray(),
            cell.getRowOffset(),
            cell.getRowOffset() + cell.getRowLength());
        Assertions.assertArrayEquals(TEST_RECORDS.get(i).key.getBytes(), key);
        i++;
      } while (scanner.next());
    });
  }

  /**
   * Point-lookup compatibility: an HBase reader seeks every key (which necessarily includes the
   * block-boundary keys stored in the root index) in a native-written multi-block file. Before the
   * {@link HFileRootIndexBlock} fix the block-index first key lacked the HBase KeyValue suffix, so
   * seeking a boundary key read the family-length byte past the end and threw
   * {@code ArrayIndexOutOfBoundsException} in {@code KeyValue.getFamilyLength}. The scan-based test
   * above never exercised this path.
   */
  @ParameterizedTest
  @EnumSource(value = CompressionCodec.class, names = {"NONE", "GZIP"})
  void hbaseReaderPointLooksUpEveryKeyInNativeMultiBlockFile(CompressionCodec codec)
      throws IOException {
    byte[] data = writeMultiBlockHudiHFile(MULTI_BLOCK_RECORDS, SMALL_BLOCK_SIZE, codec);
    try (HFile.Reader reader = createHBaseHFileReader(data)) {
      int blocks = reader.getTrailer().getDataIndexCount();
      Assertions.assertTrue(blocks > 1, "expected a multi-block file; got " + blocks);
      Assertions.assertEquals(MULTI_BLOCK_RECORDS, reader.getEntries());
      HFileScanner scanner = reader.getScanner(true, true);
      for (int i = 0; i < MULTI_BLOCK_RECORDS; i++) {
        KeyValue probe = new KeyValue(Bytes.toBytes(key(i)), null, null, null);
        Assertions.assertEquals(0, scanner.seekTo(probe), "expected exact match for " + key(i));
        Cell cell = scanner.getCell();
        Assertions.assertEquals(key(i),
            Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
        Assertions.assertEquals(value(i),
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      }
    }
  }

  /** {@code midKey()} parses a block-index key as a {@code KeyValue}; pre-fix this threw AIOOBE. */
  @Test
  void hbaseReaderMidKeyParsesNativeBlockIndexKey() throws IOException {
    byte[] data = writeMultiBlockHudiHFile(MULTI_BLOCK_RECORDS, SMALL_BLOCK_SIZE, CompressionCodec.NONE);
    try (HFile.Reader reader = createHBaseHFileReader(data)) {
      Assertions.assertTrue(reader.midKey().isPresent());
    }
  }

  /**
   * Byte comparison under both NONE and GZIP: given identical records, the native writer and the
   * HBase writer produce cells that an HBase reader sees as byte-identical (key bytes and value
   * bytes), establishing that the native writer emits HBase-format cells.
   */
  @ParameterizedTest
  @EnumSource(value = CompressionCodec.class, names = {"NONE", "GZIP"})
  void nativeAndHBaseWrittenCellsAreByteIdenticalUnderHBaseReader(CompressionCodec codec)
      throws IOException {
    byte[] nativeData = writeMultiBlockHudiHFile(MULTI_BLOCK_RECORDS, SMALL_BLOCK_SIZE, codec);
    byte[] hbaseData =
        writeMultiBlockHBaseHFile(MULTI_BLOCK_RECORDS, SMALL_BLOCK_SIZE, hbaseAlgo(codec));
    try (HFile.Reader nativeReader = createHBaseHFileReader(nativeData);
         HFile.Reader hbaseReader = createHBaseHFileReader(hbaseData)) {
      Assertions.assertEquals(hbaseReader.getEntries(), nativeReader.getEntries());
      HFileScanner ns = nativeReader.getScanner(true, true);
      HFileScanner hs = hbaseReader.getScanner(true, true);
      Assertions.assertTrue(ns.seekTo());
      Assertions.assertTrue(hs.seekTo());
      int compared = 0;
      boolean nativeHasNext;
      boolean hbaseHasNext;
      do {
        Cell nativeCell = ns.getCell();
        Cell hbaseCell = hs.getCell();
        Assertions.assertArrayEquals(keyBytes(nativeCell), keyBytes(hbaseCell),
            "KeyValue key bytes differ at record " + compared);
        Assertions.assertArrayEquals(valueBytes(nativeCell), valueBytes(hbaseCell),
            "value bytes differ at record " + compared);
        compared++;
        nativeHasNext = ns.next();
        hbaseHasNext = hs.next();
      } while (nativeHasNext && hbaseHasNext);
      Assertions.assertEquals(MULTI_BLOCK_RECORDS, compared);
      Assertions.assertFalse(nativeHasNext, "native scanner had extra cells");
      Assertions.assertFalse(hbaseHasNext, "hbase scanner had extra cells");
    }
  }

  /**
   * Cross-reader equivalence: the same native-written multi-block file reads identically through
   * the native hudi-io reader and the HBase reader (same rows and values, in order).
   */
  @Test
  void nativeWrittenFileReadsIdenticallyByBothReaders() throws IOException {
    byte[] data = writeMultiBlockHudiHFile(MULTI_BLOCK_RECORDS, SMALL_BLOCK_SIZE, CompressionCodec.NONE);
    try (HFileReader nativeReader = createHFileReader(data);
         HFile.Reader hbaseReader = createHBaseHFileReader(data)) {
      Assertions.assertEquals(MULTI_BLOCK_RECORDS, hbaseReader.getEntries());
      nativeReader.seekTo();
      HFileScanner hbaseScanner = hbaseReader.getScanner(true, true);
      Assertions.assertTrue(hbaseScanner.seekTo());
      for (int i = 0; i < MULTI_BLOCK_RECORDS; i++) {
        org.apache.hudi.io.hfile.KeyValue nativeKv = nativeReader.getKeyValue().get();
        Cell hbaseCell = hbaseScanner.getCell();
        Assertions.assertEquals(key(i), nativeKv.getKey().getContentInString());
        Assertions.assertEquals(key(i),
            Bytes.toString(hbaseCell.getRowArray(), hbaseCell.getRowOffset(), hbaseCell.getRowLength()));
        byte[] nativeValue = Arrays.copyOfRange(nativeKv.getBytes(), nativeKv.getValueOffset(),
            nativeKv.getValueOffset() + nativeKv.getValueLength());
        Assertions.assertArrayEquals(value(i).getBytes(StandardCharsets.UTF_8), nativeValue);
        Assertions.assertArrayEquals(nativeValue, valueBytes(hbaseCell));
        if (i < MULTI_BLOCK_RECORDS - 1) {
          Assertions.assertTrue(nativeReader.next());
          Assertions.assertTrue(hbaseScanner.next());
        }
      }
    }
  }

  static boolean isPrefix(byte[] prefix, byte[] array) {
    if (prefix.length > array.length) {
      return false; // can't be prefix if longer
    }
    for (int i = 0; i < prefix.length; i++) {
      if (prefix[i] != array[i]) {
        return false;
      }
    }
    return true;
  }

  static HFileReader createHFileReaderFromResource(String fileName) throws IOException {
    return createHFileReader(readHFileFromResources(fileName));
  }

  static HFileReader createHFileReader(byte[] hfileData) {
    SeekableDataInputStream inputStream = new ByteArraySeekableDataInputStream(
        new ByteBufferBackedInputStream(ByteBuffer.wrap(hfileData)));
    return new HFileReaderImpl(inputStream, hfileData.length);
  }

  static HFile.Reader createHBaseHFileReaderFromResource(String fileName) throws IOException {
    return createHBaseHFileReader(readHFileFromResources(fileName));
  }

  static HFile.Reader createHBaseHFileReader(byte[] hfileData) throws IOException {
    Path tempFile = new Path(Files.createTempFile("hbase_hfile_", ".hfile").toString());
    Files.write(Paths.get(tempFile.toString()), hfileData);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    return HFile.createReader(fs, tempFile, conf);
  }

  private static byte[] readHFileFromResources(String filename) throws IOException {
    long size = Objects.requireNonNull(TestHFileReadCompatibility.class.getResource(filename))
        .openConnection().getContentLength();
    return readAsByteArray(
        TestHFileReader.class.getResourceAsStream(filename), (int) size);
  }

  /**
   * The following are the functions used to generate hfile used in the tests.
   */
  void testWriteHFiles() throws IOException, URISyntaxException {
    String hbaseFile = Paths.get("src/test/resources/hfile/hbase-generated.hfile").toAbsolutePath().toString();
    String hudiFile = Paths.get("src/test/resources/hfile/hudi-generated.hfile").toAbsolutePath().toString();
    writeHFileWithHbase(new Path(hbaseFile));
    writeHFileWithHudi(new Path(hudiFile));
  }

  private void writeHFileWithHbase(Path filePath) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // Create HFile context with appropriate settings
    HFileContext context = new HFileContextBuilder()
        .withBlockSize(64 * 1024)
        .withCompression(Compression.Algorithm.NONE)
        .withCellComparator(CellComparatorImpl.COMPARATOR)
        .withIncludesMvcc(true)
        .build();

    // Create HBase HFile writer
    try (HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, filePath).withFileContext(context).create()) {
      // Write test records as HBase KeyValue objects
      for (TestRecord record : TEST_RECORDS) {
        KeyValue kv = new KeyValue(
            Bytes.toBytes(record.key),           // row
            new byte[0],                         // family
            new byte[0],                         // qualifier
            0L,                                  // timestamp
            Bytes.toBytes(record.value)          // value
        );
        writer.append(kv);
      }
    }
  }

  private void writeHFileWithHudi(Path filePath) throws IOException {
    writeHFileWithHudi(filePath, 1);
  }

  private void writeHFileWithHudi(Path filePath, int keyValueVersion) throws IOException {
    org.apache.hudi.io.hfile.HFileContext context = org.apache.hudi.io.hfile.HFileContext.builder()
        .blockSize(64 * 1024)
        .build();
    try (DataOutputStream outputStream = new DataOutputStream(
        Files.newOutputStream(Paths.get(filePath.toString())));
         HFileWriter writer = new HFileWriterImpl(context, outputStream)) {
      for (TestRecord record : TEST_RECORDS) {
        writer.append(record.key, record.value.getBytes("UTF-8"));
      }
      writer.appendMetaInfo("bloom_filter", "random_string".getBytes());
      // To validate if a specific KEY_VALUE_VERSION value should be set.
      if (keyValueVersion != 1) {
        writer.appendFileInfo(
            new String(KEY_VALUE_VERSION.getBytes(), StandardCharsets.UTF_8), toBytes(keyValueVersion));
      }
    }
  }

  private static String key(int i) {
    return String.format("key%06d", i);
  }

  private static String value(int i) {
    return "value-" + i;
  }

  private static Compression.Algorithm hbaseAlgo(CompressionCodec codec) {
    return codec == CompressionCodec.GZIP ? Compression.Algorithm.GZ : Compression.Algorithm.NONE;
  }

  private static byte[] writeMultiBlockHudiHFile(int numRecords, int blockSize, CompressionCodec codec)
      throws IOException {
    org.apache.hudi.io.hfile.HFileContext context = org.apache.hudi.io.hfile.HFileContext.builder()
        .blockSize(blockSize).compressionCodec(codec).build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (HFileWriter writer = new HFileWriterImpl(context, baos)) {
      for (int i = 0; i < numRecords; i++) {
        writer.append(key(i), value(i).getBytes(StandardCharsets.UTF_8));
      }
    }
    return baos.toByteArray();
  }

  // Same cells the native writer emits: family length 0, timestamp = LATEST, type = Put.
  private static byte[] writeMultiBlockHBaseHFile(int numRecords, int blockSize,
                                                  Compression.Algorithm algo) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path path = new Path(Files.createTempFile("hbase_write_", ".hfile").toString());
    HFileContext context = new HFileContextBuilder()
        .withBlockSize(blockSize)
        .withCompression(algo)
        .withCellComparator(CellComparatorImpl.COMPARATOR)
        .withIncludesMvcc(true)
        .build();
    try (HFile.Writer writer = HFile.getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, path).withFileContext(context).create()) {
      for (int i = 0; i < numRecords; i++) {
        writer.append(new KeyValue(Bytes.toBytes(key(i)), new byte[0], new byte[0],
            HConstants.LATEST_TIMESTAMP, value(i).getBytes(StandardCharsets.UTF_8)));
      }
    }
    return Files.readAllBytes(Paths.get(path.toString()));
  }

  private static byte[] keyBytes(Cell c) {
    KeyValue kv = new KeyValue(c.getRowArray(), c.getRowOffset(), c.getRowLength(),
        c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength(),
        c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
        c.getTimestamp(), KeyValue.Type.codeToType(c.getTypeByte()),
        c.getValueArray(), c.getValueOffset(), c.getValueLength());
    return Arrays.copyOfRange(kv.getKey(), 0, kv.getKeyLength());
  }

  private static byte[] valueBytes(Cell c) {
    return Arrays.copyOfRange(c.getValueArray(), c.getValueOffset(),
        c.getValueOffset() + c.getValueLength());
  }

  // Simple test record class
  private static class TestRecord {
    final String key;
    final String value;
    
    TestRecord(String key, String value) {
      this.key = key;
      this.value = value;
    }
    
    @Override
    public String toString() {
      return "TestRecord{key='" + key + "', value='" + value + "'}";
    }
  }
}
