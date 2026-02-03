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

import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
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

import static org.apache.hudi.io.util.FileIOUtils.readAsByteArray;
import static org.apache.hudi.io.hfile.HFileInfo.KEY_VALUE_VERSION;
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
    // Read HFile data from resources
    byte[] hfileData = readHFileFromResources(fileName);
    // Convert to ByteBuffer
    ByteBuffer buffer = ByteBuffer.wrap(hfileData);
    // Create SeekableDataInputStream
    SeekableDataInputStream inputStream = new ByteArraySeekableDataInputStream(
        new ByteBufferBackedInputStream(buffer)
    );
    // Create and return HFileReaderImpl
    return new HFileReaderImpl(inputStream, hfileData.length);
  }

  static HFile.Reader createHBaseHFileReaderFromResource(String fileName) throws IOException {
    // Read HFile data from resources
    byte[] hfileData = readHFileFromResources(fileName);
    // Create a temporary file to write the HFile data
    Path tempFile = new Path(Files.createTempFile("hbase_hfile_", ".hfile").toString());
    try {
      // Write the byte array to temporary file
      Files.write(Paths.get(tempFile.toString()), hfileData);
      // Create Hadoop Configuration and FileSystem
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      // Create HBase HFile.Reader from the temporary file
      HFile.Reader reader = HFile.createReader(fs, new Path(tempFile.toString()), conf);
      // Note: The temporary file will be cleaned up when the reader is closed
      // or you can manually delete it after use
      return reader;
    } catch (IOException e) {
      // Clean up temp file if creation fails
      Files.deleteIfExists(Paths.get(tempFile.toString()));
      throw e;
    }
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
