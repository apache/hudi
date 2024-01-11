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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.hudi.common.util.FileIOUtils.readAsByteArray;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHFileReader {
  public static final String SIMPLE_SCHEMA_HFILE_SUFFIX = "_simple.hfile";
  public static final String COMPLEX_SCHEMA_HFILE_SUFFIX = "_complex.hfile";
  public static final String BOOTSTRAP_INDEX_HFILE_SUFFIX = "_bootstrap_index_partitions.hfile";

  @Test
  public void testReadHFile() throws IOException {
    int bufSize = 16 * 1024 * 1024;
    Path path = new Path("file:/Users/ethan/Work/tmp/hfile-reader/hfile_keyed.hfile");
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (FSDataInputStream stream = fs.open(path, bufSize)) {
      HFileReaderImpl reader = new HFileReaderImpl(stream, fileStatus.getLen());
      reader.initializeMetadata();
      reader.seekTo();
      assertTrue(reader.getKeyValue().isPresent());
      int count = 1;
      while (reader.next()) {
        Option<KeyValue> keyValueOption = reader.getKeyValue();
        assertTrue(keyValueOption.isPresent());
        count++;
      }

      assertEquals(1000, count);
    }
  }

  @Test
  public void testReadHFilePointLookup() throws IOException {
    int bufSize = 16 * 1024 * 1024;
    Path path = new Path("file:/Users/ethan/Work/tmp/hfile-reader/hfile_keyed.hfile");
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (FSDataInputStream stream = fs.open(path, bufSize)) {
      HFileReaderImpl reader = new HFileReaderImpl(stream, fileStatus.getLen());
      reader.initializeMetadata();
      reader.seekTo();
      Key key = new UTF8StringKey("abc-00000030");
      int r = reader.seekTo(key);
      assertEquals(-1, r);

      key = new UTF8StringKey("key-00000000");
      r = reader.seekTo(key);
      assertEquals(0, r);
      assertEquals(key, reader.getKeyValue().get().getKey());

      key = new UTF8StringKey("key-00000050");
      r = reader.seekTo(key);
      assertEquals(0, r);
      assertEquals(key, reader.getKeyValue().get().getKey());

      key = new UTF8StringKey("key-000000501");
      r = reader.seekTo(key);
      assertEquals(1, r);
      assertEquals(new UTF8StringKey("key-00000050"), reader.getKeyValue().get().getKey());

      key = new UTF8StringKey("key-000000502");
      r = reader.seekTo(key);
      assertEquals(1, r);
      assertEquals(new UTF8StringKey("key-00000050"), reader.getKeyValue().get().getKey());

      key = new UTF8StringKey("key-00000051");
      r = reader.seekTo(key);
      assertEquals(0, r);
      assertEquals(key, reader.getKeyValue().get().getKey());


      key = new UTF8StringKey("key-00000651");
      r = reader.seekTo(key);
      assertEquals(0, r);
      assertEquals(key, reader.getKeyValue().get().getKey());

      key = new UTF8StringKey("key-00000999");
      r = reader.seekTo(key);
      assertEquals(0, r);
      assertEquals(key, reader.getKeyValue().get().getKey());

      key = new UTF8StringKey("key-00001000");
      r = reader.seekTo(key);
      assertEquals(2, r);
      assertFalse(reader.getKeyValue().isPresent());

      assertEquals(false, reader.next());
      assertEquals(false, reader.next());
      key = new UTF8StringKey("key-00001003");
      r = reader.seekTo(key);
      assertEquals(2, r);
      assertFalse(reader.getKeyValue().isPresent());

      //Option<KeyValue> kv0 = reader.seekTo(new UTF8StringKey("abc-00000000"));
      //assertFalse(kv0.isPresent());
      //Option<KeyValue> kv1 = reader.seekTo(new UTF8StringKey("key-00000000"));
      //assertEquals("val-00000000", getValue(kv1.get()));
      //Option<KeyValue> kv2 = reader.seekTo(new UTF8StringKey("key-00000050"));
      //assertEquals("val-00000050", getValue(kv2.get()));
      //Option<KeyValue> kv3 = reader.seekTo(new UTF8StringKey("key-00000536"));
      //assertEquals("val-00000536", getValue(kv3.get()));
      //Option<KeyValue> kv4 = reader.seekTo(new UTF8StringKey("key-10000000"));
      //assertFalse(kv4.isPresent());
      //Option<KeyValue> kv5 = reader.seekTo(new UTF8StringKey("key-00000684"));
      //assertEquals("val-00000684", getValue(kv5.get()));
      //Option<KeyValue> kv6 = reader.seekTo(new UTF8StringKey("key-00000690"));
      //assertEquals("val-00000690", getValue(kv6.get()));
    }
  }

  @Test
  public void testReadHFileWithoutKeyValueEntries() throws IOException {
    int bufSize = 16 * 1024 * 1024;
    Path path = new Path(
        "file:/Users/ethan/Work/tmp/20231215-hfile-ci-logs/record-index-0006-0_3-108-262_20231219054341404.hfile");
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (FSDataInputStream stream = fs.open(path, bufSize)) {
      HFileReaderImpl reader = new HFileReaderImpl(stream, fileStatus.getLen());
      reader.initializeMetadata();
      assertFalse(reader.seekTo());
      assertEquals(2, reader.seekTo(new UTF8StringKey("xyz")));
      assertFalse(reader.next());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/hfile/hudi_0_9_hbase_1_2_3", "/hfile/hudi_0_10_hbase_1_2_3", "/hfile/hudi_0_11_hbase_2_4_9"})
  public void testReadHFileCompatibility(String hfilePrefix) throws IOException {
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadPrimitiveRecord()
    // using different Hudi releases
    String simpleHFile = hfilePrefix + SIMPLE_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadComplexRecord()
    // using different Hudi releases
    String complexHFile = hfilePrefix + COMPLEX_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestBootstrapIndex#testBootstrapIndex()
    // using different Hudi releases.  The file is copied from .hoodie/.aux/.bootstrap/.partitions/
    String bootstrapIndexFile = hfilePrefix + BOOTSTRAP_INDEX_HFILE_SUFFIX;

    Option<Function<Integer, String>> keyCreator = Option.of(i -> "key" + String.format("%02d", i));
    verifyHFileReadCompatibility(simpleHFile, 50, keyCreator);
    verifyHFileReadCompatibility(complexHFile, 50, keyCreator);
    verifyHFileReadCompatibility(bootstrapIndexFile, 4, Option.empty());
  }

  public static byte[] readHFileFromResources(String filename) throws IOException {
    long size = TestHFileReader.class
        .getResource(filename).openConnection().getContentLength();
    return readAsByteArray(
        TestHFileReader.class.getResourceAsStream(filename), (int) size);
  }

  public static HFileReader getHFileReader(String filename) throws IOException {
    byte[] content = readHFileFromResources(filename);
    return new HFileReaderImpl(
        new FSDataInputStream(new SeekableByteArrayInputStream(content)), content.length);
  }

  private static void verifyHFileReadCompatibility(String filename,
                                                   int numEntries,
                                                   Option<Function<Integer, String>> keyCreator) throws IOException {
    try (HFileReader reader = getHFileReader(filename)) {
      reader.initializeMetadata();
      verifyHFileMetadata(reader, numEntries);
      verifyHFileValuesInSequentialReads(reader, numEntries, keyCreator);
    }
  }

  private static void verifyHFileMetadata(HFileReader reader, int numEntries) {
    assertEquals(numEntries, reader.getNumKeyValueEntries());
  }

  private static void verifyHFileValuesInSequentialReads(HFileReader reader,
                                                         int numEntries,
                                                         Option<Function<Integer, String>> keyCreator)
      throws IOException {
    verifyHFileValuesInSequentialReads(reader, numEntries, keyCreator, Option.empty());
  }

  private static void verifyHFileValuesInSequentialReads(HFileReader reader,
                                                         int numEntries,
                                                         Option<Function<Integer, String>> keyCreator,
                                                         Option<Function<Integer, String>> valueCreator)
      throws IOException {
    assertFalse(reader.isSeeked());
    assertFalse(reader.next());
    boolean result = reader.seekTo();
    assertEquals(numEntries > 0, result);
    for (int i = 0; i < numEntries; i++) {
      Option<KeyValue> keyValue = reader.getKeyValue();
      assertTrue(keyValue.isPresent());
      if (valueCreator.isPresent()) {
        assertEquals(new UTF8StringKey(keyCreator.get().apply(i)), keyValue.get().getKey());
      }
      if (valueCreator.isPresent()) {
        assertEquals(valueCreator.get().apply(i), getValue(keyValue.get()));
      }
      if (i < numEntries - 1) {
        assertTrue(reader.next());
      } else {
        assertFalse(reader.next());
      }
    }
  }

  private static String getValue(KeyValue kv) {
    return new String(kv.getBytes(), kv.getValueOffset(), kv.getValueLength());
  }

  static class SeekableByteArrayInputStream extends ByteBufferBackedInputStream implements Seekable,
      PositionedReadable {
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() throws IOException {
      return getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return copyFrom(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }
  }
}
