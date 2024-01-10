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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHFileReader {
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

  // "/hudi_0_9_hbase_1_2_3_simple.hfile", "/hudi_0_10_hbase_1_2_3_simple.hfile",
  @ParameterizedTest
  @ValueSource(strings = {
      "/hudi_0_9_hbase_1_2_3_simple.hfile", "/hudi_0_10_hbase_1_2_3_simple.hfile",
      "/hudi_0_11_hbase_2_4_9_simple.hfile"})
  public void testReadExistingHFile(String filename) throws IOException {
    int bufSize = 16 * 1024 * 1024;
    Path path =
        new Path("file:/Users/ethan/Work/repo/hudi/hudi-common/src/test/resources/" + filename);
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (FSDataInputStream stream = fs.open(path, bufSize)) {
      HFileReaderImpl reader = new HFileReaderImpl(stream, fileStatus.getLen());
      reader.initializeMetadata();
      //Option<KeyValue> kv0 = reader.seekTo(new UTF8StringKey("key00"));
      //String v0 = getValue(kv0.get());
      //Option<KeyValue> kv1 = reader.seekTo(new UTF8StringKey("key06"));
      //String v1 = getValue(kv1.get());
      //Option<KeyValue> kv2 = reader.seekTo(new UTF8StringKey("key12"));
      //assertFalse(kv2.isPresent());
    }
  }

  private static String getValue(KeyValue kv) {
    return new String(kv.getBytes(), kv.getValueOffset(), kv.getValueLength());
  }
}
