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
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestHFileReader {
  @Test
  public void testReadHFile() throws IOException {
    int bufSize = 16 * 1024 * 1024;
    Path path = new Path("file:/Users/ethan/Work/tmp/hfile-reader/hfile_keyed.hfile");
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (FSDataInputStream stream = fs.open(path, bufSize)) {
      HFileReader reader = new HFileReader(stream, fileStatus.getLen());
      reader.initializeMetadata();
      KeyValue kv0 = reader.seekTo(new StringKeyOnlyKeyValue("abc-00000000"));
      assertNull(kv0);
      KeyValue kv1 = reader.seekTo(new StringKeyOnlyKeyValue("key-00000000"));
      assertEquals("val-00000000", getValue(kv1));
      KeyValue kv2 = reader.seekTo(new StringKeyOnlyKeyValue("key-00000050"));
      assertEquals("val-00000050", getValue(kv2));
      KeyValue kv3 = reader.seekTo(new StringKeyOnlyKeyValue("key-00000536"));
      assertEquals("val-00000536", getValue(kv3));
      KeyValue kv4 = reader.seekTo(new StringKeyOnlyKeyValue("key-10000000"));
      assertNull(kv4);
      KeyValue kv5 = reader.seekTo(new StringKeyOnlyKeyValue("key-00000684"));
      assertEquals("val-00000684", getValue(kv5));
      KeyValue kv6 = reader.seekTo(new StringKeyOnlyKeyValue("key-00000690"));
      assertEquals("val-00000690", getValue(kv6));
    }
  }

  // "/hudi_0_9_hbase_1_2_3_simple.hfile", "/hudi_0_10_hbase_1_2_3_simple.hfile",
  @ParameterizedTest
  @ValueSource(strings = {
      "/hudi_0_9_hbase_1_2_3_simple.hfile", "/hudi_0_10_hbase_1_2_3_simple.hfile", "/hudi_0_11_hbase_2_4_9_simple.hfile"})
  public void testReadExistingHFile(String filename) throws IOException {
    int bufSize = 16 * 1024 * 1024;
    Path path = new Path("file:/Users/ethan/Work/repo/hudi/hudi-common/src/test/resources/" + filename);
    Configuration conf = new Configuration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(path);
    try (FSDataInputStream stream = fs.open(path, bufSize)) {
      HFileReader reader = new HFileReader(stream, fileStatus.getLen());
      reader.initializeMetadata();
      KeyValue kv0 = reader.seekTo(new StringKeyOnlyKeyValue("key00"));
      String v0 = getValue(kv0);
      KeyValue kv1 = reader.seekTo(new StringKeyOnlyKeyValue("key12"));
      assertNull(kv1);
    }
  }

  private static String getValue(KeyValue kv) {
    return new String(kv.getBytes(), kv.getValueOffset(), kv.getValueLength());
  }
}
