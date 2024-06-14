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

package org.apache.hudi.io.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Function;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.io.hfile.TestHFileReader.CUSTOM_META_KEY;
import static org.apache.hudi.io.hfile.TestHFileReader.CUSTOM_META_VALUE;
import static org.apache.hudi.io.hfile.TestHFileReader.DUMMY_BLOOM_FILTER;

/**
 * Utils for reader and writer tests.
 */
public class TestHoodieReaderWriterUtils {
  public static void writeHFileForTesting(String fileLocation,
                                   int blockSize,
                                   Compression.Algorithm compressionAlgo,
                                   int numEntries,
                                   Function<Integer, String> keyCreator,
                                   Function<Integer, String> valueCreator,
                                   boolean uniqueKeys) throws IOException {
    HFileContext context = new HFileContextBuilder()
        .withBlockSize(blockSize)
        .withCompression(compressionAlgo)
        .build();
    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    Path filePath = new Path(fileLocation);
    FileSystem fs = filePath.getFileSystem(conf);
    try (HFile.Writer writer = HFile.getWriterFactory(conf, cacheConfig)
        .withPath(fs, filePath)
        .withFileContext(context)
        .create()) {
      for (int i = 0; i < numEntries; i++) {
        byte[] keyBytes = getUTF8Bytes(keyCreator.apply(i));
        writer.append(new KeyValue(keyBytes, null, null, getUTF8Bytes(valueCreator.apply(i))));
        if (!uniqueKeys) {
          for (int j = 0; j < 20; j++) {
            writer.append(new KeyValue(
                keyBytes, null, null, getUTF8Bytes(valueCreator.apply(i) + "_" + j)));
          }
        }
      }
      writer.appendFileInfo(getUTF8Bytes(CUSTOM_META_KEY), getUTF8Bytes(CUSTOM_META_VALUE));
      writer.appendMetaBlock(HoodieNativeAvroHFileReader.KEY_BLOOM_FILTER_META_BLOCK, new Writable() {
        @Override
        public void write(DataOutput out) throws IOException {
          out.write(getUTF8Bytes(DUMMY_BLOOM_FILTER));
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        }
      });
    }
  }
}
