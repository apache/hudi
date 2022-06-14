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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;

import java.io.IOException;

/**
 * Util class for HFile reading and writing in Hudi
 */
public class HoodieHFileUtils {
  // Based on HBase 2.4.9, the primaryReplicaReader is mainly used for constructing
  // block cache key, so if we do not use block cache then it is OK to set it as any
  // value. We use true here.
  private static final boolean USE_PRIMARY_REPLICA_READER = true;

  /**
   * Creates HFile reader for a file with default `primaryReplicaReader` as true.
   *
   * @param fs            File system.
   * @param path          Path to file to read.
   * @param cacheConfig   Cache configuration.
   * @param configuration Configuration
   * @return HFile reader
   * @throws IOException Upon error.
   */
  public static HFile.Reader createHFileReader(
      FileSystem fs, Path path, CacheConfig cacheConfig, Configuration configuration) throws IOException {
    return HFile.createReader(fs, path, cacheConfig, USE_PRIMARY_REPLICA_READER, configuration);
  }

  /**
   * Creates HFile reader for byte array with default `primaryReplicaReader` as true.
   *
   * @param fs        File system.
   * @param dummyPath Dummy path to file to read.
   * @param content   Content in byte array.
   * @return HFile reader
   * @throws IOException Upon error.
   */
  public static HFile.Reader createHFileReader(
      FileSystem fs, Path dummyPath, byte[] content) throws IOException {
    Configuration conf = new Configuration();
    HoodieAvroHFileReader.SeekableByteArrayInputStream bis = new HoodieAvroHFileReader.SeekableByteArrayInputStream(content);
    FSDataInputStream fsdis = new FSDataInputStream(bis);
    FSDataInputStreamWrapper stream = new FSDataInputStreamWrapper(fsdis);
    ReaderContext context = new ReaderContextBuilder()
        .withFilePath(dummyPath)
        .withInputStreamWrapper(stream)
        .withFileSize(content.length)
        .withFileSystem(fs)
        .withPrimaryReplicaReader(USE_PRIMARY_REPLICA_READER)
        .withReaderType(ReaderContext.ReaderType.STREAM)
        .build();
    HFileInfo fileInfo = new HFileInfo(context, conf);
    HFile.Reader reader = HFile.createReader(context, fileInfo, new CacheConfig(conf), conf);
    fileInfo.initMetaAndIndex(reader);
    return reader;
  }
}
