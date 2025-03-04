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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
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
      FileSystem fs, Path path, CacheConfig cacheConfig, Configuration configuration) {
    try {
      return HFile.createReader(fs, path, cacheConfig, USE_PRIMARY_REPLICA_READER, configuration);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize HFile reader for  " + path, e);
    }
  }

  /**
   * Creates HFile reader for a file with default `primaryReplicaReader` as true.
   *
   * @param storage       {@link HoodieStorage} instance.
   * @param path          path of file to read.
   * @param cacheConfig   Cache configuration.
   * @param configuration Configuration
   * @return HFile reader
   * @throws IOException Upon error.
   */
  public static HFile.Reader createHFileReader(
      HoodieStorage storage, StoragePath path, CacheConfig cacheConfig, Configuration configuration) {
    try {
      return HFile.createReader((FileSystem) storage.getFileSystem(),
          new Path(path.toUri()), cacheConfig, USE_PRIMARY_REPLICA_READER, configuration);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize HFile reader for  " + path, e);
    }
  }

  /**
   * Creates HFile reader for byte array with default `primaryReplicaReader` as true.
   *
   * @param storage       {@link HoodieStorage} instance.
   * @param dummyPath Dummy path to file to read.
   * @param content       Content in byte array.
   * @return HFile reader
   * @throws IOException Upon error.
   */
  public static HFile.Reader createHFileReader(
      HoodieStorage storage, StoragePath dummyPath, byte[] content) {
    // Avoid loading default configs, from the FS, since this configuration is mostly
    // used as a stub to initialize HFile reader
    Configuration conf = new Configuration(false);
    SeekableByteArrayInputStream bis = new SeekableByteArrayInputStream(content);
    FSDataInputStream fsdis = new FSDataInputStream(bis);
    FSDataInputStreamWrapper stream = new FSDataInputStreamWrapper(fsdis);
    ReaderContext context = new ReaderContextBuilder()
        .withFilePath(new Path(dummyPath.toUri()))
        .withInputStreamWrapper(stream)
        .withFileSize(content.length)
        .withFileSystem((FileSystem) storage.getFileSystem())
        .withPrimaryReplicaReader(USE_PRIMARY_REPLICA_READER)
        .withReaderType(ReaderContext.ReaderType.STREAM)
        .build();
    try {
      HFileInfo fileInfo = new HFileInfo(context, conf);
      HFile.Reader reader = HFile.createReader(context, fileInfo, new CacheConfig(conf), conf);
      fileInfo.initMetaAndIndex(reader);
      return reader;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize HFile reader for  " + dummyPath, e);
    }
  }

  static class SeekableByteArrayInputStream extends ByteBufferBackedInputStream
      implements Seekable, PositionedReadable {
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
