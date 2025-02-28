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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
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

  /**
   * Create the specified file on the filesystem. By default, this will:
   * <ol>
   * <li>apply the umask in the configuration (if it is enabled)</li>
   * <li>use the fs configured buffer size (or 4096 if not set)</li>
   * <li>use the default replication</li>
   * <li>use the default block size</li>
   * <li>not track progress</li>
   * </ol>
   * @param fs        {@link FileSystem} on which to write the file
   * @param path      {@link Path} to the file to write
   * @param perm      intial permissions
   * @param overwrite Whether or not the created file should be overwritten.
   * @return output stream to the created file
   * @throws IOException if the file cannot be created
   */
  public static FSDataOutputStream create(FileSystem fs, Path path, FsPermission perm,
                                          boolean overwrite) throws IOException {
    return fs.create(path, perm, overwrite, getDefaultBufferSize(fs),
        getDefaultReplication(fs, path), getDefaultBlockSize(fs, path), null);
  }

  /**
   * Returns the default buffer size to use during writes. The size of the buffer should probably be
   * a multiple of hardware page size (4096 on Intel x86), and it determines how much data is
   * buffered during read and write operations.
   * @param fs filesystem object
   * @return default buffer size to use during writes
   */
  public static int getDefaultBufferSize(final FileSystem fs) {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  /**
   * Get the default replication.
   * @param fs filesystem object
   * @param path path of file
   * @return default replication for the path's filesystem
   */
  public static short getDefaultReplication(final FileSystem fs, final Path path) {
    return fs.getDefaultReplication(path);
  }

  /**
   * Return the number of bytes that large input files should be optimally be split into to minimize
   * i/o time.
   * @param fs filesystem object
   * @return the default block size for the path's filesystem
   */
  public static long getDefaultBlockSize(final FileSystem fs, final Path path) {
    return fs.getDefaultBlockSize(path);
  }
}
