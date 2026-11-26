/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.vortex;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.exception.HoodieException;

import dev.vortex.api.DataSource;
import dev.vortex.api.ImmutableScanOptions;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.Session;
import dev.vortex.api.VortexColumnStatistics;
import dev.vortex.io.NativeReadable;
import dev.vortex.jni.NativeFiles;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A Vortex file that is open for reading.
 *
 * <p>The file is read through the Hadoop {@code FileSystem} of its path. Thus Vortex reads every
 * storage that Hudi can use, with the credentials and settings in the Hadoop configuration. When
 * the caller gives the file length, no metadata request is necessary to open the file.
 *
 * <p>All files share one Vortex {@link Session}, which is safe for concurrent use.
 *
 * <p>Close this object after all scans and Arrow readers of the file are closed. The native code
 * reads through the readable until then.
 */
public final class VortexInputFile implements AutoCloseable {

  static {
    // Set the Arrow properties before this class uses Arrow buffers and vectors.
    VortexArrowProperties.ensureConfigured();
  }

  private static final long UNKNOWN_LENGTH = -1;

  private static final String EXACT_ROW_COUNT_CLASS = "Exact";

  private static volatile Session sharedSession;

  private final String path;
  private final Configuration conf;
  private final NativeReadable readable;
  private final DataSource dataSource;
  private long length;

  private VortexInputFile(String path, Configuration conf, NativeReadable readable, DataSource dataSource,
                          long length) {
    this.path = path;
    this.conf = conf;
    this.readable = readable;
    this.dataSource = dataSource;
    this.length = length;
  }

  /**
   * Opens a Vortex file whose length is not known.
   */
  public static VortexInputFile open(Configuration conf, String path) {
    return open(conf, path, UNKNOWN_LENGTH);
  }

  /**
   * Opens a Vortex file.
   *
   * @param conf   Hadoop configuration for the file system of the path
   * @param path   path of the file
   * @param length length of the file in bytes, or a negative value if it is not known
   */
  public static VortexInputFile open(Configuration conf, String path, long length) {
    // With workers, Vortex decodes the next batches while the Java thread reads a batch.
    VortexWorkerThreads.ensureAtLeast(conf.getInt(HoodieStorageConfig.VORTEX_WORKER_THREADS.key(),
        HoodieStorageConfig.VORTEX_WORKER_THREADS.defaultValue()));

    NativeReadable readable = HoodieVortexReadable.open(conf, path, length);
    try {
      int ioConcurrency = conf.getInt(HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY.key(),
          HoodieStorageConfig.VORTEX_READ_IO_CONCURRENCY.defaultValue());
      DataSource dataSource = DataSource.open(session(), Collections.singletonList(readable), ioConcurrency);
      return new VortexInputFile(path, conf, readable, dataSource, readable.length());
    } catch (RuntimeException | Error e) {
      closeQuietly(readable, e);
      throw e;
    }
  }

  /**
   * Returns the Vortex session that all reads share.
   */
  public static Session session() {
    Session session = sharedSession;
    if (session != null) {
      return session;
    }

    synchronized (VortexInputFile.class) {
      if (sharedSession == null) {
        sharedSession = Session.create();
      }
      return sharedSession;
    }
  }

  public String path() {
    return path;
  }

  /**
   * Returns the length of the file in bytes.
   */
  public long length() {
    if (length < 0) {
      Path file = new Path(path);
      try {
        length = file.getFileSystem(conf).getFileStatus(file).getLen();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to get the length of Vortex file " + path, e);
      }
    }
    return length;
  }

  public Schema arrowSchema(BufferAllocator allocator) {
    return dataSource.arrowSchema(allocator);
  }

  /**
   * Returns the number of rows in the file, if the footer gives an exact count.
   */
  public OptionalLong exactRowCount() {
    // RowCount.Exact is a record, which this module cannot name: it compiles for Java 8.
    DataSource.RowCount rowCount = dataSource.rowCount();
    if (rowCount.getClass().getSimpleName().equals(EXACT_ROW_COUNT_CLASS)) {
      return rowCount.asOptional();
    }
    return OptionalLong.empty();
  }

  public Scan scan(ScanOptions options) {
    int splitConcurrency = conf.getInt(HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY.key(),
        HoodieStorageConfig.VORTEX_READ_SPLIT_CONCURRENCY.defaultValue());
    if (splitConcurrency > 0 && !options.splitConcurrency().isPresent()) {
      return dataSource.scan(ImmutableScanOptions.copyOf(options).withSplitConcurrency(splitConcurrency));
    }
    return dataSource.scan(options);
  }

  /**
   * Returns the metadata segments of the file footer.
   */
  public Map<String, byte[]> readMetadata() {
    return NativeFiles.readMetadata(session(), readable);
  }

  /**
   * Returns the statistics of each top-level column, from the file footer.
   */
  public List<VortexColumnStatistics> readStatistics() {
    return NativeFiles.readStatistics(session(), readable);
  }

  /**
   * Closes the data source, then the readable.
   */
  @Override
  public void close() {
    try {
      dataSource.close();
    } finally {
      try {
        readable.close();
      } catch (IOException e) {
        throw new HoodieException("Failed to close Vortex file: " + path, e);
      }
    }
  }

  private static void closeQuietly(NativeReadable readable, Throwable failure) {
    try {
      readable.close();
    } catch (IOException | RuntimeException e) {
      failure.addSuppressed(e);
    }
  }
}
