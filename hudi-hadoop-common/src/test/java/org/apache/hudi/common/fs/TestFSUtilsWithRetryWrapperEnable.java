/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version loop.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-loop.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.fs;

import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieRetryWrapperFileSystem;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests file system utils with retry wrapper enable.
 * P.S extends TestFSUtils and setUp a HoodieWrapperFileSystem for metaClient which can test all the TestFSUtils uts with RetryWrapperEnable
 */
public class TestFSUtilsWithRetryWrapperEnable extends TestFSUtils {

  private static final String EXCEPTION_MESSAGE = "Fake runtime exception here.";
  private long maxRetryIntervalMs;
  private int maxRetryNumbers;
  private long initialRetryIntervalMs;

  @Override
  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
    FileSystemRetryConfig fileSystemRetryConfig =
        FileSystemRetryConfig.newBuilder().withFileSystemActionRetryEnabled(true).build();
    maxRetryIntervalMs = fileSystemRetryConfig.getMaxRetryIntervalMs();
    maxRetryNumbers = fileSystemRetryConfig.getMaxRetryNumbers();
    initialRetryIntervalMs = fileSystemRetryConfig.getInitialRetryIntervalMs();

    FakeRemoteFileSystem fakeFs = new FakeRemoteFileSystem(
        HadoopFSUtils.getFs(metaClient.getMetaPath().toString(), metaClient.getStorageConf()), 2);
    FileSystem fileSystem =
        new HoodieRetryWrapperFileSystem(fakeFs, maxRetryIntervalMs, maxRetryNumbers,
            initialRetryIntervalMs, "");

    HoodieWrapperFileSystem fs =
        new HoodieWrapperFileSystem(fileSystem, new NoOpConsistencyGuard());
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        HadoopFSUtils.convertToStoragePath(new Path(metaClient.getBasePath().toString())),
        HadoopFSUtils.getStorageConf(fs.getConf()));
    metaClient.setStorage(storage);
  }

  // Test the scenario that fs keeps retrying until it fails.
  @Test
  public void testProcessFilesWithExceptions() throws Exception {
    FakeRemoteFileSystem fakeFs = new FakeRemoteFileSystem(
        HadoopFSUtils.getFs(metaClient.getMetaPath().toString(), metaClient.getStorageConf()), 100);
    FileSystem fileSystem =
        new HoodieRetryWrapperFileSystem(fakeFs, maxRetryIntervalMs, maxRetryNumbers,
            initialRetryIntervalMs, "");
    HoodieWrapperFileSystem fs =
        new HoodieWrapperFileSystem(fileSystem, new NoOpConsistencyGuard());
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        HadoopFSUtils.convertToStoragePath(new Path(metaClient.getMetaPath().toString())),
        HadoopFSUtils.getStorageConf(fs.getConf()));
    metaClient.setStorage(storage);
    List<String> folders =
        Arrays.asList("2016/04/15", ".hoodie/.temp/2/2016/04/15");
    folders.forEach(f -> assertThrows(RuntimeException.class, () -> metaClient.getStorage()
        .createDirectory(new StoragePath(new StoragePath(basePath), f))));
  }

  @Test
  public void testGetSchema() {
    FakeRemoteFileSystem fakeFs = new FakeRemoteFileSystem(
        HadoopFSUtils.getFs(metaClient.getMetaPath().toString(), metaClient.getStorageConf()), 100);
    FileSystem fileSystem =
        new HoodieRetryWrapperFileSystem(fakeFs, maxRetryIntervalMs, maxRetryNumbers,
            initialRetryIntervalMs, "");
    HoodieWrapperFileSystem fs =
        new HoodieWrapperFileSystem(fileSystem, new NoOpConsistencyGuard());
    assertDoesNotThrow(fs::getScheme, "Method #getSchema does not implement correctly");
  }

  @Test
  public void testGetDefaultReplication() {
    FakeRemoteFileSystem fakeFs = new FakeRemoteFileSystem(
        HadoopFSUtils.getFs(metaClient.getMetaPath().toString(), metaClient.getStorageConf()), 100);
    FileSystem fileSystem =
        new HoodieRetryWrapperFileSystem(fakeFs, maxRetryIntervalMs, maxRetryNumbers,
            initialRetryIntervalMs, "");
    HoodieWrapperFileSystem fs =
        new HoodieWrapperFileSystem(fileSystem, new NoOpConsistencyGuard());
    assertEquals(fs.getDefaultReplication(), 3);
    assertEquals(fs.getDefaultReplication(new Path(basePath)), 3);
  }

  /**
   * Fake remote FileSystem which will throw RuntimeException something like AmazonS3Exception 503.
   */
  class FakeRemoteFileSystem extends FileSystem {

    private FileSystem fs;
    private int count = 1;
    private int loop;
    private short defaultReplication = 3;

    public FakeRemoteFileSystem(FileSystem fs, int retryLoop) {
      this.fs = fs;
      this.loop = retryLoop;
    }
    
    @Override
    public URI getUri() {
      return fs.getUri();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.open(f, bufferSize);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.append(f, bufferSize, progress);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.rename(src, dst);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.delete(f, recursive);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
      if (count % loop == 0) {
        count++;
        return fs.listStatus(f);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
      fs.setWorkingDirectory(newDir);
    }

    @Override
    public Path getWorkingDirectory() {
      return fs.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.mkdirs(f, permission);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      if (count % loop == 0) {
        count++;
        return fs.getFileStatus(f);
      } else {
        count++;
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
      return fs.listLocatedStatus(f);
    }

    @Override
    public Configuration getConf() {
      return fs.getConf();
    }

    @Override
    public String getScheme() {
      return fs.getScheme();
    }

    @Override
    public short getDefaultReplication() {
      return  defaultReplication;
    }

    @Override
    public short getDefaultReplication(Path path) {
      return defaultReplication;
    }

  }
}
