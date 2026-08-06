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

package org.apache.hudi.hadoop.fs;

import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopFileStatus;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePathInfo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HadoopFSUtils}
 */
public class TestHadoopFSUtils {
  /**
   * HUDI-4602: {@link FileSystem#getScheme()} is optional in Hadoop -- the base implementation throws
   * {@link UnsupportedOperationException} -- and proxy implementations such as Presto's
   * {@code PrestoS3FileSystem} do not override it. Opening a log file went straight through
   * {@code isGCSFileSystem}, so a MOR {@code _rt} query on Presto failed with
   * "Not implemented by the PrestoS3FileSystem FileSystem implementation" rather than reading anything.
   *
   * <p>{@link FilterFileSystem} has the same shape: it leaves {@code getScheme()} to the throwing base
   * implementation while overriding {@code getUri()}.
   */
  @Test
  public void testGetFSDataInputStreamWhenGetSchemeIsUnimplemented(@TempDir java.nio.file.Path tempDir) throws IOException {
    java.nio.file.Path file = tempDir.resolve("log.file");
    byte[] contents = new byte[] {1, 2, 3, 4};
    Files.write(file, contents);
    // newInstanceLocal rather than getLocal, so closing this does not evict a cached FileSystem that
    // other tests in the same JVM share.
    try (FileSystem fs = newFsWithoutGetScheme(FileSystem.newInstanceLocal(new Configuration()))) {
      try (FSDataInputStream stream =
               HadoopFSUtils.getFSDataInputStream(fs, new StoragePath(file.toUri()), 1024, true)) {
        byte[] read = new byte[contents.length];
        stream.readFully(read);
        assertArrayEquals(contents, read, "The read path should not depend on the optional getScheme()");
      }
    }
  }

  @Test
  public void testGetSchemeFallsBackToTheUriWhenUnimplemented() throws IOException {
    try (FileSystem localFs = FileSystem.newInstanceLocal(new Configuration())) {
      assertEquals("file", HadoopFSUtils.getScheme(localFs),
          "LocalFileSystem overrides getScheme(), so the helper should return what it reports "
              + "rather than falling back to getUri()");

      // FilterFileSystem#close closes the delegate, so the wrapper is not given its own block: it owns
      // nothing, and closing it here would close localFs a second time.
      FileSystem noScheme = newFsWithoutGetScheme(localFs);
      assertEquals("file", HadoopFSUtils.getScheme(noScheme),
          "FilterFileSystem does not override getScheme(), so the helper should fall back to "
              + "getUri().getScheme()");
    }
  }

  /**
   * A URI with no scheme cannot stand in for an unimplemented {@code getScheme()}. {@code InLineFileSystem}
   * is the case in this module: {@code getScheme()} returns "inlinefs" while {@code getUri()} is
   * {@code URI.create("inlinefs")}, which has no colon and so no scheme. Returning null there would surface
   * far away as "does not support scheme null" with the original failure discarded, so it must fail here.
   */
  @Test
  public void testGetSchemeFailsLoudlyWhenNeitherSourceHasOne() throws IOException {
    try (FileSystem localFs = FileSystem.newInstanceLocal(new Configuration())) {
      FileSystem schemeless = new NoSchemeFileSystem(localFs, URI.create("inlinefs"));

      HoodieException thrown =
          assertThrows(HoodieException.class, () -> HadoopFSUtils.getScheme(schemeless));
      assertTrue(thrown.getMessage().contains("carries no scheme"),
          () -> "the failure should say the URI carries no scheme, but was: " + thrown.getMessage());
      assertInstanceOf(UnsupportedOperationException.class, thrown.getCause(),
          "the original getScheme() failure must be chained rather than discarded");
    }
  }

  /**
   * The three call sites this rerouted that no test in the repo reached: {@code registerFileSystem},
   * {@code HoodieWrapperFileSystem#convertToHoodiePath} - which is on the write path, via
   * {@code HoodieBaseParquetWriter} and friends - and {@code HoodieHadoopStorage#getScheme}. All three threw
   * {@link UnsupportedOperationException} on a filesystem without {@code getScheme()} before this change.
   */
  @Test
  public void testCallSitesWorkOnAFileSystemWithoutGetScheme(@TempDir java.nio.file.Path tempDir) {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", NoSchemeLocalFileSystem.class, FileSystem.class);
    StoragePath path = new StoragePath(tempDir.toUri());

    assertDoesNotThrow(() -> HadoopFSUtils.registerFileSystem(path, conf),
        "registerFileSystem resolves the scheme to build the fs.<scheme>.impl key");
    assertDoesNotThrow(() -> HoodieWrapperFileSystem.convertToHoodiePath(path, conf),
        "convertToHoodiePath is on the write path and resolves the scheme to rewrite it");
    assertEquals("file", new HoodieHadoopStorage(path, HadoopFSUtils.getStorageConf(conf)).getScheme(),
        "HoodieHadoopStorage#getScheme is what HoodieStorage callers reach");
  }

  /**
   * {@code isGCSFileSystem} and {@code isCHDFileSystem} become reachable for a filesystem without
   * {@code getScheme()} for the first time with this change, and they select different stream wrappers.
   * Neither predicate had a test before.
   */
  @ParameterizedTest
  @CsvSource({
      "gs://bucket, org.apache.hudi.hadoop.fs.SchemeAwareFSDataInputStream",
      "ofs://cluster, org.apache.hudi.hadoop.fs.BoundedFsDataInputStream"
  })
  public void testSchemeSpecificStreamIsSelectedWithoutGetScheme(String uri, String expectedStream,
                                                                 @TempDir java.nio.file.Path tempDir) throws IOException {
    java.nio.file.Path file = tempDir.resolve("log.file");
    Files.write(file, new byte[] {1, 2, 3, 4});
    try (FileSystem localFs = FileSystem.newInstanceLocal(new Configuration())) {
      // Reports a gs:// or ofs:// URI while leaving getScheme() to the throwing base implementation.
      FileSystem fs = new NoSchemeFileSystem(localFs, URI.create(uri));
      assertThrows(UnsupportedOperationException.class, fs::getScheme);

      try (FSDataInputStream stream =
               HadoopFSUtils.getFSDataInputStream(fs, new StoragePath(file.toUri()), 1024, true)) {
        assertEquals(expectedStream, stream.getClass().getName(),
            "the scheme-specific wrapper should be selected from the fallback-resolved scheme");
      }
    }
  }

  /** A FileSystem with the reported shape: {@code getUri()} works, {@code getScheme()} throws. */
  private static FileSystem newFsWithoutGetScheme(FileSystem delegate) {
    FileSystem fs = new FilterFileSystem(delegate);
    // The premise of every assertion below: this is the call the read path used to make unguarded.
    assertThrows(UnsupportedOperationException.class, fs::getScheme);
    return fs;
  }

  /** Same shape, but reporting a URI of our choosing so scheme-specific branches can be reached. */
  private static class NoSchemeFileSystem extends FilterFileSystem {
    private final URI uri;

    NoSchemeFileSystem(FileSystem delegate, URI uri) {
      super(delegate);
      this.uri = uri;
    }

    @Override
    public URI getUri() {
      return uri;
    }
  }

  /**
   * A {@link LocalFileSystem} that does not implement {@code getScheme()}, so it can be registered as
   * {@code fs.file.impl} and reached through the normal {@code FileSystem.get} path.
   */
  public static class NoSchemeLocalFileSystem extends LocalFileSystem {
    @Override
    public String getScheme() {
      throw new UnsupportedOperationException(
          "Not implemented by the NoSchemeLocalFileSystem FileSystem implementation");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/a/b/c",
      "s3://bucket/partition=1%2F2%2F3",
      "hdfs://x/y/z.file#bar"
  })
  public void testPathConversion(String pathString) {
    // Hadoop Path -> StoragePath -> Hadoop Path
    Path path = new Path(pathString);
    StoragePath storagePath = convertToStoragePath(path);
    Path convertedPath = convertToHadoopPath(storagePath);
    assertEquals(path.toUri(), storagePath.toUri());
    assertEquals(path, convertedPath);

    // StoragePath -> Hadoop Path -> StoragePath
    storagePath = new StoragePath(pathString);
    path = convertToHadoopPath(storagePath);
    StoragePath convertedStoragePath = convertToStoragePath(path);
    assertEquals(storagePath.toUri(), path.toUri());
    assertEquals(storagePath, convertedStoragePath);
  }

  @ParameterizedTest
  @CsvSource({
      "/a/b/c,1000,false,1,1000000,1238493920",
      "/x/y/z,0,true,2,0,2002403203"
  })
  public void testFileStatusConversion(String path,
                                       long length,
                                       boolean isDirectory,
                                       short blockReplication,
                                       long blockSize,
                                       long modificationTime) {
    // FileStatus -> StoragePathInfo -> FileStatus
    FileStatus fileStatus = new FileStatus(
        length, isDirectory, blockReplication, blockSize, modificationTime, new Path(path));
    StoragePathInfo pathInfo = convertToStoragePathInfo(fileStatus);
    assertStoragePathInfo(
        pathInfo, path, length, isDirectory, blockReplication, blockSize, modificationTime);
    FileStatus convertedFileStatus = convertToHadoopFileStatus(pathInfo);
    assertFileStatus(
        convertedFileStatus, path, length, isDirectory, blockReplication, blockSize, modificationTime);

    // StoragePathInfo -> FileStatus -> StoragePathInfo
    pathInfo = new StoragePathInfo(
        new StoragePath(path), length, isDirectory, blockReplication, blockSize, modificationTime);
    fileStatus = convertToHadoopFileStatus(pathInfo);
    assertFileStatus(
        fileStatus, path, length, isDirectory, blockReplication, blockSize, modificationTime);
    StoragePathInfo convertedPathInfo = convertToStoragePathInfo(fileStatus);
    assertStoragePathInfo(
        convertedPathInfo, path, length, isDirectory, blockReplication, blockSize, modificationTime);
  }

  @Test
  public void testStorageConfigurationCopyAndFileSystemOverloads(@TempDir java.nio.file.Path tempDir) {
    Configuration conf = new Configuration(false);
    conf.set("test.key", "before");
    StorageConfiguration<Configuration> shared = HadoopFSUtils.getStorageConf(conf);
    StorageConfiguration<Configuration> copied = HadoopFSUtils.getStorageConfWithCopy(conf);
    conf.set("test.key", "after");

    assertEquals("after", shared.getString("test.key").get());
    assertEquals("before", copied.getString("test.key").get());
    assertEquals("file", HadoopFSUtils.getFs(tempDir.toUri().toString(), shared).getScheme());
    assertEquals("file", HadoopFSUtils.getFs(tempDir.toUri().toString(), shared, true).getScheme());
    assertEquals("file", HadoopFSUtils.getFs(new Path(tempDir.toUri()), shared).getScheme());
    assertEquals("file", HadoopFSUtils.getFs(new Path(tempDir.toUri()), shared, true).getScheme());
    assertEquals("file", HadoopFSUtils.getFs(new StoragePath(tempDir.toUri()), conf).getScheme());
    assertEquals("file", HadoopFSUtils.getFs(tempDir.toString(), conf, true).getScheme());
    assertInstanceOf(StorageConfiguration.class, HadoopFSUtils.getStorageConf());
  }

  @Test
  public void testAvroPathPermissionAndStatusConversions() {
    Path path = new Path("s3://bucket/table/file.parquet");
    HoodiePath hoodiePath = HadoopFSUtils.fromPath(path);
    assertEquals(path, HadoopFSUtils.toPath(hoodiePath));
    assertNull(HadoopFSUtils.toPath(null));
    assertNull(HadoopFSUtils.fromPath(null));

    FsPermission permission = new FsPermission(
        FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ, true);
    HoodieFSPermission hoodiePermission = HadoopFSUtils.fromFSPermission(permission);
    assertEquals(permission, HadoopFSUtils.toFSPermission(hoodiePermission));
    assertNull(HadoopFSUtils.toFSPermission(null));
    assertNull(HadoopFSUtils.fromFSPermission(null));

    FileStatus status = new FileStatus(123, false, 2, 4096, 1000, 900,
        permission, "owner", "group", path);
    HoodieFileStatus converted = HadoopFSUtils.fromFileStatus(status);
    assertEquals(path, HadoopFSUtils.toPath(converted.getPath()));
    assertEquals(123, converted.getLength());
    assertEquals("owner", converted.getOwner());
    assertEquals("group", converted.getGroup());
    assertEquals(permission, HadoopFSUtils.toFSPermission(converted.getPermission()));
    assertNull(HadoopFSUtils.fromFileStatus(null));
  }

  @Test
  public void testStatusLocationsAndFileNameHelpers() {
    FileStatus fileStatus = new FileStatus(10, false, 1, 128, 1000, new Path("/table/file.parquet"));
    StoragePathInfo pathInfo = HadoopFSUtils.convertToStoragePathInfo(
        fileStatus, new String[] {"host1", "host2"});
    assertArrayEquals(new String[] {"host1", "host2"}, pathInfo.getLocations());

    assertTrue(HadoopFSUtils.isBaseFile(new Path("fileId_1-0-1_000.parquet")));
    assertTrue(HadoopFSUtils.isLogFile(new Path(".file_100.log.1_1-0-1")));
    assertTrue(HadoopFSUtils.isDataFile(new Path("fileId_1-0-1_000.orc")));
    assertFalse(HadoopFSUtils.isDataFile(new Path("README.md")));
    assertEquals(new Path("file:///table/partition"),
        HadoopFSUtils.constructAbsolutePathInHadoopPath("file:///table", "partition"));
  }

  @Test
  public void testDfsFullPartitionPath() throws IOException {
    try (FileSystem fs = FileSystem.newInstanceLocal(new Configuration())) {
      assertEquals(fs.getUri() + "/tmp/table",
          HadoopFSUtils.getDFSFullPartitionPath(fs, new Path("/tmp/table")));
    }
  }

  @Test
  public void testFileNameAndRelativePathDelegates() {
    assertEquals("partition", HadoopFSUtils.getRelativePartitionPath(
        new Path("/table"), new Path("/table/partition")));
    Path logPath = new Path("/table/.file-id_001.log.1_1-0-1");
    assertEquals("file-id", HadoopFSUtils.getFileIdFromLogPath(logPath));
    assertEquals("001", HadoopFSUtils.getDeltaCommitTimeFromLogPath(logPath));
  }

  @Test
  public void testRecoverLeaseStopsAfterSuccess() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DistributedFileSystem fs = new DistributedFileSystem() {
      @Override
      public boolean recoverLease(Path path) {
        attempts.incrementAndGet();
        return true;
      }
    };

    assertTrue(HadoopFSUtils.recoverDFSFileLease(fs, new Path("/table/file")));
    assertEquals(1, attempts.get());
  }

  @Test
  public void testParallelFileProcessingAndStatusAtLevel(@TempDir java.nio.file.Path tempDir) throws Exception {
    Configuration conf = new Configuration();
    HoodieLocalEngineContext context = new HoodieLocalEngineContext(HadoopFSUtils.getStorageConf(conf));
    try (FileSystem fs = FileSystem.newInstanceLocal(conf)) {
      Map<String, Integer> lengths = HadoopFSUtils.parallelizeFilesProcess(
          context, fs, 2, pair -> pair.getKey().length(), Arrays.asList("a", "longer"));
      assertEquals(1, lengths.get("a"));
      assertEquals(6, lengths.get("longer"));
      assertTrue(HadoopFSUtils.parallelizeFilesProcess(
          context, fs, 2, pair -> pair.getKey().length(), Collections.<String>emptyList()).isEmpty());

      Path root = new Path(tempDir.resolve("root").toUri());
      Path secondLevel = new Path(new Path(root, "first"), "second");
      assertTrue(fs.mkdirs(secondLevel));
      Path leaf = new Path(secondLevel, "leaf.txt");
      assertTrue(fs.createNewFile(leaf));
      List<FileStatus> statuses = HadoopFSUtils.getFileStatusAtLevel(context, fs, root, 2, 2);
      assertEquals(Collections.singletonList(leaf),
          statuses.stream().map(FileStatus::getPath).collect(java.util.stream.Collectors.toList()));
    }
  }

  private void assertFileStatus(FileStatus fileStatus,
                                String path,
                                long length,
                                boolean isDirectory,
                                short blockReplication,
                                long blockSize,
                                long modificationTime) {
    assertEquals(new Path(path), fileStatus.getPath());
    assertEquals(length, fileStatus.getLen());
    assertEquals(isDirectory, fileStatus.isDirectory());
    assertEquals(!isDirectory, fileStatus.isFile());
    assertEquals(blockReplication, fileStatus.getReplication());
    assertEquals(blockSize, fileStatus.getBlockSize());
    assertEquals(modificationTime, fileStatus.getModificationTime());
  }

  private void assertStoragePathInfo(StoragePathInfo pathInfo,
                                     String path,
                                     long length,
                                     boolean isDirectory,
                                     short blockReplication,
                                     long blockSize,
                                     long modificationTime) {
    assertEquals(new StoragePath(path), pathInfo.getPath());
    assertEquals(length, pathInfo.getLength());
    assertEquals(isDirectory, pathInfo.isDirectory());
    assertEquals(!isDirectory, pathInfo.isFile());
    assertEquals(blockReplication, pathInfo.getBlockReplication());
    assertEquals(blockSize, pathInfo.getBlockSize());
    assertEquals(modificationTime, pathInfo.getModificationTime());
  }
}
