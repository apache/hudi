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

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieWrapperFileSystemOperations {

  @Test
  void testPathTranslationAndInitialization(@TempDir java.nio.file.Path tempDir) throws Exception {
    Configuration conf = new Configuration();
    try (FileSystem local = FileSystem.newInstanceLocal(conf)) {
      HoodieWrapperFileSystem fs = new HoodieWrapperFileSystem(local, new NoOpConsistencyGuard());
      Path path = new Path(tempDir.resolve("file.parquet").toUri());

      assertEquals("hoodie-file", HoodieWrapperFileSystem.getHoodieScheme("file"));
      assertThrows(IllegalArgumentException.class, () -> HoodieWrapperFileSystem.getHoodieScheme("unknown"));
      assertEquals("hoodie-file", HoodieWrapperFileSystem.convertToHoodiePath(
          new StoragePath(path.toUri()), conf).toUri().getScheme());

      Path converted = HoodieWrapperFileSystem.convertPathWithScheme(
          new Path(new URI("file", null, path.toUri().getPath(), "query=1", "fragment")), "hoodie-file");
      assertTrue(converted instanceof CachingPath);
      assertEquals("hoodie-file", converted.toUri().getScheme());
      assertEquals("query=1", converted.toUri().getQuery());
      assertEquals("fragment", converted.toUri().getFragment());

      assertEquals("file", fs.getScheme());
      assertEquals("hoodie-file", fs.convertToHoodiePath(path).toUri().getScheme());
      assertSame(local, fs.getFileSystem());
      assertEquals(local.getUri(), fs.getUri());
      assertEquals(local.getConf(), fs.getConf());
      assertEquals(local.hashCode(), fs.hashCode());
      assertTrue(fs.equals(local));
      assertEquals(local.toString(), fs.toString());
      fs.setConf(new Configuration());
      assertSame(local.getConf(), fs.getConf());
      fs.close();

      HoodieWrapperFileSystem initialized = new HoodieWrapperFileSystem();
      initialized.initialize(URI.create("hoodie-file:" + tempDir.toUri().getPath()), conf);
      assertEquals("file", initialized.getScheme());
      assertEquals("file", initialized.getFileSystem().getScheme());

      HoodieWrapperFileSystem initializedWithoutPrefix = new HoodieWrapperFileSystem();
      initializedWithoutPrefix.initialize(tempDir.toUri(), conf);
      assertEquals("file", initializedWithoutPrefix.getScheme());
    }
  }

  @Test
  void testLocalFileOperationsAndStreamAccounting(@TempDir java.nio.file.Path tempDir) throws IOException {
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      HoodieWrapperFileSystem fs = new HoodieWrapperFileSystem(local, new NoOpConsistencyGuard());
      Path directory = new Path(tempDir.resolve("table").toUri());
      Path file = new Path(directory, "data.bin");

      assertTrue(fs.mkdirs(directory));
      assertTrue(fs.mkdirs(directory, FsPermission.getDirDefault()));
      fs.setWorkingDirectory(directory);
      assertEquals("hoodie-file", fs.getWorkingDirectory().toUri().getScheme());
      assertEquals("hoodie-file", fs.getHomeDirectory().toUri().getScheme());

      try (FSDataOutputStream output = fs.create(file)) {
        output.write(new byte[] {1, 2, 3, 4}, 0, 4);
        assertEquals(4, fs.getBytesWritten(file));
      }
      assertThrows(IllegalArgumentException.class, () -> fs.getBytesWritten(file));

      try (FSDataInputStream input = fs.open(file)) {
        assertEquals(1, input.read());
      }
      try (FSDataInputStream input = fs.open(file, 128)) {
        assertEquals(1, input.read());
      }

      assertTrue(fs.exists(file));
      assertTrue(fs.isFile(file));
      assertFalse(fs.isDirectory(file));
      assertEquals(4, fs.getLength(file));
      assertEquals(4, fs.getFileStatus(file).getLen());
      assertEquals(4, fs.getContentSummary(file).getLength());
      assertEquals(1, fs.listStatus(directory).length);
      assertEquals(1, fs.listStatus(directory, candidate -> true).length);
      assertEquals(1, fs.listStatus(new Path[] {file}).length);
      assertEquals(1, fs.listStatus(new Path[] {file}, candidate -> true).length);
      assertEquals(0, fs.listStatus(new Path[0]).length);
      assertEquals(0, fs.listStatus(new Path[0], candidate -> true).length);
      assertEquals(1, fs.globStatus(new Path(directory, "*.bin")).length);
      assertEquals(1, fs.globStatus(new Path(directory, "*.bin"), candidate -> true).length);
      assertTrue(fs.listLocatedStatus(directory).hasNext());
      assertTrue(fs.listFiles(directory, true).hasNext());

      FileStatus status = fs.getFileStatus(file);
      assertNotNull(fs.getFileBlockLocations(status, 0, status.getLen()));
      assertNotNull(fs.getFileBlockLocations(file, 0, status.getLen()));
      assertNotNull(fs.getServerDefaults());
      assertNotNull(fs.getServerDefaults(file));
      assertNotNull(fs.getStatus());
      assertNotNull(fs.getStatus(file));
      assertTrue(fs.getBlockSize(file) > 0);
      assertTrue(fs.getDefaultBlockSize() > 0);
      assertTrue(fs.getDefaultBlockSize(file) > 0);
      assertTrue(fs.getDefaultReplication() > 0);
      assertTrue(fs.getDefaultReplication(file) > 0);
      assertTrue(fs.getReplication(file) > 0);
      assertTrue(fs.setReplication(file, fs.getReplication(file)));
      fs.access(file, FsAction.READ);
      fs.setPermission(file, FsPermission.getFileDefault());
      fs.setTimes(file, System.currentTimeMillis(), -1);
      fs.setVerifyChecksum(true);
      fs.setWriteChecksum(true);
      assertEquals(local.getFileChecksum(file), fs.getFileChecksum(file));

      assertEquals("hoodie-file", fs.makeQualified(file).toUri().getScheme());
      assertEquals("hoodie-file", fs.resolvePath(file).toUri().getScheme());
      assertEquals(local.getCanonicalServiceName(), fs.getCanonicalServiceName());
      assertEquals(local.getName(), fs.getName());
      assertEquals(local.supportsSymlinks(), fs.supportsSymlinks());
      assertArrayEquals(local.getChildFileSystems(), fs.getChildFileSystems());

      Path renamed = new Path(directory, "renamed.bin");
      assertTrue(fs.rename(file, renamed));
      assertTrue(fs.delete(renamed));
      assertFalse(fs.exists(renamed));
      assertTrue(fs.createNewFile(new Path(directory, "empty")));
    }
  }

  @Test
  void testCreateOverloads(@TempDir java.nio.file.Path tempDir) throws IOException {
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      HoodieWrapperFileSystem fs = new HoodieWrapperFileSystem(local, new NoOpConsistencyGuard());
      Path directory = new Path(tempDir.toUri());
      FsPermission permission = FsPermission.getFileDefault();
      short replication = local.getDefaultReplication(directory);
      long blockSize = local.getDefaultBlockSize(directory);
      AtomicInteger suffix = new AtomicInteger();

      close(fs.create(nextPath(directory, suffix), true));
      close(fs.create(nextPath(directory, suffix), () -> { }));
      close(fs.create(nextPath(directory, suffix), replication));
      close(fs.create(nextPath(directory, suffix), replication, () -> { }));
      close(fs.create(nextPath(directory, suffix), true, 4096));
      close(fs.create(nextPath(directory, suffix), true, 4096, () -> { }));
      close(fs.create(nextPath(directory, suffix), true, 4096, replication, blockSize));
      close(fs.create(nextPath(directory, suffix), true, 4096, replication, blockSize, () -> { }));
      close(fs.create(nextPath(directory, suffix), permission, true, 4096, replication, blockSize, () -> { }));
      close(fs.create(nextPath(directory, suffix), permission, EnumSet.of(CreateFlag.CREATE),
          4096, replication, blockSize, () -> { }));
      close(fs.create(nextPath(directory, suffix), permission, EnumSet.of(CreateFlag.CREATE),
          4096, replication, blockSize, () -> { }, Options.ChecksumOpt.createDisabled()));

      Path nonRecursive = nextPath(directory, suffix);
      close(fs.createNonRecursive(nonRecursive, true, 4096, replication, blockSize, () -> { }));
      close(fs.createNonRecursive(nextPath(directory, suffix), permission, true,
          4096, replication, blockSize, () -> { }));
      close(fs.createNonRecursive(nextPath(directory, suffix), permission, EnumSet.of(CreateFlag.CREATE),
          4096, replication, blockSize, () -> { }));
    }
  }

  @Test
  void testDataAndMetadataMetricsAreSeparated(@TempDir java.nio.file.Path tempDir) throws IOException {
    Registry dataRegistry = Registry.getRegistry("wrapper-coverage-data-" + System.nanoTime());
    Registry metaRegistry = Registry.getRegistry("wrapper-coverage-meta-" + System.nanoTime());
    HoodieWrapperFileSystem.setMetricsRegistry(dataRegistry, metaRegistry);
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      HoodieWrapperFileSystem fs = new HoodieWrapperFileSystem(local, new NoOpConsistencyGuard());
      Path dataDirectory = new Path(tempDir.resolve("data").toUri());
      Path metaDirectory = new Path(tempDir.resolve(".hoodie").toUri());

      assertTrue(fs.mkdirs(dataDirectory));
      assertTrue(fs.mkdirs(metaDirectory));
      assertEquals(1L, dataRegistry.getAllCounts().get("mkdirs"));
      assertEquals(1L, metaRegistry.getAllCounts().get("mkdirs"));

      HoodieWrapperFileSystem.executeFuncWithTimeAndByteMetrics(
          "write", dataDirectory, 17, () -> true);
      assertEquals(17L, dataRegistry.getAllCounts().get("write.totalBytes"));
      assertEquals(1L, dataRegistry.getAllCounts().get("write"));
    } finally {
      HoodieWrapperFileSystem.setMetricsRegistry(null, null);
    }
  }

  @Test
  void testOptionalDelegatedFileSystemApis() throws IOException {
    FileSystem delegate = mock(FileSystem.class);
    when(delegate.getUri()).thenReturn(URI.create("file:///"));
    when(delegate.getScheme()).thenReturn("file");
    when(delegate.getUsed()).thenReturn(42L);

    HoodieWrapperFileSystem fs = wrapper(delegate, new NoOpConsistencyGuard());
    Path directory = new Path("hoodie-file:///optional");
    Path file = new Path(directory, "data.bin");
    Path link = new Path(directory, "data.link");
    Path deleteOnExit = new Path(directory, "delete-on-exit");
    Path defaultDirectory = new Path("file:///optional");
    Path defaultFile = new Path(defaultDirectory, "data.bin");
    Path defaultLink = new Path(defaultDirectory, "data.link");
    Path defaultDeleteOnExit = new Path(defaultDirectory, "delete-on-exit");
    Credentials credentials = new Credentials();
    List<AclEntry> aclEntries = Collections.emptyList();
    byte[] attribute = new byte[] {1, 2};
    EnumSet<XAttrSetFlag> xAttrFlags = EnumSet.of(XAttrSetFlag.REPLACE);
    List<String> xAttrNames = Collections.singletonList("user.hudi");

    when(delegate.getLinkTarget(defaultLink)).thenReturn(defaultFile);
    when(delegate.createSnapshot(defaultDirectory, "snapshot")).thenReturn(defaultDirectory);
    when(delegate.deleteOnExit(defaultDeleteOnExit)).thenReturn(true);
    when(delegate.cancelDeleteOnExit(defaultDeleteOnExit)).thenReturn(true);

    fs.getDelegationToken("renewer");
    fs.addDelegationTokens("renewer", credentials);
    fs.listCorruptFileBlocks(directory);
    assertEquals(42L, fs.getUsed());
    fs.getFileChecksum(file, 1);
    fs.setOwner(file, "owner", null);
    fs.createSymlink(file, link, false);
    fs.getFileLinkStatus(link);
    assertEquals("hoodie-file", fs.getLinkTarget(link).toUri().getScheme());
    assertEquals("hoodie-file", fs.createSnapshot(directory, "snapshot").toUri().getScheme());
    fs.renameSnapshot(directory, "snapshot", "renamed");
    fs.deleteSnapshot(directory, "renamed");
    fs.modifyAclEntries(file, aclEntries);
    fs.removeAclEntries(file, aclEntries);
    fs.removeDefaultAcl(file);
    fs.removeAcl(file);
    fs.setAcl(file, aclEntries);
    fs.getAclStatus(file);
    fs.setXAttr(file, "user.hudi", attribute);
    fs.setXAttr(file, "user.hudi", attribute, xAttrFlags);
    fs.getXAttr(file, "user.hudi");
    fs.getXAttrs(file);
    fs.getXAttrs(file, xAttrNames);
    fs.listXAttrs(file);
    fs.removeXAttr(file, "user.hudi");
    assertTrue(fs.deleteOnExit(deleteOnExit));
    assertTrue(fs.cancelDeleteOnExit(deleteOnExit));

    verify(delegate).getDelegationToken("renewer");
    verify(delegate).addDelegationTokens("renewer", credentials);
    verify(delegate).listCorruptFileBlocks(defaultDirectory);
    verify(delegate).getUsed();
    verify(delegate).getFileChecksum(defaultFile, 1);
    verify(delegate).setOwner(defaultFile, "owner", null);
    verify(delegate).createSymlink(defaultFile, defaultLink, false);
    verify(delegate).getFileLinkStatus(defaultLink);
    verify(delegate).getLinkTarget(defaultLink);
    verify(delegate).createSnapshot(defaultDirectory, "snapshot");
    verify(delegate).renameSnapshot(defaultDirectory, "snapshot", "renamed");
    verify(delegate).deleteSnapshot(defaultDirectory, "renamed");
    verify(delegate).modifyAclEntries(defaultFile, aclEntries);
    verify(delegate).removeAclEntries(defaultFile, aclEntries);
    verify(delegate).removeDefaultAcl(defaultFile);
    verify(delegate).removeAcl(defaultFile);
    verify(delegate).setAcl(defaultFile, aclEntries);
    verify(delegate).getAclStatus(defaultFile);
    verify(delegate).setXAttr(defaultFile, "user.hudi", attribute);
    verify(delegate).setXAttr(defaultFile, "user.hudi", attribute, xAttrFlags);
    verify(delegate).getXAttr(defaultFile, "user.hudi");
    verify(delegate).getXAttrs(defaultFile);
    verify(delegate).getXAttrs(defaultFile, xAttrNames);
    verify(delegate).listXAttrs(defaultFile);
    verify(delegate).removeXAttr(defaultFile, "user.hudi");
    verify(delegate).deleteOnExit(defaultDeleteOnExit);
    verify(delegate).cancelDeleteOnExit(defaultDeleteOnExit);
  }

  @Test
  void testLocalCopyAndMoveOverloads(@TempDir java.nio.file.Path tempDir) throws Exception {
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      HoodieWrapperFileSystem fs = new HoodieWrapperFileSystem(local, new NoOpConsistencyGuard());
      Path destinationDirectory = new Path(tempDir.resolve("destinations").toUri());
      assertTrue(fs.mkdirs(destinationDirectory));

      Path copied = new Path(destinationDirectory, "copied");
      fs.copyFromLocalFile(localFile(tempDir, "copy-source", 1), copied);
      assertTrue(fs.exists(copied));

      Path moved = new Path(destinationDirectory, "moved");
      fs.moveFromLocalFile(localFile(tempDir, "move-source", 2), moved);
      assertTrue(fs.exists(moved));

      Path copiedWithFlag = new Path(destinationDirectory, "copied-with-flag");
      fs.copyFromLocalFile(false, localFile(tempDir, "copy-flag-source", 3), copiedWithFlag);
      assertTrue(fs.exists(copiedWithFlag));

      Path copiedWithFlags = new Path(destinationDirectory, "copied-with-flags");
      fs.copyFromLocalFile(false, true, localFile(tempDir, "copy-flags-source", 4), copiedWithFlags);
      assertTrue(fs.exists(copiedWithFlags));

      Path arrayDestination = new Path(destinationDirectory, "array-copy");
      assertTrue(fs.mkdirs(arrayDestination));
      fs.copyFromLocalFile(false, true,
          new Path[] {localFile(tempDir, "array-source-1", 5), localFile(tempDir, "array-source-2", 6)},
          arrayDestination);
      assertEquals(2, fs.listStatus(arrayDestination).length);

      Path moveArrayDestination = new Path(destinationDirectory, "array-move");
      assertTrue(fs.mkdirs(moveArrayDestination));
      fs.moveFromLocalFile(
          new Path[] {localFile(tempDir, "move-array-source-1", 7), localFile(tempDir, "move-array-source-2", 8)},
          moveArrayDestination);
      assertEquals(2, fs.listStatus(moveArrayDestination).length);

      Path localCopy = new Path(tempDir.resolve("local-copy").toUri());
      fs.copyToLocalFile(copied, localCopy);
      assertTrue(local.exists(localCopy));
      Path localCopyWithFlag = new Path(tempDir.resolve("local-copy-flag").toUri());
      fs.copyToLocalFile(false, copied, localCopyWithFlag);
      assertTrue(local.exists(localCopyWithFlag));
      Path localRawCopy = new Path(tempDir.resolve("local-raw-copy").toUri());
      fs.copyToLocalFile(false, copied, localRawCopy, true);
      assertTrue(local.exists(localRawCopy));

      Path localMove = new Path(tempDir.resolve("local-move").toUri());
      fs.moveToLocalFile(copiedWithFlag, localMove);
      assertTrue(local.exists(localMove));
    }
  }

  @Test
  void testConsistencyGuardTimeoutBranches(@TempDir java.nio.file.Path tempDir) throws Exception {
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      Path root = new Path(tempDir.toUri());

      HoodieWrapperFileSystem mkdirFs = wrapper(local, new FailingConsistencyGuard(1, 0));
      assertThrows(HoodieException.class, () -> mkdirFs.mkdirs(new Path(root, "mkdir-timeout")));
      HoodieWrapperFileSystem permissionMkdirFs = wrapper(local, new FailingConsistencyGuard(1, 0));
      assertThrows(HoodieException.class, () -> permissionMkdirFs.mkdirs(
          new Path(root, "permission-mkdir-timeout"), FsPermission.getDirDefault()));

      HoodieWrapperFileSystem createFs = wrapper(local, new FailingConsistencyGuard(1, 0));
      assertThrows(HoodieException.class, () -> createFs.createNewFile(new Path(root, "create-timeout")));

      Path statusFile = new Path(root, "status-timeout");
      assertTrue(local.createNewFile(statusFile));
      HoodieWrapperFileSystem statusFs = wrapper(local, new FailingConsistencyGuard(1, 0));
      assertEquals(0, statusFs.getFileStatus(statusFile).getLen());

      Path deleteFile = new Path(root, "delete-timeout");
      assertTrue(local.createNewFile(deleteFile));
      HoodieWrapperFileSystem deleteFs = wrapper(local, new FailingConsistencyGuard(0, 1));
      assertThrows(HoodieException.class, () -> deleteFs.delete(deleteFile, false));

      Path renameBefore = new Path(root, "rename-before");
      assertTrue(local.createNewFile(renameBefore));
      HoodieWrapperFileSystem renameBeforeFs = wrapper(local, new FailingConsistencyGuard(1, 0));
      assertThrows(HoodieException.class,
          () -> renameBeforeFs.rename(renameBefore, new Path(root, "rename-before-destination")));

      Path renameDestination = new Path(root, "rename-destination");
      assertTrue(local.createNewFile(renameDestination));
      HoodieWrapperFileSystem renameDestinationFs = wrapper(local, new FailingConsistencyGuard(2, 0));
      assertThrows(HoodieException.class,
          () -> renameDestinationFs.rename(renameDestination, new Path(root, "rename-destination-result")));

      Path renameDisappear = new Path(root, "rename-disappear");
      assertTrue(local.createNewFile(renameDisappear));
      HoodieWrapperFileSystem renameDisappearFs = wrapper(local, new FailingConsistencyGuard(0, 1));
      assertThrows(HoodieException.class,
          () -> renameDisappearFs.rename(renameDisappear, new Path(root, "rename-disappear-result")));
    }
  }

  private static Path nextPath(Path directory, AtomicInteger suffix) {
    return new Path(directory, "create-" + suffix.incrementAndGet());
  }

  private static void close(FSDataOutputStream stream) throws IOException {
    stream.close();
  }

  private static HoodieWrapperFileSystem wrapper(FileSystem fileSystem, ConsistencyGuard consistencyGuard) {
    return new HoodieWrapperFileSystem(fileSystem, consistencyGuard);
  }

  private static Path localFile(java.nio.file.Path tempDir, String name, int value) throws IOException {
    java.nio.file.Path file = tempDir.resolve(name);
    Files.write(file, new byte[] {(byte) value});
    return new Path(file.toUri());
  }

  private static final class FailingConsistencyGuard implements ConsistencyGuard {
    private final int failAppearCall;
    private final int failDisappearCall;
    private int appearCalls;
    private int disappearCalls;

    private FailingConsistencyGuard(int failAppearCall, int failDisappearCall) {
      this.failAppearCall = failAppearCall;
      this.failDisappearCall = failDisappearCall;
    }

    @Override
    public void waitTillFileAppears(StoragePath filePath) throws TimeoutException {
      if (++appearCalls == failAppearCall) {
        throw new TimeoutException(filePath.toString());
      }
    }

    @Override
    public void waitTillFileDisappears(StoragePath filePath) throws TimeoutException {
      if (++disappearCalls == failDisappearCall) {
        throw new TimeoutException(filePath.toString());
      }
    }

    @Override
    public void waitTillAllFilesAppear(String dirPath, List<String> files) {
    }

    @Override
    public void waitTillAllFilesDisappear(String dirPath, List<String> files) {
    }
  }
}
