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

package org.apache.hudi.storage.hadoop;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.TestHoodieStorageBase;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieHadoopStorage}.
 */
public class TestHoodieHadoopStorage extends TestHoodieStorageBase {
  private static final String CONF_KEY = "hudi.testing.key";
  private static final String CONF_VALUE = "value";

  @Override
  protected HoodieStorage getStorage(Object fs, Object conf) {
    return new HoodieHadoopStorage((FileSystem) fs);
  }

  @Override
  protected Object getFileSystem(Object conf) {
    return HadoopFSUtils.getFs(getTempDir(), (Configuration) conf, true);
  }

  @Override
  protected Object getConf() {
    Configuration conf = new Configuration();
    conf.set(CONF_KEY, CONF_VALUE);
    return conf;
  }

  @Test
  void testClose() throws IOException {
    Configuration conf = new Configuration();
    String path = getTempDir();
    FileSystem fileSystem = HadoopFSUtils.getFs(path, conf, true);
    HoodieStorage storage = new HoodieHadoopStorage(fileSystem);
    storage.close();
    // This validates that HoodieHadoopStorage#close does not close the underlying FileSystem
    // object. If the underlying FileSystem object is closed, the cache of the object based on
    // the path is closed and removed, which causes problems if it is reused elsewhere. Fetching
    // the FileSystem object on the same path again in this case returns a different object,
    // which can be caught here.
    assertSame(fileSystem, storage.getFileSystem());
    assertSame(fileSystem, HadoopFSUtils.getFs(getTempDir(), conf, true));
  }

  @Test
  void testCreateImmutableFileCleansTemporaryFileAfterCloseFailure() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSystem = HadoopFSUtils.getFs(getTempDir(), conf, true);
    HoodieStorage storage = new CloseFailingHoodieHadoopStorage(fileSystem);
    StoragePath directory = new StoragePath(getTempDir(), "testImmutableFileCloseFailure");
    StoragePath path = new StoragePath(directory, "1.file");
    storage.createDirectory(directory);

    HoodieIOException exception = assertThrows(HoodieIOException.class,
        () -> storage.createImmutableFileInPath(path,
            Option.of(HoodieInstantWriter.convertByteArrayToWriter(new byte[] {42}))));

    assertTrue(exception.getMessage().startsWith("Failed to create immutable file "));
    assertFalse(storage.exists(path));
    assertTrue(storage.listDirectEntries(directory).isEmpty());
  }

  @Test
  void testCreateImmutableFileCleansTemporaryFileAfterUncheckedRenameFailure() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSystem = HadoopFSUtils.getFs(getTempDir(), conf, true);
    HoodieStorage storage = new RenameFailingHoodieHadoopStorage(fileSystem);
    StoragePath directory = new StoragePath(getTempDir(), "testImmutableFileRenameFailure");
    StoragePath path = new StoragePath(directory, "1.file");
    storage.createDirectory(directory);

    HoodieException exception = assertThrows(HoodieException.class,
        () -> storage.createImmutableFileInPath(path,
            Option.of(HoodieInstantWriter.convertByteArrayToWriter(new byte[] {42}))));

    assertEquals("rename failure", exception.getMessage());
    assertFalse(storage.exists(path));
    assertTrue(storage.listDirectEntries(directory).isEmpty());
  }

  @Test
  void testCreateImmutableFileSuppressesUncheckedCleanupFailure() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fileSystem = HadoopFSUtils.getFs(getTempDir(), conf, true);
    CleanupFailingHoodieHadoopStorage storage = new CleanupFailingHoodieHadoopStorage(fileSystem);
    StoragePath directory = new StoragePath(getTempDir(), "testImmutableFileCleanupFailure");
    StoragePath path = new StoragePath(directory, "1.file");
    storage.createDirectory(directory);

    HoodieIOException exception = assertThrows(HoodieIOException.class,
        () -> storage.createImmutableFileInPath(path, Option.of(outputStream -> {
          outputStream.write(42);
          throw new IOException("write failure");
        })));

    assertEquals("write failure", exception.getCause().getMessage());
    assertEquals(1, exception.getSuppressed().length);
    assertEquals("cleanup failure", exception.getSuppressed()[0].getMessage());
    assertFalse(storage.exists(path));
    assertEquals(1, storage.listDirectEntries(directory).size());
    StoragePath temporaryPath = storage.listDirectEntries(directory).get(0).getPath();
    storage.deleteTemporaryFileAfterTest(temporaryPath);
  }

  private static class CloseFailingHoodieHadoopStorage extends HoodieHadoopStorage {
    CloseFailingHoodieHadoopStorage(FileSystem fileSystem) {
      super(fileSystem);
    }

    @Override
    public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
      return new FilterOutputStream(super.create(path, overwrite)) {
        @Override
        public void close() throws IOException {
          super.close();
          throw new IOException("close failure");
        }
      };
    }
  }

  private static class RenameFailingHoodieHadoopStorage extends HoodieHadoopStorage {
    RenameFailingHoodieHadoopStorage(FileSystem fileSystem) {
      super(fileSystem);
    }

    @Override
    public boolean rename(StoragePath oldPath, StoragePath newPath) {
      throw new HoodieException("rename failure");
    }
  }

  private static class CleanupFailingHoodieHadoopStorage extends HoodieHadoopStorage {
    CleanupFailingHoodieHadoopStorage(FileSystem fileSystem) {
      super(fileSystem);
    }

    @Override
    public boolean deleteFile(StoragePath path) {
      throw new HoodieIOException("cleanup failure");
    }

    void deleteTemporaryFileAfterTest(StoragePath path) throws IOException {
      super.deleteFile(path);
    }
  }
}
