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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieRetryWrapperFileSystem {

  @Test
  void testFileOperationsDelegateToWrappedFileSystem(@TempDir java.nio.file.Path tempDir) throws IOException {
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      HoodieRetryWrapperFileSystem fs = new HoodieRetryWrapperFileSystem(local, 0, 0, 0, "");
      Path directory = new Path(tempDir.toUri());
      Path file = new Path(directory, "data.bin");

      assertEquals(local.getUri(), fs.getUri());
      assertEquals(local.getConf(), fs.getConf());
      assertEquals("file", fs.getScheme());
      assertEquals(local.getDefaultReplication(), fs.getDefaultReplication());
      assertEquals(local.getDefaultReplication(file), fs.getDefaultReplication(file));
      fs.setWorkingDirectory(directory);
      assertEquals(directory, fs.getWorkingDirectory());
      assertTrue(fs.mkdirs(directory, FsPermission.getDirDefault()));

      try (FSDataOutputStream output = fs.create(file)) {
        output.write(new byte[] {1, 2, 3});
      }
      assertTrue(fs.exists(file));
      assertEquals(3, fs.getFileStatus(file).getLen());
      fs.open(file).close();
      fs.open(file, 128).close();

      Path[] files = new Path[] {file};
      assertEquals(1, fs.listStatus(directory).length);
      assertEquals(1, fs.listStatus(directory, path -> true).length);
      assertEquals(1, fs.listStatus(files).length);
      assertEquals(1, fs.listStatus(files, path -> true).length);
      assertEquals(1, fs.globStatus(new Path(directory, "*.bin")).length);
      assertEquals(1, fs.globStatus(new Path(directory, "*.bin"), path -> true).length);
      assertTrue(fs.listLocatedStatus(directory).hasNext());
      assertTrue(fs.listFiles(directory, true).hasNext());

      Path renamed = new Path(directory, "renamed.bin");
      assertTrue(fs.rename(file, renamed));
      assertTrue(fs.delete(renamed, false));
      assertFalse(fs.exists(renamed));
      assertTrue(fs.createNewFile(new Path(directory, "empty")));
    }
  }

  @Test
  void testCreateOverloadsDelegate(@TempDir java.nio.file.Path tempDir) throws IOException {
    try (FileSystem local = FileSystem.newInstanceLocal(new Configuration())) {
      HoodieRetryWrapperFileSystem fs = new HoodieRetryWrapperFileSystem(local, 0, 0, 0, "");
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
    }
  }

  @Test
  void testDeleteRetriesWhenDelegateReportsExistingFile() throws IOException {
    FlakyDeleteFileSystem delegate = new FlakyDeleteFileSystem(FileSystem.newInstanceLocal(new Configuration()));
    try {
      HoodieRetryWrapperFileSystem fs = new HoodieRetryWrapperFileSystem(delegate, 0, 2, 0, "");

      assertTrue(fs.delete(new Path("file:///eventually-deleted")));
      assertEquals(3, delegate.deleteAttempts);
      assertEquals(2, delegate.existsChecks);
    } finally {
      delegate.close();
    }
  }

  private static Path nextPath(Path directory, AtomicInteger suffix) {
    return new Path(directory, "create-" + suffix.incrementAndGet());
  }

  private static void close(FSDataOutputStream stream) throws IOException {
    stream.close();
  }

  private static class FlakyDeleteFileSystem extends FilterFileSystem {
    private int deleteAttempts;
    private int existsChecks;

    private FlakyDeleteFileSystem(FileSystem delegate) {
      super(delegate);
    }

    @Override
    public boolean delete(Path path, boolean recursive) {
      return ++deleteAttempts >= 3;
    }

    @Override
    public boolean exists(Path path) {
      existsChecks++;
      return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      return fs.getFileStatus(path);
    }
  }
}
