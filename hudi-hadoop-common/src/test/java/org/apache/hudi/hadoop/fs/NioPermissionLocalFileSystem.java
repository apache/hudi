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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

/**
 * Local file system for tests that applies permissions through java.nio instead of forking a
 * {@code chmod} process per file, which is what {@link RawLocalFileSystem} does when the Hadoop
 * native library is not available (the usual case for tests). Only plain rwx modes are handled
 * this way; sticky/setuid/setgid modes, directories carrying setuid/setgid bits (which chmod
 * preserves) and file systems without POSIX attributes fall back to the stock implementation.
 *
 * <p>Use {@link #install} rather than naming the class in a configuration: it keeps the rename
 * contract of whichever local file system the {@code file} scheme resolves to on the classpath.
 */
public class NioPermissionLocalFileSystem extends LocalFileSystem {

  private static final String HIVE_PROXY_LOCAL_FS = "org.apache.hadoop.hive.ql.io.ProxyLocalFileSystem";
  private static final URI LOCAL_FS_URI = URI.create("file:///");

  public NioPermissionLocalFileSystem() {
    super(new NioPermissionRawLocalFileSystem());
  }

  /**
   * Points the {@code file} scheme of {@code conf} at this file system (or {@link WithHiveRename}, see below),
   * and replaces the current user's cached local file system, which would otherwise keep being returned. Needed by
   * harnesses that build their configuration with {@code new Configuration(false)} and so never read a
   * test core-site.xml. The replaced instance is closed, which deletes its {@code deleteOnExit} paths.
   *
   * <p>Where the {@code file} scheme resolves to Hive's {@code ProxyLocalFileSystem} (classpaths with
   * hive-exec, such as hudi-utilities and hudi-spark), {@link WithHiveRename} is used so that renaming
   * onto an existing file still fails. Any other local file system implementation is left in place and
   * {@code conf} is not changed.
   */
  public static void install(Configuration conf) throws IOException {
    Class<? extends FileSystem> stock = FileSystem.getFileSystemClass("file", new Configuration(false));
    Class<? extends FileSystem> impl;
    if (stock == LocalFileSystem.class) {
      impl = NioPermissionLocalFileSystem.class;
    } else if (HIVE_PROXY_LOCAL_FS.equals(stock.getName())) {
      impl = WithHiveRename.class;
    } else {
      return;
    }
    conf.set("fs.file.impl", impl.getName());
    FileSystem cached = FileSystem.get(LOCAL_FS_URI, conf);
    if (cached.getClass() != impl) {
      cached.close();
      FileSystem.get(LOCAL_FS_URI, conf);
    }
  }

  /**
   * {@link NioPermissionLocalFileSystem} with the rename contract of Hive's {@code ProxyLocalFileSystem}.
   */
  public static class WithHiveRename extends NioPermissionLocalFileSystem {
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      // same as ProxyLocalFileSystem: an existing destination file fails the rename
      return !isFile(dst) && super.rename(src, dst);
    }
  }

  public static class NioPermissionRawLocalFileSystem extends RawLocalFileSystem {
    private static final PosixFilePermission[] BITS = {
        PosixFilePermission.OTHERS_EXECUTE, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_READ,
        PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_READ,
        PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ};

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
      if (NativeIO.isAvailable()) {
        super.setPermission(p, permission);
        return;
      }
      int mode = permission.toShort();
      java.nio.file.Path path = pathToFile(p).toPath();
      if ((mode & ~0777) != 0 || hasSpecialBits(path)) {
        super.setPermission(p, permission);
        return;
      }
      Set<PosixFilePermission> perms = EnumSet.noneOf(PosixFilePermission.class);
      for (int i = 0; i < BITS.length; i++) {
        if ((mode & (1 << i)) != 0) {
          perms.add(BITS[i]);
        }
      }
      try {
        Files.setPosixFilePermissions(path, perms);
      } catch (UnsupportedOperationException e) {
        super.setPermission(p, permission);
      }
    }

    private static boolean hasSpecialBits(java.nio.file.Path path) {
      if (!Files.isDirectory(path)) {
        return false;
      }
      try {
        return (((Integer) Files.getAttribute(path, "unix:mode")) & 06000) != 0;
      } catch (UnsupportedOperationException | IllegalArgumentException | IOException e) {
        return true;
      }
    }
  }
}
