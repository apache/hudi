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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.atomic.AtomicInteger;

/** A bucket-folder file system that injects source status, existence and open faults by mode; no network or Spark session. */
public class BlobUnhappyFileSystem extends BucketFolderFileSystem {

  static final String MODE = "test.blob.unhappy.mode";
  static final AtomicInteger OPENS = new AtomicInteger();

  static void install(Configuration conf, java.nio.file.Path root, String mode) {
    BucketFolderFileSystem.install(conf, root);
    conf.set("fs." + SCHEME + ".impl", BlobUnhappyFileSystem.class.getName());
    conf.set(MODE, mode);
    OPENS.set(0);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    String mode = getConf().get(MODE, "healthy");
    if ("status-denied".equals(mode) || "status-unknown".equals(mode)) {
      throw new FileNotFoundException("injected status failure for " + path);
    }
    return super.getFileStatus(path);
  }

  @Override
  public boolean exists(Path path) throws IOException {
    String mode = getConf().get(MODE, "healthy");
    if ("status-unknown".equals(mode) || "open-unknown".equals(mode)) {
      throw new IOException("injected existence recheck failure");
    }
    // The existence recheck must be independent of a getFileStatus denial.
    return pathToFile(path).exists();
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    int attempt = OPENS.incrementAndGet();
    String mode = getConf().get(MODE, "healthy");
    if ("open-missing".equals(mode)) {
      java.nio.file.Files.deleteIfExists(pathToFile(path).toPath());
      throw new FileNotFoundException("injected object removed after status");
    }
    if ("open-denied".equals(mode) || "open-unknown".equals(mode)) {
      throw new FileNotFoundException("injected unreadable object");
    }
    if ("access-denied".equals(mode)) {
      throw new AccessDeniedException(path.toString(), null, "injected 403");
    }
    FSDataInputStream input = super.open(path, bufferSize);
    if (!"always-partial".equals(mode) && !("partial-once".equals(mode) && attempt == 1)) {
      return input;
    }
    return new FSDataInputStream(new FSInputStream() {
      private boolean deliveredPrefix;

      @Override
      public int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
      }

      @Override
      public int read(byte[] buffer, int offset, int length) throws IOException {
        if (length == 0) {
          return 0;
        }
        if (deliveredPrefix) {
          throw new IOException("injected source connection reset after prefix");
        }
        int count = input.read(buffer, offset, Math.min(length, 4));
        deliveredPrefix = count > 0;
        return count;
      }

      @Override
      public void seek(long position) throws IOException {
        input.seek(position);
      }

      @Override
      public long getPos() throws IOException {
        return input.getPos();
      }

      @Override
      public boolean seekToNewSource(long position) throws IOException {
        return input.seekToNewSource(position);
      }

      @Override
      public void close() throws IOException {
        input.close();
      }
    });
  }
}
