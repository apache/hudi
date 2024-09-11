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

package org.apache.hudi.common.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A non-serializable file system for testing only. See {@link TestHoodieSerializableFileStatus}
 * Can't make this an inner class as the outer class would also be non-serializable and invalidate
 * the purpose of testing
 */
public class NonSerializableFileSystem extends FileSystem {
  @Override
  public URI getUri() {
    try {
      return new URI("");
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Override
  public FSDataInputStream open(Path path, int i) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i,
      short i1, long l, Progressable progressable) throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable)
      throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    return false;
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    FileStatus[] ret = new FileStatus[5];
    for (int i = 0; i < 5; i++) {
      ret[i] = new FileStatus(100L, false, 1, 10000L,
          0L, 0, null, "owner", "group", path) {
        Configuration conf = getConf();

        @Override
        public long getLen() {
          return -1;
        }
      };
    }
    return ret;
  }

  @Override
  public void setWorkingDirectory(Path path) {
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    return false;
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return null;
  }

  public Configuration getConf() {
    return new Configuration();
  }
}
