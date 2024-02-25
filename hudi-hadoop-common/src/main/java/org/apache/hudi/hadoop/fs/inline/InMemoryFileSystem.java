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

package org.apache.hudi.hadoop.fs.inline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A FileSystem which stores all content in memory and returns a byte[] when {@link #getFileAsBytes()} is called
 * This FileSystem is used only in write path. Does not support any read apis except {@link #getFileAsBytes()}.
 */
public class InMemoryFileSystem extends FileSystem {

  // TODO: this needs to be per path to support num_cores > 1, and we should release the buffer once done
  private ByteArrayOutputStream bos;
  private Configuration conf = null;
  public static final String SCHEME = "inmemfs";
  private URI uri;

  InMemoryFileSystem() {
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.conf = conf;
    this.uri = name;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(Path inlinePath, int bufferSize) {
    return null;
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                   Progressable progressable) throws IOException {
    bos = new ByteArrayOutputStream();
    try {
      this.uri = new URI(path.toString());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Path not parsable as URI " + path);
    }
    return new FSDataOutputStream(bos, new Statistics(getScheme()));
  }

  public byte[] getFileAsBytes() {
    return bos.toByteArray();
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    return null;
  }

  @Override
  public boolean rename(Path path, Path path1) throws IOException {
    throw new UnsupportedOperationException("Can't rename files");
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    throw new UnsupportedOperationException("Can't delete files");
  }

  @Override
  public FileStatus[] listStatus(Path inlinePath) throws FileNotFoundException, IOException {
    throw new UnsupportedOperationException("No support for listStatus");
  }

  @Override
  public void setWorkingDirectory(Path path) {
    throw new UnsupportedOperationException("Can't set working directory");
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    throw new UnsupportedOperationException("Can't mkdir");
  }

  @Override
  public boolean exists(Path f) throws IOException {
    throw new UnsupportedOperationException("Can't check for exists");
  }

  @Override
  public FileStatus getFileStatus(Path inlinePath) throws IOException {
    throw new UnsupportedOperationException("No support for getFileStatus");
  }

  @Override
  public void close() throws IOException {
    super.close();
    bos.close();
  }
}
