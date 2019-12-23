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

package org.apache.hudi.utilities.inline.fs;

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

/**
 * Enables storing any file (something that is written using using FileSystem apis) "inline" into another file
 * <p>
 * - Writing an inlined file at a given path, simply writes it to an in-memory buffer and returns it as byte[]
 * - Reading an inlined file at a given offset, length, read it out as if it were an independent file of that length
 * - Inlined path is of the form "inlinefs:///path/to/outer/file/<outer_file_scheme>/inline_file/?start_offset=<start_offset>&length=<length>
 * <p>
 * TODO: The reader/writer may try to use relative paths based on the inlinepath and it may not work. Need to handle
 * this gracefully eg. the parquet summary metadata reading. TODO: If this shows promise, also support directly writing
 * the inlined file to the underneath file without buffer
 */
public class InlineFileSystem extends FileSystem {

  public static final String SCHEME = "inlinefs";

  // TODO: this needs to be per path to support num_cores > 1, and we should release the buffer once done
  private ByteArrayOutputStream bos;
  private Configuration conf = null;

  InlineFileSystem() {
    bos = new ByteArrayOutputStream();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.conf = conf;
  }

  @Override
  public URI getUri() {
    return URI.create(getScheme());
  }

  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(Path inlinePath, int bufferSize) throws IOException {
    Path outerPath = InLineFSUtils.getOuterfilePathFromInlinePath(inlinePath, getScheme());
    FileSystem outerFs = outerPath.getFileSystem(conf);
    FSDataInputStream outerStream = outerFs.open(outerPath, bufferSize);
    return new InlineFsDataInputStream(InLineFSUtils.startOffset(inlinePath), outerStream, InLineFSUtils.length(inlinePath));
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                   Progressable progressable) throws IOException {
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
    return new FileStatus[] {getFileStatus(inlinePath)};
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
    return false;
  }

  @Override
  public boolean exists(Path f) throws IOException {
    try {
      return getFileStatus(f) != null;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path inlinePath) throws IOException {
    Path outerPath = InLineFSUtils.getOuterfilePathFromInlinePath(inlinePath, getScheme());
    FileSystem outerFs = outerPath.getFileSystem(conf);
    FileStatus status = outerFs.getFileStatus(outerPath);
    FileStatus toReturn = new FileStatus(InLineFSUtils.length(inlinePath), status.isDirectory(), status.getReplication(), status.getBlockSize(),
        status.getModificationTime(), status.getAccessTime(), status.getPermission(), status.getOwner(),
        status.getGroup(), inlinePath);
    return toReturn;
  }
}