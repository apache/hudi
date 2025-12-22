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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

/**
 * Enables reading any inline file at a given offset and length. This {@link FileSystem} is used only in read path and does not support
 * any write apis.
 * <p>
 * - Reading an inlined file at a given offset, length, read it out as if it were an independent file of that length
 * - Inlined path is of the form "inlinefs:///path/to/outer/file/<outer_file_scheme>/?start_offset=<start_offset>&length=<length>
 * <p>
 * TODO: The reader/writer may try to use relative paths based on the inlinepath and it may not work. Need to handle
 * this gracefully eg. the parquet summary metadata reading. TODO: If this shows promise, also support directly writing
 * the inlined file to the underneath file without buffer
 */
public class InLineFileSystem extends FileSystem {

  public static final String SCHEME = InLineFSUtils.SCHEME;
  private Configuration conf = null;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    this.conf = conf;
  }

  @Override
  public URI getUri() {
    return URI.create(getScheme());
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public FSDataInputStream open(Path inlinePath, int bufferSize) throws IOException {
    Path outerPath = HadoopInLineFSUtils.getOuterFilePathFromInlinePath(inlinePath);
    FileSystem outerFs = outerPath.getFileSystem(conf);
    FSDataInputStream outerStream = outerFs.open(outerPath, bufferSize);
    StoragePath inlineStoragePath = convertToStoragePath(inlinePath);
    return new InLineFsDataInputStream(HadoopInLineFSUtils.startOffset(inlineStoragePath), outerStream, HadoopInLineFSUtils.length(inlineStoragePath));
  }

  @Override
  public boolean exists(Path f) {
    try {
      return getFileStatus(f) != null;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path inlinePath) throws IOException {
    Path outerPath = HadoopInLineFSUtils.getOuterFilePathFromInlinePath(inlinePath);
    FileSystem outerFs = outerPath.getFileSystem(conf);
    FileStatus status = outerFs.getFileStatus(outerPath);
    FileStatus toReturn = new FileStatus(HadoopInLineFSUtils.length(convertToStoragePath(inlinePath)), status.isDirectory(), status.getReplication(), status.getBlockSize(),
        status.getModificationTime(), status.getAccessTime(), status.getPermission(), status.getOwner(),
        status.getGroup(), inlinePath);
    return toReturn;
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                   Progressable progressable) throws IOException {
    throw new UnsupportedOperationException("Can't rename files");
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException("Can't rename files");
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
  public FileStatus[] listStatus(Path inlinePath) throws IOException {
    return new FileStatus[] {getFileStatus(inlinePath)};
  }

  @Override
  public void setWorkingDirectory(Path path) {
    throw new UnsupportedOperationException("Can't set working directory");
  }

  @Override
  public Path getWorkingDirectory() {
    throw new UnsupportedOperationException("Can't get working directory");
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    throw new UnsupportedOperationException("Can't set working directory");
  }
}