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

package org.apache.hudi.storage;

import org.apache.hudi.io.InlineSeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

public class InlineHoodieStorage<S extends SeekableDataInputStream> extends HoodieStorage {

  private static final String START_OFFSET_STR = "start_offset";
  private static final String LENGTH_STR = "length";
  private static final String SCHEME_SEPARATOR = "" + StoragePath.COLON_CHAR;
  private static final String EQUALS_STR = "=";
  private static final String LOCAL_FILESYSTEM_SCHEME = "file";

  public static final String SCHEME = "inlinefs";
  private final HoodieStorage fs;

  public InlineHoodieStorage(HoodieStorage fs) {
    this.fs = fs;
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public int getDefaultBlockSize(StoragePath path) {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public int getDefaultBufferSize() {
    return fs.getDefaultBufferSize();
  }

  @Override
  public short getDefaultReplication(StoragePath path) {
    return 0;
  }

  @Override
  public URI getUri() {
    return URI.create(getScheme());
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
    return null;
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite, Integer bufferSize, Short replication, Long sizeThreshold) throws IOException {
    return null;
  }

  @Override
  public InputStream open(StoragePath path) throws IOException {
    StoragePath outerPath = getOuterFilePathFromInlinePath(path);
    SeekableDataInputStream outerStream = fs.openSeekable(outerPath);
    InlineSeekableDataInputStream id = new InlineSeekableDataInputStream(startOffset(path), outerStream, length(path));
    id.re
    return fs.open(outerPath);
  }


  /**
   * Returns length of the block (embedded w/in the base file) identified by the given InlineFS path
   *
   * input: "inlinefs:/file1/s3a/?start_offset=20&length=40".
   * output: 40
   */
  private static long length(StoragePath inlinePath) {
    assertInlineFSPath(inlinePath);

    String[] slices = inlinePath.toString().split("[?&=]");
    return Long.parseLong(slices[slices.length - 1]);
  }

  /**
   * Returns start offset w/in the base for the block identified by the given InlineFS path
   *
   * input: "inlinefs://file1/s3a/?start_offset=20&length=40".
   * output: 20
   */
  private static long startOffset(StoragePath inlineFSPath) {
    assertInlineFSPath(inlineFSPath);

    String[] slices = inlineFSPath.toString().split("[?&=]");
    return Long.parseLong(slices[slices.length - 3]);
  }

  private static StoragePath getOuterFilePathFromInlinePath(StoragePath inlineFSPath) {
    assertInlineFSPath(inlineFSPath);

    final String outerFileScheme = inlineFSPath.getParent().getName();
    final StoragePath basePath = inlineFSPath.getParent().getParent();
    checkArgument(basePath.toString().contains(SCHEME_SEPARATOR),
        "Invalid InLineFS path: " + inlineFSPath);

    final String pathExceptScheme = basePath.toString().substring(basePath.toString().indexOf(SCHEME_SEPARATOR) + 1);
    final String fullPath = outerFileScheme + SCHEME_SEPARATOR
        + (outerFileScheme.equals(LOCAL_FILESYSTEM_SCHEME) ? StoragePath.SEPARATOR : "")
        + pathExceptScheme;
    return new StoragePath(fullPath);
  }

  private static void assertInlineFSPath(StoragePath inlinePath) {
    String scheme = inlinePath.toUri().getScheme();
    checkArgument(SCHEME.equals(scheme));
  }

  @Override
  public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize) throws IOException {
    return null;
  }

  @Override
  public OutputStream append(StoragePath path) throws IOException {
    return null;
  }

  @Override
  public boolean exists(StoragePath path) throws IOException {
    return false;
  }

  @Override
  public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
    return null;
  }

  @Override
  public boolean createDirectory(StoragePath path) throws IOException {
    return false;
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException {
    return null;
  }

  @Override
  public List<StoragePathInfo> listFiles(StoragePath path) throws IOException {
    return null;
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path, StoragePathFilter filter) throws IOException {
    return null;
  }

  @Override
  public List<StoragePathInfo> globEntries(StoragePath pathPattern, StoragePathFilter filter) throws IOException {
    return null;
  }

  @Override
  public boolean rename(StoragePath oldPath, StoragePath newPath) throws IOException {
    return false;
  }

  @Override
  public boolean deleteDirectory(StoragePath path) throws IOException {
    return false;
  }

  @Override
  public boolean deleteFile(StoragePath path) throws IOException {
    return false;
  }

  @Override
  public StoragePath makeQualified(StoragePath path) {
    return null;
  }

  @Override
  public Object getFileSystem() {
    return null;
  }

  @Override
  public Object getConf() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
