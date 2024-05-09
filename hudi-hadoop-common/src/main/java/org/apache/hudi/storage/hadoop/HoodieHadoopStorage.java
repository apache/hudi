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

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HadoopSeekableDataInputStream;
import org.apache.hudi.hadoop.fs.HadoopSeekableDataOutputStream;
import org.apache.hudi.hadoop.fs.HoodieRetryWrapperFileSystem;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.io.SeekableDataOutputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePathInfo;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getFs;

/**
 * Implementation of {@link HoodieStorage} using Hadoop's {@link FileSystem}
 */
public class HoodieHadoopStorage extends HoodieStorage {
  private final FileSystem fs;

  public HoodieHadoopStorage(HoodieStorage storage) {
    FileSystem fs = (FileSystem) storage.getFileSystem();
    if (fs instanceof HoodieWrapperFileSystem) {
      this.fs = ((HoodieWrapperFileSystem) fs).getFileSystem();
    } else {
      this.fs = fs;
    }
  }

  public HoodieHadoopStorage(String basePath, Configuration conf) {
    this(HadoopFSUtils.getFs(basePath, conf));
  }

  public HoodieHadoopStorage(StoragePath path, StorageConfiguration<?> conf) {
    this(HadoopFSUtils.getFs(path, conf.unwrapAs(Configuration.class)));
  }

  public HoodieHadoopStorage(String basePath, StorageConfiguration<?> conf) {
    this(HadoopFSUtils.getFs(basePath, conf));
  }

  public HoodieHadoopStorage(StoragePath path,
                             StorageConfiguration<?> conf,
                             boolean enableRetry,
                             long maxRetryIntervalMs,
                             int maxRetryNumbers,
                             long initialRetryIntervalMs,
                             String retryExceptions,
                             ConsistencyGuard consistencyGuard) {
    FileSystem fileSystem = getFs(path, conf.unwrapCopyAs(Configuration.class));

    if (enableRetry) {
      fileSystem = new HoodieRetryWrapperFileSystem(fileSystem,
          maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptions);
    }
    checkArgument(!(fileSystem instanceof HoodieWrapperFileSystem),
        "File System not expected to be that of HoodieWrapperFileSystem");
    this.fs = new HoodieWrapperFileSystem(fileSystem, consistencyGuard);
  }

  public HoodieHadoopStorage(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public String getScheme() {
    return fs.getScheme();
  }

  @Override
  public URI getUri() {
    return fs.getUri();
  }

  @Override
  public int getDefaultBlockSize(StoragePath path) {
    return (int) fs.getDefaultBlockSize(convertToHadoopPath(path));
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite) throws IOException {
    return fs.create(convertToHadoopPath(path), overwrite);
  }

  @Override
  public OutputStream create(StoragePath path, boolean overwrite, Integer bufferSize, Short replication, Long sizeThreshold) throws IOException {
    return fs.create(convertToHadoopPath(path), false, bufferSize, replication, sizeThreshold, null);
  }

  @Override
  public int getDefaultBufferSize() {
    return fs.getConf().getInt("io.file.buffer.size", 4096);
  }

  @Override
  public short getDefaultReplication(StoragePath path) {
    return fs.getDefaultReplication(convertToHadoopPath(path));
  }

  @Override
  public InputStream open(StoragePath path) throws IOException {
    return fs.open(convertToHadoopPath(path));
  }

  @Override
  public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream) throws IOException {
    return new HadoopSeekableDataInputStream(
        HadoopFSUtils.getFSDataInputStream(fs, path, bufferSize, wrapStream));
  }

  @Override
  public SeekableDataOutputStream makeOutputSeekable(OutputStream stream) throws IOException {
    return new HadoopSeekableDataOutputStream(new FSDataOutputStream(stream, null));
  }

  @Override
  public OutputStream append(StoragePath path) throws IOException {
    return fs.append(convertToHadoopPath(path));
  }

  @Override
  public boolean exists(StoragePath path) throws IOException {
    return fs.exists(convertToHadoopPath(path));
  }

  @Override
  public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
    return convertToStoragePathInfo(fs.getFileStatus(convertToHadoopPath(path)));
  }

  @Override
  public boolean createDirectory(StoragePath path) throws IOException {
    return fs.mkdirs(convertToHadoopPath(path));
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException {
    return Arrays.stream(fs.listStatus(convertToHadoopPath(path)))
        .map(HadoopFSUtils::convertToStoragePathInfo)
        .collect(Collectors.toList());
  }

  @Override
  public List<StoragePathInfo> listFiles(StoragePath path) throws IOException {
    List<StoragePathInfo> result = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(convertToHadoopPath(path), true);
    while (iterator.hasNext()) {
      result.add(convertToStoragePathInfo(iterator.next()));
    }
    return result;
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(List<StoragePath> pathList) throws IOException {
    return Arrays.stream(fs.listStatus(pathList.stream()
            .map(HadoopFSUtils::convertToHadoopPath)
            .toArray(Path[]::new)))
        .map(HadoopFSUtils::convertToStoragePathInfo)
        .collect(Collectors.toList());
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(StoragePath path,
                                                 StoragePathFilter filter)
      throws IOException {
    return Arrays.stream(fs.listStatus(
            convertToHadoopPath(path), e ->
                filter.accept(convertToStoragePath(e))))
        .map(HadoopFSUtils::convertToStoragePathInfo)
        .collect(Collectors.toList());
  }

  @Override
  public List<StoragePathInfo> globEntries(StoragePath pathPattern)
      throws IOException {
    return Arrays.stream(fs.globStatus(convertToHadoopPath(pathPattern)))
        .map(HadoopFSUtils::convertToStoragePathInfo)
        .collect(Collectors.toList());
  }

  @Override
  public List<StoragePathInfo> globEntries(StoragePath pathPattern, StoragePathFilter filter)
      throws IOException {
    return Arrays.stream(fs.globStatus(convertToHadoopPath(pathPattern), path ->
            filter.accept(convertToStoragePath(path))))
        .map(HadoopFSUtils::convertToStoragePathInfo)
        .collect(Collectors.toList());
  }

  @Override
  public boolean rename(StoragePath oldPath, StoragePath newPath) throws IOException {
    return fs.rename(convertToHadoopPath(oldPath), convertToHadoopPath(newPath));
  }

  @Override
  public boolean deleteDirectory(StoragePath path) throws IOException {
    return fs.delete(convertToHadoopPath(path), true);
  }

  @Override
  public boolean deleteFile(StoragePath path) throws IOException {
    return fs.delete(convertToHadoopPath(path), false);
  }

  @Override
  public StoragePath makeQualified(StoragePath path) {
    return convertToStoragePath(
        fs.makeQualified(convertToHadoopPath(path)));
  }

  @Override
  public Object getFileSystem() {
    return fs;
  }

  @Override
  public StorageConfiguration<Configuration> getConf() {
    return new HadoopStorageConfiguration(fs.getConf());
  }

  @Override
  public Configuration unwrapConf() {
    return fs.getConf();
  }

  @Override
  public OutputStream create(StoragePath path) throws IOException {
    return fs.create(convertToHadoopPath(path));
  }

  @Override
  public boolean createNewFile(StoragePath path) throws IOException {
    return fs.createNewFile(convertToHadoopPath(path));
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }
}
