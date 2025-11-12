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
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HadoopSeekableDataInputStream;
import org.apache.hudi.hadoop.fs.HoodieRetryWrapperFileSystem;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.conf.Configuration;
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

  public HoodieHadoopStorage(StoragePath path, StorageConfiguration<?> conf) {
    super(conf);
    this.fs = HadoopFSUtils.getFs(path, conf.unwrapAs(Configuration.class));
  }

  public HoodieHadoopStorage(Path path, Configuration conf) {
    super(HadoopFSUtils.getStorageConf(conf));
    this.fs = HadoopFSUtils.getFs(path, conf);
  }

  public HoodieHadoopStorage(String path, Configuration conf) {
    super(HadoopFSUtils.getStorageConf(conf));
    this.fs = HadoopFSUtils.getFs(path, conf);
  }

  public HoodieHadoopStorage(String path, StorageConfiguration<?> conf) {
    super(conf);
    this.fs = HadoopFSUtils.getFs(path, conf);
  }

  public HoodieHadoopStorage(StoragePath path,
                             StorageConfiguration<?> conf,
                             boolean enableRetry,
                             long maxRetryIntervalMs,
                             int maxRetryNumbers,
                             long initialRetryIntervalMs,
                             String retryExceptions,
                             ConsistencyGuard consistencyGuard) {
    super(conf);
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
    super(new HadoopStorageConfiguration(fs.getConf()));
    this.fs = fs;
  }

  @Override
  public HoodieStorage newInstance(StoragePath path, StorageConfiguration<?> storageConf) {
    return new HoodieHadoopStorage(path, storageConf);
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
  public void setModificationTime(StoragePath path, long modificationTimeInMillisEpoch) throws IOException {
    fs.setTimes(HadoopFSUtils.convertToHadoopPath(path), modificationTimeInMillisEpoch, modificationTimeInMillisEpoch);
  }

  @Override
  public List<StoragePathInfo> listDirectEntries(List<StoragePath> pathList,
                                                 StoragePathFilter filter) throws IOException {
    return Arrays.stream(fs.listStatus(
            pathList.stream()
                .map(HadoopFSUtils::convertToHadoopPath)
                .toArray(Path[]::new),
            e -> filter.accept(convertToStoragePath(e))))
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
    return delete(path, true);

  }

  @Override
  public boolean deleteFile(StoragePath path) throws IOException {
    return delete(path, false);
  }

  private boolean delete(StoragePath path, boolean recursive) throws IOException {
    Path hadoopPath = convertToHadoopPath(path);
    boolean success = fs.delete(hadoopPath, recursive);
    if (!success) {
      if (fs.exists(hadoopPath)) {
        throw new HoodieIOException("Failed to delete invalid data file: " + path);
      }
    }
    return success;
  }

  @Override
  public Object getFileSystem() {
    return fs;
  }

  @Override
  public HoodieStorage getRawStorage() {
    if (fs instanceof HoodieWrapperFileSystem) {
      return new HoodieHadoopStorage(((HoodieWrapperFileSystem) fs).getFileSystem());
    } else {
      return this;
    }
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
    // Don't close the wrapped `FileSystem` object.
    // This will end up closing it for every thread since it
    // could be cached across JVM. We don't own that object anyway.
  }
}
