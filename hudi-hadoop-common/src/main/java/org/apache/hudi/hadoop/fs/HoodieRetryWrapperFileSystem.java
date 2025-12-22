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

import org.apache.hudi.common.util.RetryHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

/**
 * Extending {@link FileSystem} implementation of I/O operations with retries.
 */
public class HoodieRetryWrapperFileSystem extends FileSystem {

  private FileSystem fileSystem;
  private long maxRetryIntervalMs;
  private int maxRetryNumbers;
  private long initialRetryIntervalMs;
  private String retryExceptionsList;

  public HoodieRetryWrapperFileSystem(FileSystem fs, long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions) {
    this.fileSystem = fs;
    this.maxRetryIntervalMs = maxRetryIntervalMs;
    this.maxRetryNumbers = maxRetryNumbers;
    this.initialRetryIntervalMs = initialRetryIntervalMs;
    this.retryExceptionsList = retryExceptions;

  }

  @Override
  public URI getUri() {
    return fileSystem.getUri();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new RetryHelper<FSDataInputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.open(f, bufferSize)).start();
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    return new RetryHelper<FSDataInputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.open(f)).start();
  }

  @Override
  public FSDataOutputStream create(Path f,
                                   FsPermission permission,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.create(f, overwrite)).start();
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.create(f)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.create(f, progress)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, short replication) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.create(f, replication)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, replication, progress)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, overwrite, bufferSize)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
      throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, overwrite, bufferSize, progress)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, overwrite, bufferSize, replication, blockSize, progress)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, permission, flags, bufferSize, replication, blockSize, progress)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
                                   short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, permission, flags, bufferSize, replication,
            blockSize, progress, checksumOpt)).start();
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
      throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.create(f, overwrite, bufferSize, replication, blockSize)).start();
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    return new RetryHelper<Boolean, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.createNewFile(f)).start().booleanValue();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.append(f, bufferSize, progress)).start();
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.append(f)).start();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return new RetryHelper<FSDataOutputStream, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.append(f, bufferSize)).start();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return new RetryHelper<Boolean, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.rename(src, dst)).start().booleanValue();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return new RetryHelper<Boolean, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.delete(f, recursive)).start().booleanValue();
  }

  @Override
  public boolean delete(Path f) throws IOException {
    return new RetryHelper<Boolean, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.delete(f, true)).start().booleanValue();
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return new RetryHelper<FileStatus[], IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.listStatus(f)).start();
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
    return new RetryHelper<FileStatus[], IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.listStatus(f, filter)).start();
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws IOException {
    return new RetryHelper<FileStatus[], IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.listStatus(files)).start();
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException {
    return new RetryHelper<FileStatus[], IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.listStatus(files, filter)).start();
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return new RetryHelper<FileStatus[], IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.globStatus(pathPattern)).start();
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    return new RetryHelper<FileStatus[], IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.globStatus(pathPattern, filter)).start();
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    return new RetryHelper<RemoteIterator<LocatedFileStatus>, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.listLocatedStatus(f)).start();
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws IOException {
    return new RetryHelper<RemoteIterator<LocatedFileStatus>, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.listFiles(f, recursive)).start();
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    fileSystem.setWorkingDirectory(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return fileSystem.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return new RetryHelper<Boolean, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList)
        .tryWith(() -> fileSystem.mkdirs(f, permission)).start().booleanValue();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return new RetryHelper<FileStatus, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.getFileStatus(f)).start();
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return new RetryHelper<Boolean, IOException>(maxRetryIntervalMs, maxRetryNumbers, initialRetryIntervalMs, retryExceptionsList).tryWith(() -> fileSystem.exists(f)).start().booleanValue();
  }

  @Override
  public Configuration getConf() {
    return fileSystem.getConf();
  }

  @Override
  public String getScheme() {
    return fileSystem.getScheme();
  }

  @Override
  public short getDefaultReplication() {
    return fileSystem.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    return fileSystem.getDefaultReplication(path);
  }

}
