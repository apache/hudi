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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopFileStatus;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToHadoopPath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePathInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HadoopFSUtils}
 */
public class TestHadoopFSUtils {
  @ParameterizedTest
  @ValueSource(strings = {
      "/a/b/c",
      "s3://bucket/partition=1%2F2%2F3",
      "hdfs://x/y/z.file#bar"
  })
  public void testPathConversion(String pathString) {
    // Hadoop Path -> StoragePath -> Hadoop Path
    Path path = new Path(pathString);
    StoragePath storagePath = convertToStoragePath(path);
    Path convertedPath = convertToHadoopPath(storagePath);
    assertEquals(path.toUri(), storagePath.toUri());
    assertEquals(path, convertedPath);

    // StoragePath -> Hadoop Path -> StoragePath
    storagePath = new StoragePath(pathString);
    path = convertToHadoopPath(storagePath);
    StoragePath convertedStoragePath = convertToStoragePath(path);
    assertEquals(storagePath.toUri(), path.toUri());
    assertEquals(storagePath, convertedStoragePath);
  }

  @ParameterizedTest
  @CsvSource({
      "/a/b/c,1000,false,1,1000000,1238493920",
      "/x/y/z,0,true,2,0,2002403203"
  })
  public void testFileStatusConversion(String path,
                                       long length,
                                       boolean isDirectory,
                                       short blockReplication,
                                       long blockSize,
                                       long modificationTime) {
    // FileStatus -> StoragePathInfo -> FileStatus
    FileStatus fileStatus = new FileStatus(
        length, isDirectory, blockReplication, blockSize, modificationTime, new Path(path));
    StoragePathInfo pathInfo = convertToStoragePathInfo(fileStatus);
    assertStoragePathInfo(
        pathInfo, path, length, isDirectory, blockReplication, blockSize, modificationTime);
    FileStatus convertedFileStatus = convertToHadoopFileStatus(pathInfo);
    assertFileStatus(
        convertedFileStatus, path, length, isDirectory, blockReplication, blockSize, modificationTime);

    // StoragePathInfo -> FileStatus -> StoragePathInfo
    pathInfo = new StoragePathInfo(
        new StoragePath(path), length, isDirectory, blockReplication, blockSize, modificationTime);
    fileStatus = convertToHadoopFileStatus(pathInfo);
    assertFileStatus(
        fileStatus, path, length, isDirectory, blockReplication, blockSize, modificationTime);
    StoragePathInfo convertedPathInfo = convertToStoragePathInfo(fileStatus);
    assertStoragePathInfo(
        convertedPathInfo, path, length, isDirectory, blockReplication, blockSize, modificationTime);
  }

  private void assertFileStatus(FileStatus fileStatus,
                                String path,
                                long length,
                                boolean isDirectory,
                                short blockReplication,
                                long blockSize,
                                long modificationTime) {
    assertEquals(new Path(path), fileStatus.getPath());
    assertEquals(length, fileStatus.getLen());
    assertEquals(isDirectory, fileStatus.isDirectory());
    assertEquals(!isDirectory, fileStatus.isFile());
    assertEquals(blockReplication, fileStatus.getReplication());
    assertEquals(blockSize, fileStatus.getBlockSize());
    assertEquals(modificationTime, fileStatus.getModificationTime());
  }

  private void assertStoragePathInfo(StoragePathInfo pathInfo,
                                     String path,
                                     long length,
                                     boolean isDirectory,
                                     short blockReplication,
                                     long blockSize,
                                     long modificationTime) {
    assertEquals(new StoragePath(path), pathInfo.getPath());
    assertEquals(length, pathInfo.getLength());
    assertEquals(isDirectory, pathInfo.isDirectory());
    assertEquals(!isDirectory, pathInfo.isFile());
    assertEquals(blockReplication, pathInfo.getBlockReplication());
    assertEquals(blockSize, pathInfo.getBlockSize());
    assertEquals(modificationTime, pathInfo.getModificationTime());
  }
}
