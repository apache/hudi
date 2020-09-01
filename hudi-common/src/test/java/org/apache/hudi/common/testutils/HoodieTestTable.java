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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.FileCreateUtils.createCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createInflightDeltaCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createMarkerFile;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedCommit;
import static org.apache.hudi.common.testutils.FileCreateUtils.createRequestedDeltaCommit;

public class HoodieTestTable {

  private final String basePath;
  private final FileSystem fs;
  private HoodieTableMetaClient metaClient;
  private String currentInstantTime;

  private HoodieTestTable(String basePath, FileSystem fs, HoodieTableMetaClient metaClient) {
    ValidationUtils.checkArgument(Objects.equals(basePath, metaClient.getBasePath()));
    ValidationUtils.checkArgument(Objects.equals(fs, metaClient.getRawFs()));
    this.basePath = basePath;
    this.fs = fs;
    this.metaClient = metaClient;
  }

  public static HoodieTestTable of(HoodieTableMetaClient metaClient) {
    return new HoodieTestTable(metaClient.getBasePath(), metaClient.getRawFs(), metaClient);
  }

  public HoodieTestTable addRequestedCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addRequestedDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addInflightDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(basePath, instantTime);
    createInflightDeltaCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addCommit(String instantTime) throws Exception {
    createRequestedCommit(basePath, instantTime);
    createInflightCommit(basePath, instantTime);
    createCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable addDeltaCommit(String instantTime) throws Exception {
    createRequestedDeltaCommit(basePath, instantTime);
    createInflightDeltaCommit(basePath, instantTime);
    createDeltaCommit(basePath, instantTime);
    currentInstantTime = instantTime;
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return this;
  }

  public HoodieTestTable forCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable forDeltaCommit(String instantTime) {
    currentInstantTime = instantTime;
    return this;
  }

  public HoodieTestTable withMarkerFile(String partitionPath, IOType ioType) throws IOException {
    return withMarkerFile(partitionPath, UUID.randomUUID().toString(), ioType);
  }

  public HoodieTestTable withMarkerFile(String partitionPath, String fileId, IOType ioType) throws IOException {
    createMarkerFile(basePath, partitionPath, currentInstantTime, fileId, ioType);
    return this;
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, int num, IOType ioType) throws IOException {
    String[] fileIds = IntStream.range(0, num).mapToObj(i -> UUID.randomUUID().toString()).toArray(String[]::new);
    return withMarkerFiles(partitionPath, fileIds, ioType);
  }

  public HoodieTestTable withMarkerFiles(String partitionPath, String[] fileIds, IOType ioType) throws IOException {
    for (String fileId : fileIds) {
      createMarkerFile(basePath, partitionPath, currentInstantTime, fileId, ioType);
    }
    return this;
  }

  /**
   * Insert one base file to each of the given distinct partitions.
   *
   * @return A {@link Map} of partition and its newly inserted file's id.
   */
  public Map<String, String> withInserts(String... partitions) throws Exception {
    Map<String, String> partitionFileIdMap = new HashMap<>();
    for (String p : partitions) {
      String fileId = UUID.randomUUID().toString();
      FileCreateUtils.createDataFile(basePath, p, currentInstantTime, fileId);
      partitionFileIdMap.put(p, fileId);
    }
    return partitionFileIdMap;
  }

  public HoodieTestTable withUpdates(String partition, String... fileIds) throws Exception {
    for (String f : fileIds) {
      FileCreateUtils.createDataFile(basePath, partition, currentInstantTime, f);
    }
    return this;
  }

  public String withLogFile(String partitionPath) throws Exception {
    String fileId = UUID.randomUUID().toString();
    withLogFile(partitionPath, fileId);
    return fileId;
  }

  public HoodieTestTable withLogFile(String partitionPath, String fileId) throws Exception {
    return withLogFile(partitionPath, fileId, 0);
  }

  public HoodieTestTable withLogFile(String partitionPath, String fileId, int version) throws Exception {
    FileCreateUtils.createLogFile(basePath, partitionPath, currentInstantTime, fileId, version);
    return this;
  }

  public boolean filesExist(Map<String, String> partitionAndFileId, String instantTime) {
    return partitionAndFileId.entrySet().stream().allMatch(entry -> {
      String partition = entry.getKey();
      String fileId = entry.getValue();
      return fileExists(partition, instantTime, fileId);
    });
  }

  public boolean filesExist(String partition, String instantTime, String... fileIds) {
    return Arrays.stream(fileIds).allMatch(f -> fileExists(partition, instantTime, f));
  }

  public boolean fileExists(String partition, String instantTime, String fileId) {
    try {
      return fs.exists(new Path(Paths.get(basePath, partition,
          FSUtils.makeDataFileName(instantTime, "1-0-1", fileId)).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public boolean logFilesExist(String partition, String instantTime, String fileId, int... versions) {
    return Arrays.stream(versions).allMatch(v -> logFileExists(partition, instantTime, fileId, v));
  }

  public boolean logFileExists(String partition, String instantTime, String fileId, int version) {
    try {
      return fs.exists(new Path(Paths.get(basePath, partition,
          FSUtils.makeLogFileName(fileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(), instantTime, version, "1-0-1")).toString()));
    } catch (IOException e) {
      throw new HoodieTestTableException(e);
    }
  }

  public List<FileStatus> listAllFiles(String partitionPath) throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(Paths.get(basePath, partitionPath).toString()));
  }

  public List<FileStatus> listAllFilesInTempFolder() throws IOException {
    return FileSystemTestUtils.listRecursive(fs, new Path(Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME).toString()));
  }

  public static class HoodieTestTableException extends RuntimeException {
    public HoodieTestTableException(Throwable t) {
      super(t);
    }
  }
}
