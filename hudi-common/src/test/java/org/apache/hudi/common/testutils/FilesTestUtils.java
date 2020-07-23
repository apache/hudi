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
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FilesTestUtils {

  private static void fakeMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    String parentPath = basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME;
    new File(parentPath).mkdirs();
    new File(parentPath + "/" + instantTime + suffix).createNewFile();
  }

  public static void fakeCommit(String basePath, String instantTime) throws IOException {
    fakeMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void fakeRequestedCommit(String basePath, String instantTime) throws IOException {
    fakeMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  public static void fakeInflightCommit(String basePath, String instantTime) throws IOException {
    fakeMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  public static void fakeDeltaCommit(String basePath, String instantTime) throws IOException {
    fakeMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void fakeRequestedDeltaCommit(String basePath, String instantTime) throws IOException {
    fakeMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
  }

  public static void fakeInflightDeltaCommit(String basePath, String instantTime) throws IOException {
    fakeMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  public static void fakeDataFile(String basePath, String partitionPath, String instantTime, String fileId)
      throws Exception {
    fakeDataFile(basePath, partitionPath, instantTime, fileId, 0);
  }

  public static void fakeDataFile(String basePath, String partitionPath, String instantTime, String fileId, long length)
      throws Exception {
    String parentPath = String.format("%s/%s", basePath, partitionPath);
    new File(parentPath).mkdirs();
    String path = String.format("%s/%s", parentPath, FSUtils.makeDataFileName(instantTime, "1-0-1", fileId));
    new File(path).createNewFile();
    new RandomAccessFile(path, "rw").setLength(length);
  }

  public static void fakeLogFile(String basePath, String partitionPath, String baseInstantTime, String fileId, int version)
      throws Exception {
    fakeLogFile(basePath, partitionPath, baseInstantTime, fileId, version, 0);
  }

  public static void fakeLogFile(String basePath, String partitionPath, String baseInstantTime, String fileId, int version, int length)
      throws Exception {
    String parentPath = String.format("%s/%s", basePath, partitionPath);
    new File(parentPath).mkdirs();
    String path = String.format("%s/%s", parentPath, FSUtils.makeLogFileName(fileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseInstantTime, version, "1-0-1"));
    new File(path).createNewFile();
    new RandomAccessFile(path, "rw").setLength(length);
  }

  public static String createMarkerFile(String basePath, String partitionPath, String instantTime, String fileID, IOType ioType)
      throws IOException {
    java.nio.file.Path folderPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    Files.createDirectories(folderPath);
    String markerFileName = String.format("%s_%s_%s%s%s.%s", fileID, "1-0-1", instantTime,
        HoodieFileFormat.PARQUET.getFileExtension(), HoodieTableMetaClient.MARKER_EXTN, ioType);
    java.nio.file.Path markerFilePath = folderPath.resolve(markerFileName);
    Files.createFile(markerFilePath);
    return markerFilePath.toAbsolutePath().toString();
  }
}
