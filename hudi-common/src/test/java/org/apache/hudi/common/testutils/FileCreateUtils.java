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
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class FileCreateUtils {

  private static void createMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.notExists(metaFilePath)) {
      Files.createFile(metaFilePath);
    }
  }

  public static void createCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void createRequestedCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  public static void createInflightCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  public static void createDeltaCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void createRequestedDeltaCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
  }

  public static void createInflightDeltaCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  public static void createDataFile(String basePath, String partitionPath, String instantTime, String fileId)
      throws Exception {
    createDataFile(basePath, partitionPath, instantTime, fileId, 0);
  }

  public static void createDataFile(String basePath, String partitionPath, String instantTime, String fileId, long length)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path dataFilePath = parentPath.resolve(FSUtils.makeDataFileName(instantTime, "1-0-1", fileId));
    if (Files.notExists(dataFilePath)) {
      Files.createFile(dataFilePath);
    }
    new RandomAccessFile(dataFilePath.toFile(), "rw").setLength(length);
  }

  public static void createLogFile(String basePath, String partitionPath, String baseInstantTime, String fileId, int version)
      throws Exception {
    createLogFile(basePath, partitionPath, baseInstantTime, fileId, version, 0);
  }

  public static void createLogFile(String basePath, String partitionPath, String baseInstantTime, String fileId, int version, int length)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path logFilePath = parentPath.resolve(FSUtils.makeLogFileName(fileId, HoodieFileFormat.HOODIE_LOG.getFileExtension(), baseInstantTime, version, "1-0-1"));
    if (Files.notExists(logFilePath)) {
      Files.createFile(logFilePath);
    }
    new RandomAccessFile(logFilePath.toFile(), "rw").setLength(length);
  }

  public static String createMarkerFile(String basePath, String partitionPath, String instantTime, String fileID, IOType ioType)
      throws IOException {
    Path folderPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    Files.createDirectories(folderPath);
    String markerFileName = String.format("%s_%s_%s%s%s.%s", fileID, "1-0-1", instantTime,
        HoodieFileFormat.PARQUET.getFileExtension(), HoodieTableMetaClient.MARKER_EXTN, ioType);
    Path markerFilePath = folderPath.resolve(markerFileName);
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  public static long getTotalMarkerFileCount(String basePath, String partitionPath, String instantTime, IOType ioType) throws IOException {
    Path markerDir = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    if (Files.notExists(markerDir)) {
      return 0;
    }
    return Files.list(markerDir).filter(p -> p.getFileName().toString()
        .endsWith(String.format("%s.%s", HoodieTableMetaClient.MARKER_EXTN, ioType))).count();
  }

  /**
   * Find total basefiles for passed in paths.
   */
  public static Map<String, Long> getBaseFileCountsForPaths(String basePath, FileSystem fs, String... paths) {
    Map<String, Long> toReturn = new HashMap<>();
    try {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(fs.getConf(), basePath, true);
      for (String path : paths) {
        TableFileSystemView.BaseFileOnlyView fileSystemView = new HoodieTableFileSystemView(metaClient,
            metaClient.getCommitsTimeline().filterCompletedInstants(), fs.globStatus(new org.apache.hadoop.fs.Path(path)));
        toReturn.put(path, fileSystemView.getLatestBaseFiles().count());
      }
      return toReturn;
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }
}
