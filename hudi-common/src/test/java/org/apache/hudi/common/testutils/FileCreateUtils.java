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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCleanMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCleanerPlan;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeRequestedReplaceMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeRollbackMetadata;

public class FileCreateUtils {

  private static final String WRITE_TOKEN = "1-0-1";

  public static String baseFileName(String instantTime, String fileId) {
    return baseFileName(instantTime, fileId, HoodieFileFormat.PARQUET.getFileExtension());
  }

  public static String baseFileName(String instantTime, String fileId, String fileExtension) {
    return FSUtils.makeDataFileName(instantTime, WRITE_TOKEN, fileId, fileExtension);
  }

  public static String logFileName(String instantTime, String fileId, int version) {
    return logFileName(instantTime, fileId, version, HoodieFileFormat.HOODIE_LOG.getFileExtension());
  }

  public static String logFileName(String instantTime, String fileId, int version, String fileExtension) {
    return FSUtils.makeLogFileName(fileId, fileExtension, instantTime, version, WRITE_TOKEN);
  }

  public static String markerFileName(String instantTime, String fileId, IOType ioType) {
    return markerFileName(instantTime, fileId, ioType, HoodieFileFormat.PARQUET.getFileExtension());
  }

  public static String markerFileName(String instantTime, String fileId, IOType ioType, String fileExtension) {
    return String.format("%s_%s_%s%s%s.%s", fileId, WRITE_TOKEN, instantTime, fileExtension, HoodieTableMetaClient.MARKER_EXTN, ioType);
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.notExists(metaFilePath)) {
      Files.createFile(metaFilePath);
    }
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix, FileSystem fs) throws IOException {
    org.apache.hadoop.fs.Path parentPath = new org.apache.hadoop.fs.Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    if (!fs.exists(parentPath)) {
      fs.create(parentPath).close();
    }
    org.apache.hadoop.fs.Path metaFilePath = new org.apache.hadoop.fs.Path(parentPath, instantTime + suffix);
    if (!fs.exists(metaFilePath)) {
      fs.create(metaFilePath).close();
    }
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix, byte[] content) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.notExists(metaFilePath)) {
      Files.write(metaFilePath, content);
    }
  }

  private static void deleteMetaFile(String basePath, String instantTime, String suffix, FileSystem fs) throws IOException {
    org.apache.hadoop.fs.Path parentPath = new org.apache.hadoop.fs.Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    org.apache.hadoop.fs.Path metaFilePath = new org.apache.hadoop.fs.Path(parentPath, instantTime + suffix);
    if (fs.exists(metaFilePath)) {
      fs.delete(metaFilePath, true);
    }
  }

  public static void createCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void createCommit(String basePath, String instantTime, HoodieCommitMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION, metadata.toJsonString().getBytes(StandardCharsets.UTF_8));
  }

  public static void createCommit(String basePath, String instantTime, FileSystem fs) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION, fs);
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

  public static void createDeltaCommit(String basePath, String instantTime, FileSystem fs) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, fs);
  }

  public static void createRequestedDeltaCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
  }

  public static void createInflightDeltaCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  public static void createReplaceCommit(String basePath, String instantTime, HoodieReplaceCommitMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION, metadata.toJsonString().getBytes(StandardCharsets.UTF_8));
  }

  public static void createRequestedReplaceCommit(String basePath, String instantTime, HoodieRequestedReplaceMetadata requestedReplaceMetadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION, serializeRequestedReplaceMetadata(requestedReplaceMetadata).get());
  }

  public static void createInflightReplaceCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
  }

  public static void createCleanFile(String basePath, String instantTime, HoodieCleanMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.CLEAN_EXTENSION, serializeCleanMetadata(metadata).get());
  }

  public static void createRequestedCleanFile(String basePath, String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_CLEAN_EXTENSION, serializeCleanerPlan(cleanerPlan).get());
  }

  public static void createInflightCleanFile(String basePath, String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION, serializeCleanerPlan(cleanerPlan).get());
  }

  public static void createInflightRollbackFile(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION);
  }

  public static void createRollbackFile(String basePath, String instantTime, HoodieRollbackMetadata rollbackMetadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.ROLLBACK_EXTENSION, serializeRollbackMetadata(rollbackMetadata).get());
  }

  private static void createAuxiliaryMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.AUXILIARYFOLDER_NAME);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.notExists(metaFilePath)) {
      Files.createFile(metaFilePath);
    }
  }

  public static void createRequestedCompaction(String basePath, String instantTime) throws IOException {
    createAuxiliaryMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION);
  }

  public static void createInflightCompaction(String basePath, String instantTime) throws IOException {
    createAuxiliaryMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  public static void createPartitionMetaFile(String basePath, String partitionPath) throws IOException {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE);
    if (Files.notExists(metaFilePath)) {
      Files.createFile(metaFilePath);
    }
  }

  public static void createBaseFile(String basePath, String partitionPath, String instantTime, String fileId)
      throws Exception {
    createBaseFile(basePath, partitionPath, instantTime, fileId, 0);
  }

  public static void createBaseFile(String basePath, String partitionPath, String instantTime, String fileId, long length)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path baseFilePath = parentPath.resolve(baseFileName(instantTime, fileId));
    if (Files.notExists(baseFilePath)) {
      Files.createFile(baseFilePath);
    }
    new RandomAccessFile(baseFilePath.toFile(), "rw").setLength(length);
  }

  public static void createLogFile(String basePath, String partitionPath, String instantTime, String fileId, int version)
      throws Exception {
    createLogFile(basePath, partitionPath, instantTime, fileId, version, 0);
  }

  public static void createLogFile(String basePath, String partitionPath, String instantTime, String fileId, int version, int length)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path logFilePath = parentPath.resolve(logFileName(instantTime, fileId, version));
    if (Files.notExists(logFilePath)) {
      Files.createFile(logFilePath);
    }
    new RandomAccessFile(logFilePath.toFile(), "rw").setLength(length);
  }

  public static String createMarkerFile(String basePath, String partitionPath, String instantTime, String fileId, IOType ioType)
      throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    Files.createDirectories(parentPath);
    Path markerFilePath = parentPath.resolve(markerFileName(instantTime, fileId, ioType));
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  public static long getTotalMarkerFileCount(String basePath, String partitionPath, String instantTime, IOType ioType) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    if (Files.notExists(parentPath)) {
      return 0;
    }
    return Files.list(parentPath).filter(p -> p.getFileName().toString()
        .endsWith(String.format("%s.%s", HoodieTableMetaClient.MARKER_EXTN, ioType))).count();
  }

  /**
   * Find total basefiles for passed in paths.
   */
  public static Map<String, Long> getBaseFileCountsForPaths(String basePath, FileSystem fs, String... paths) {
    Map<String, Long> toReturn = new HashMap<>();
    try {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
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

  public static void deleteDeltaCommit(String basePath, String instantTime, FileSystem fs) throws IOException {
    deleteMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, fs);
  }
}
