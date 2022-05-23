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
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCleanMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCleanerPlan;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCompactionPlan;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeRequestedReplaceMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeRestoreMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeRollbackMetadata;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeRollbackPlan;

public class FileCreateUtils {

  private static final Logger LOG = LogManager.getLogger(FileCreateUtils.class);

  private static final String WRITE_TOKEN = "1-0-1";
  private static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
  /** An empty byte array */
  public static final byte[] EMPTY_BYTES = new byte[0];

  public static String baseFileName(String instantTime, String fileId) {
    return baseFileName(instantTime, fileId, BASE_FILE_EXTENSION);
  }

  public static String baseFileName(String instantTime, String fileId, String fileExtension) {
    return FSUtils.makeBaseFileName(instantTime, WRITE_TOKEN, fileId, fileExtension);
  }

  public static String logFileName(String instantTime, String fileId, int version) {
    return logFileName(instantTime, fileId, version, HoodieFileFormat.HOODIE_LOG.getFileExtension());
  }

  public static String logFileName(String instantTime, String fileId, int version, String fileExtension) {
    return FSUtils.makeLogFileName(fileId, fileExtension, instantTime, version, WRITE_TOKEN);
  }

  public static String markerFileName(String instantTime, String fileId, IOType ioType) {
    return markerFileName(instantTime, fileId, ioType, BASE_FILE_EXTENSION);
  }

  public static String markerFileName(String instantTime, String fileId, IOType ioType, String fileExtension) {
    return String.format("%s_%s_%s%s%s.%s", fileId, WRITE_TOKEN, instantTime, fileExtension, HoodieTableMetaClient.MARKER_EXTN, ioType);
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

  private static void createMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    createMetaFile(basePath, instantTime, suffix, "".getBytes());
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix, byte[] content) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.notExists(metaFilePath)) {
      if (content.length == 0) {
        Files.createFile(metaFilePath);
      } else {
        Files.write(metaFilePath, content);
      }
    }
  }

  private static void deleteMetaFile(String basePath, String instantTime, String suffix, FileSystem fs) throws IOException {
    org.apache.hadoop.fs.Path parentPath = new org.apache.hadoop.fs.Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    org.apache.hadoop.fs.Path metaFilePath = new org.apache.hadoop.fs.Path(parentPath, instantTime + suffix);
    if (fs.exists(metaFilePath)) {
      fs.delete(metaFilePath, true);
    }
  }

  private static void deleteMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.exists(metaFilePath)) {
      Files.delete(metaFilePath);
    }
  }

  public static void createCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void createCommit(String basePath, String instantTime, Option<HoodieCommitMetadata> metadata) throws IOException {
    if (metadata.isPresent()) {
      createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION,
          metadata.get().toJsonString().getBytes(StandardCharsets.UTF_8));
    } else {
      createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
    }
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

  public static void createDeltaCommit(String basePath, String instantTime, HoodieCommitMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, metadata.toJsonString().getBytes(StandardCharsets.UTF_8));
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

  public static void createInflightReplaceCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
  }

  public static void createReplaceCommit(String basePath, String instantTime, HoodieReplaceCommitMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION, metadata.toJsonString().getBytes(StandardCharsets.UTF_8));
  }

  public static void createRequestedReplaceCommit(String basePath, String instantTime, Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata) throws IOException {
    if (requestedReplaceMetadata.isPresent()) {
      createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION, serializeRequestedReplaceMetadata(requestedReplaceMetadata.get()).get());
    } else {
      createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION);
    }
  }

  public static void createInflightReplaceCommit(String basePath, String instantTime, Option<HoodieCommitMetadata> inflightReplaceMetadata) throws IOException {
    if (inflightReplaceMetadata.isPresent()) {
      createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION, inflightReplaceMetadata.get().toJsonString().getBytes(StandardCharsets.UTF_8));
    } else {
      createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
    }
  }

  public static void createRequestedCompactionCommit(String basePath, String instantTime, HoodieCompactionPlan requestedCompactionPlan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION, serializeCompactionPlan(requestedCompactionPlan).get());
  }

  public static void createCleanFile(String basePath, String instantTime, HoodieCleanMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.CLEAN_EXTENSION, serializeCleanMetadata(metadata).get());
  }

  public static void createCleanFile(String basePath, String instantTime, HoodieCleanMetadata metadata, boolean isEmpty) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.CLEAN_EXTENSION, isEmpty ? EMPTY_BYTES : serializeCleanMetadata(metadata).get());
  }

  public static void createRequestedCleanFile(String basePath, String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_CLEAN_EXTENSION, serializeCleanerPlan(cleanerPlan).get());
  }

  public static void createRequestedCleanFile(String basePath, String instantTime, HoodieCleanerPlan cleanerPlan, boolean isEmpty) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_CLEAN_EXTENSION, isEmpty ? EMPTY_BYTES : serializeCleanerPlan(cleanerPlan).get());
  }

  public static void createInflightCleanFile(String basePath, String instantTime, HoodieCleanerPlan cleanerPlan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION, serializeCleanerPlan(cleanerPlan).get());
  }

  public static void createInflightCleanFile(String basePath, String instantTime, HoodieCleanerPlan cleanerPlan, boolean isEmpty) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION, isEmpty ? EMPTY_BYTES : serializeCleanerPlan(cleanerPlan).get());
  }

  public static void createRequestedRollbackFile(String basePath, String instantTime, HoodieRollbackPlan plan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION, serializeRollbackPlan(plan).get());
  }

  public static void createRequestedRollbackFile(String basePath, String instantTime, byte[] content) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION, content);
  }

  public static void createRequestedRollbackFile(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION);
  }

  public static void createInflightRollbackFile(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION);
  }

  public static void createRollbackFile(String basePath, String instantTime, HoodieRollbackMetadata hoodieRollbackMetadata, boolean isEmpty) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.ROLLBACK_EXTENSION, isEmpty ? EMPTY_BYTES : serializeRollbackMetadata(hoodieRollbackMetadata).get());
  }

  public static void createRestoreFile(String basePath, String instantTime, HoodieRestoreMetadata hoodieRestoreMetadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.RESTORE_ACTION, serializeRestoreMetadata(hoodieRestoreMetadata).get());
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

  public static void createPendingInflightCompaction(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  public static void createPartitionMetaFile(String basePath, String partitionPath) throws IOException {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path metaFilePath = parentPath.resolve(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);
    if (Files.notExists(metaFilePath)) {
      Files.createFile(metaFilePath);
    }
  }

  public static void createBaseFile(String basePath, String partitionPath, String instantTime, String fileId)
      throws Exception {
    createBaseFile(basePath, partitionPath, instantTime, fileId, 1);
  }

  public static void createBaseFile(String basePath, String partitionPath, String instantTime, String fileId, long length)
      throws Exception {
    createBaseFile(basePath, partitionPath, instantTime, fileId, length, Instant.now().toEpochMilli());
  }

  public static void createBaseFile(String basePath, String partitionPath, String instantTime, String fileId, long length, long lastModificationTimeMilli)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
    Files.createDirectories(parentPath);
    Path baseFilePath = parentPath.resolve(baseFileName(instantTime, fileId));
    if (Files.notExists(baseFilePath)) {
      Files.createFile(baseFilePath);
    }
    new RandomAccessFile(baseFilePath.toFile(), "rw").setLength(length);
    Files.setLastModifiedTime(baseFilePath, FileTime.fromMillis(lastModificationTimeMilli));
  }

  public static Path getBaseFilePath(String basePath, String partitionPath, String instantTime, String fileId) {
    Path parentPath = Paths.get(basePath, partitionPath);
    return parentPath.resolve(baseFileName(instantTime, fileId));
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

  private static void removeMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Path metaFilePath = parentPath.resolve(instantTime + suffix);
    if (Files.exists(metaFilePath)) {
      Files.delete(metaFilePath);
    }
  }

  public static void deleteCommit(String basePath, String instantTime) throws IOException {
    removeMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void deleteRequestedCommit(String basePath, String instantTime) throws IOException {
    removeMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  public static void deleteInflightCommit(String basePath, String instantTime) throws IOException {
    removeMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  public static void deleteDeltaCommit(String basePath, String instantTime) throws IOException {
    removeMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void deleteReplaceCommit(String basePath, String instantTime) throws IOException {
    removeMetaFile(basePath, instantTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION);
  }

  public static void deleteRollbackCommit(String basePath, String instantTime) throws IOException {
    removeMetaFile(basePath, instantTime, HoodieTimeline.ROLLBACK_EXTENSION);
  }

  public static java.nio.file.Path renameFileToTemp(java.nio.file.Path sourcePath, String instantTime) throws IOException {
    java.nio.file.Path dummyFilePath = sourcePath.getParent().resolve(instantTime + ".temp");
    Files.move(sourcePath, dummyFilePath);
    return dummyFilePath;
  }

  public static void renameTempToMetaFile(java.nio.file.Path tempFilePath, java.nio.file.Path destPath) throws IOException {
    Files.move(tempFilePath, destPath);
  }

  public static long getTotalMarkerFileCount(String basePath, String partitionPath, String instantTime, IOType ioType) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    if (Files.notExists(parentPath)) {
      return 0;
    }
    return Files.list(parentPath).filter(p -> p.getFileName().toString()
        .endsWith(String.format("%s.%s", HoodieTableMetaClient.MARKER_EXTN, ioType))).count();
  }

  public static List<Path> getPartitionPaths(Path basePath) throws IOException {
    if (Files.notExists(basePath)) {
      return Collections.emptyList();
    }
    return Files.list(basePath).filter(entry -> !entry.getFileName().toString().equals(HoodieTableMetaClient.METAFOLDER_NAME)
            && !isBaseOrLogFilename(entry.getFileName().toString())
            && !entry.getFileName().toString().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX))
        .collect(Collectors.toList());
  }

  public static boolean isBaseOrLogFilename(String filename) {
    for (HoodieFileFormat format : HoodieFileFormat.values()) {
      if (filename.contains(format.getFileExtension())) {
        return true;
      }
    }
    return false;
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
