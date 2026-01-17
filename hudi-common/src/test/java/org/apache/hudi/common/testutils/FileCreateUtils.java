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
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utils for creating dummy Hudi files in testing.
 * TODO[Davis Zhang]: For all function calls with preTableVersion8, they should be derive directly by consulting metaClient. Remove the
 * parameter and revise all call sites properly.
 */
public class FileCreateUtils extends FileCreateUtilsBase {

  private static void createMetaFile(HoodieTableMetaClient metaClient, String instantTime, String suffix,
                                     HoodieStorage storage) throws IOException {
    createMetaFile(metaClient, instantTime, suffix, storage, metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT));
  }

  private static void createMetaFile(HoodieTableMetaClient metaClient, String instantTime, String suffix,
                                     HoodieStorage storage, boolean preTableVersion8) throws IOException {
    createMetaFileInMetaPath(metaClient.getMetaPath(), instantTime, suffix, storage, preTableVersion8);
  }

  private static void createMetaFile(HoodieTableMetaClient metaClient, String instantTime, String suffix) throws IOException {
    createMetaFile(metaClient, instantTime, Option.empty(), suffix, Option.empty());
  }

  private static <T> void createMetaFile(
      HoodieTableMetaClient metaClient, String instantTime, Option<String> completeTime, String suffix, Option<T> metadata) throws IOException {
    createMetaFileInTimelinePath(metaClient, instantTime, completeTime.isEmpty() ? InProcessTimeGenerator::createNewInstantTime : completeTime::get, suffix,
        metadata.flatMap(m -> metaClient.getCommitMetadataSerDe().getInstantWriter(m)));
  }

  private static <T> void createMetaFile(HoodieTableMetaClient metaClient, String instantTime, String suffix, Option<T> metadata) throws IOException {
    createMetaFileInTimelinePath(metaClient, instantTime, InProcessTimeGenerator::createNewInstantTime, suffix,
        metadata.flatMap(m -> metaClient.getCommitMetadataSerDe().getInstantWriter(m)));
  }

  public static String markerFileName(String fileName, IOType ioType) {
    return String.format("%s%s.%s", fileName, HoodieTableMetaClient.MARKER_EXTN, ioType.name());
  }

  public static String dataFileMarkerFileName(String instantTime, String fileId, IOType ioType, String fileExtension, String writeToken) {
    return markerFileName(FSUtils.makeBaseFileName(instantTime, writeToken, fileId, fileExtension), ioType);
  }

  public static String logFileMarkerFileName(String instantTime, String fileId, IOType ioType, int logVersion) {
    return logFileMarkerFileName(instantTime, fileId, ioType, HoodieLogFile.DELTA_EXTENSION, logVersion);
  }

  public static String logFileMarkerFileName(String instantTime, String fileId, IOType ioType, String fileExtension, int logVersion) {
    return markerFileName(FSUtils.makeLogFileName(fileId, fileExtension, instantTime, logVersion, WRITE_TOKEN), ioType);
  }

  private static void deleteMetaFile(HoodieTableMetaClient metaClient, String instantTime, String suffix,
                                     HoodieStorage storage) throws IOException {
    deleteMetaFileInTimeline(metaClient.getTimelinePath().toUri().getPath(), instantTime, suffix, storage);
  }

  public static void createCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe, String instantTime,
                                  Option<HoodieCommitMetadata> metadata) throws IOException {
    createCommit(metaClient, commitMetadataSerDe, instantTime, Option.empty(), metadata);
  }

  public static void createCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe, String instantTime,
                                  Option<String> completionTime, Option<HoodieCommitMetadata> metadata) throws IOException {
    final Supplier<String> completionTimeSupplier = () -> completionTime.isPresent() ? completionTime.get() : InProcessTimeGenerator.createNewInstantTime();
    if (metadata.isPresent()) {
      HoodieCommitMetadata commitMetadata = metadata.get();
      createMetaFileInTimelinePath(metaClient, instantTime, completionTimeSupplier, HoodieTimeline.COMMIT_EXTENSION,
          metaClient.getCommitMetadataSerDe().getInstantWriter(commitMetadata));
    } else {
      createMetaFileInTimelinePath(metaClient, instantTime, completionTimeSupplier, HoodieTimeline.COMMIT_EXTENSION, Option.empty());
    }
  }

  public static void createSavepointCommit(HoodieTableMetaClient metaClient, String instantTime, Option<String> completeTime,
                                           HoodieSavepointMetadata savepointMetadata)
      throws IOException {
    createMetaFile(metaClient, instantTime, completeTime, HoodieTimeline.SAVEPOINT_EXTENSION, Option.of(savepointMetadata));
  }

  public static void createCommit(HoodieTableMetaClient metaClient, String instantTime)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void createRequestedCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  public static void createInflightCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  public static void createDeltaCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe, String instantTime, Option<String> completeTime,
                                       HoodieCommitMetadata metadata) throws IOException {
    createMetaFile(metaClient, instantTime, completeTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, Option.of(metadata));
  }

  public static void createDeltaCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void createDeltaCommit(HoodieTableMetaClient metaClient, String instantTime,
                                       HoodieStorage storage) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, storage);
  }

  public static void createDeltaCommit(HoodieTableMetaClient metaClient, String instantTime,
                                       HoodieStorage storage, boolean preTableVersion8) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, storage, preTableVersion8);
  }

  public static void createRequestedDeltaCommit(HoodieTableMetaClient metaClient, String instantTime)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
  }

  public static void createInflightDeltaCommit(HoodieTableMetaClient metaClient, String instantTime)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  public static void createInflightDeltaCommit(HoodieTableMetaClient metaClient, String instantTime, HoodieStorage storage)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION, storage);
  }

  public static void createInflightReplaceCommit(HoodieTableMetaClient metaClient, String instantTime)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
  }

  public static void createReplaceCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe,
                                         String instantTime, Option<String> completeTime, HoodieReplaceCommitMetadata metadata) throws IOException {
    createMetaFile(metaClient, instantTime, completeTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION,
        Option.of(metadata));
  }

  public static void createReplaceCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe,
                                         String instantTime, HoodieReplaceCommitMetadata metadata) throws IOException {
    createMetaFile(metaClient, instantTime, Option.empty(), HoodieTimeline.REPLACE_COMMIT_EXTENSION,
        Option.of(metadata));
  }

  public static void createReplaceCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe,
                                         String instantTime, String completionTime, HoodieReplaceCommitMetadata metadata) throws IOException {
    createMetaFileInTimelinePath(
        metaClient, instantTime, () -> completionTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION,
        metaClient.getCommitMetadataSerDe().getInstantWriter(metadata));
  }

  public static void createRequestedClusterCommit(HoodieTableMetaClient metaClient, String instantTime,
                                                  HoodieRequestedReplaceMetadata requestedReplaceMetadata)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_CLUSTERING_COMMIT_EXTENSION,
        Option.of(requestedReplaceMetadata));
  }

  public static void createInflightClusterCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe,
                                                     String instantTime, Option<HoodieReplaceCommitMetadata> inflightReplaceMetadata)
      throws IOException {
    if (inflightReplaceMetadata.isPresent()) {
      createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_CLUSTERING_COMMIT_EXTENSION, inflightReplaceMetadata);
    } else {
      createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_CLUSTERING_COMMIT_EXTENSION);
    }
  }

  public static void createRequestedReplaceCommit(HoodieTableMetaClient metaClient, String instantTime,
                                                  Option<HoodieRequestedReplaceMetadata> requestedReplaceMetadata)
      throws IOException {
    if (requestedReplaceMetadata.isPresent()) {
      createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION, requestedReplaceMetadata);
    } else {
      createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION);
    }
  }

  public static void createInflightReplaceCommit(HoodieTableMetaClient metaClient, CommitMetadataSerDe commitMetadataSerDe,
                                                 String instantTime, Option<HoodieCommitMetadata> inflightReplaceMetadata)
      throws IOException {
    if (inflightReplaceMetadata.isPresent()) {
      createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION, inflightReplaceMetadata);
    } else {
      createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
    }
  }

  public static void createRequestedCompactionCommit(HoodieTableMetaClient metaClient, String instantTime,
                                                     HoodieCompactionPlan requestedCompactionPlan)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION,
        Option.of(requestedCompactionPlan));
  }

  public static void createCleanFile(HoodieTableMetaClient metaClient, String instantTime,
                                     HoodieCleanMetadata metadata) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.CLEAN_EXTENSION,
        Option.of(metadata));
  }

  public static void createCleanFile(HoodieTableMetaClient metaClient, String instantTime, Option<String> completeTime,
                                     HoodieCleanMetadata metadata, boolean isEmpty)
      throws IOException {
    createMetaFile(metaClient, instantTime, completeTime, HoodieTimeline.CLEAN_EXTENSION,
        isEmpty ? Option.empty() : Option.of(metadata));
  }

  public static void createRequestedCleanFile(HoodieTableMetaClient metaClient, String instantTime,
                                              HoodieCleanerPlan cleanerPlan) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_CLEAN_EXTENSION,
        Option.of(cleanerPlan));
  }

  public static void createRequestedCleanFile(HoodieTableMetaClient metaClient, String instantTime,
                                              HoodieCleanerPlan cleanerPlan, boolean isEmpty)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_CLEAN_EXTENSION,
        isEmpty ? Option.empty() : Option.of(cleanerPlan));
  }

  public static void createInflightCleanFile(HoodieTableMetaClient metaClient, String instantTime,
                                             HoodieCleanerPlan cleanerPlan) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION, Option.of(cleanerPlan));
  }

  public static void createInflightCleanFile(HoodieTableMetaClient metaClient, String instantTime,
                                             HoodieCleanerPlan cleanerPlan, boolean isEmpty)
      throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION,
        isEmpty ? Option.empty() : Option.of(cleanerPlan));
  }

  public static void createRequestedRollbackFile(HoodieTableMetaClient metaClient, String instantTime, HoodieRollbackPlan plan) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION, Option.of(plan));
  }

  public static void createRequestedRollbackFile(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION);
  }

  public static void createInflightRollbackFile(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION);
  }

  public static void createRollbackFile(HoodieTableMetaClient metaClient, String instantTime, HoodieRollbackMetadata hoodieRollbackMetadata, boolean isEmpty) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.ROLLBACK_EXTENSION, isEmpty ? Option.empty() : Option.of(hoodieRollbackMetadata));
  }

  public static void createRestoreFile(HoodieTableMetaClient metaClient, String instantTime, HoodieRestoreMetadata hoodieRestoreMetadata) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.RESTORE_ACTION, Option.of(hoodieRestoreMetadata));
  }

  public static void createRequestedCompaction(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION);
  }

  public static void createInflightCompaction(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  public static void createInflightSavepoint(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    createMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_SAVEPOINT_EXTENSION);
  }

  protected static void createMetaFileInTimelinePath(
      HoodieTableMetaClient metaClient, String instantTime, Supplier<String> completionTimeSupplier, String suffix, Option<HoodieInstantWriter> writer) throws IOException {
    try {
      Path parentPath = Paths.get(metaClient.getTimelinePath().makeQualified(new URI("file:///")).toUri());
      Files.createDirectories(parentPath);
      if (suffix.contains(HoodieTimeline.INFLIGHT_EXTENSION) || suffix.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        Path metaFilePath = parentPath.resolve(instantTime + suffix);
        if (Files.notExists(metaFilePath)) {
          if (writer.isEmpty()) {
            Files.createFile(metaFilePath);
          } else {
            try (OutputStream outputStream = Files.newOutputStream(metaFilePath)) {
              writer.get().writeToStream(outputStream);
            }
          }
        }
      } else {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(parentPath, instantTime + "*" + suffix)) {
          // The instant file is not exist
          if (!dirStream.iterator().hasNext()) {
            // doesn't contains completion time
            String completedInstantFilePrefix = "";
            if (metaClient.getTimelineLayoutVersion().getVersion().equals(TimelineLayoutVersion.VERSION_1)) {
              completedInstantFilePrefix = instantTime;
            } else if (metaClient.getTimelineLayoutVersion().getVersion().equals(TimelineLayoutVersion.VERSION_2)) {
              completedInstantFilePrefix = instantTime + "_" + completionTimeSupplier.get();
            }
            Path metaFilePath = parentPath.resolve(completedInstantFilePrefix + suffix);
            if (writer.isEmpty()) {
              Files.createFile(metaFilePath);
            } else {
              try (OutputStream outputStream = Files.newOutputStream(metaFilePath)) {
                writer.get().writeToStream(outputStream);
              }
            }
          }
        }
      }
    } catch (URISyntaxException ex) {
      throw new HoodieException(ex);
    }
  }

  public static String createBaseFile(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, String fileId)
      throws Exception {
    return createBaseFile(metaClient, partitionPath, instantTime, fileId, 1);
  }

  public static String createBaseFile(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, String fileId, long length)
      throws Exception {
    return createBaseFile(metaClient, partitionPath, instantTime, fileId, length, Instant.now().toEpochMilli());
  }

  public static String createBaseFile(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, String fileId, long length, long lastModificationTimeMilli)
      throws Exception {
    Path parentPath = Paths.get(metaClient.getBasePath().toUri().getPath(), partitionPath);
    Files.createDirectories(parentPath);
    Path baseFilePath = parentPath.resolve(baseFileName(instantTime, fileId));
    if (Files.notExists(baseFilePath)) {
      Files.createFile(baseFilePath);
    }
    try (RandomAccessFile raf = new RandomAccessFile(baseFilePath.toFile(), "rw")) {
      raf.setLength(length);
    }
    Files.setLastModifiedTime(baseFilePath, FileTime.fromMillis(lastModificationTimeMilli));
    return baseFilePath.toString();
  }

  public static Path getBaseFilePath(String basePath, String partitionPath, String instantTime, String fileId) {
    Path parentPath = Paths.get(basePath, partitionPath);
    return parentPath.resolve(baseFileName(instantTime, fileId));
  }

  public static String createLogFile(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, String fileId, int version)
      throws Exception {
    return createLogFile(metaClient, partitionPath, instantTime, fileId, version, 0);
  }

  public static String createLogFile(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, String fileId, int version, int length)
      throws Exception {
    Path parentPath = Paths.get(metaClient.getBasePath().toUri().getPath(), partitionPath);
    Files.createDirectories(parentPath);
    Path logFilePath = parentPath.resolve(logFileName(instantTime, fileId, version));
    if (Files.notExists(logFilePath)) {
      Files.createFile(logFilePath);
    }
    RandomAccessFile raf = new RandomAccessFile(logFilePath.toFile(), "rw");
    raf.setLength(length);
    raf.close();
    return logFilePath.toString();
  }

  public static String createMarkerFile(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, String fileId, IOType ioType)
      throws IOException {
    if (IOType.APPEND == ioType) {
      String logFileName = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime, HoodieLogFile.LOGFILE_BASE_VERSION, WRITE_TOKEN);
      String markerFileName = markerFileName(logFileName, ioType);
      return createMarkerFile(metaClient, partitionPath, instantTime, markerFileName);
    }
    return createMarkerFile(metaClient, partitionPath, instantTime, instantTime, fileId, ioType, WRITE_TOKEN);
  }

  public static String createMarkerFile(HoodieTableMetaClient metaClient, String partitionPath, String commitInstant,
                                        String instantTime, String fileId, IOType ioType, String writeToken) throws IOException {
    return createMarkerFile(metaClient, partitionPath, commitInstant, markerFileName(instantTime, fileId, ioType, BASE_FILE_EXTENSION, writeToken));
  }

  public static String createMarkerFile(HoodieTableMetaClient metaClient, String partitionPath, String commitInstant, String markerFileName) throws IOException {
    Path parentPath = Paths.get(metaClient.getTempFolderPath(), commitInstant, partitionPath);
    Files.createDirectories(parentPath);
    Path markerFilePath = parentPath.resolve(markerFileName);
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  public static String createMarkerFile(String basePath, String partitionPath, String commitInstant,
      String instantTime, String fileId, IOType ioType, String writeToken) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    Files.createDirectories(parentPath);
    Path markerFilePath = parentPath.resolve(dataFileMarkerFileName(instantTime, fileId, ioType, BASE_FILE_EXTENSION, writeToken));
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  public static String createLogFileMarker(String basePath, String partitionPath, String instantTime, String fileId, IOType ioType)
      throws IOException {
    return createLogFileMarker(basePath, partitionPath, instantTime, instantTime, fileId, ioType, HoodieLogFile.LOGFILE_BASE_VERSION);
  }

  public static String createLogFileMarker(String basePath, String partitionPath, String baseInstantTime, String instantTime, String fileId,
                                           IOType ioType,
                                           int logVersion)
      throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    Files.createDirectories(parentPath);
    Path markerFilePath = parentPath.resolve(logFileMarkerFileName(baseInstantTime, fileId, ioType, logVersion));
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  public static String createFileMarkerByFileName(String basePath, String partitionPath, String instantTime, String fileName, IOType ioType)
      throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, instantTime, partitionPath);
    Files.createDirectories(parentPath);
    Path markerFilePath = parentPath.resolve(markerFileName(fileName, ioType));
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  public static String createLogFileMarker(HoodieTableMetaClient metaClient, String partitionPath,
                                           String instantTime, String logFileName)
      throws IOException {
    return createMarkerFile(metaClient, partitionPath, instantTime,
        markerFileName(logFileName,
            metaClient.getTableConfig().getTableVersion()
                .greaterThanOrEquals(HoodieTableVersion.EIGHT) ? IOType.CREATE : IOType.APPEND));
  }

  private static void removeMetaFile(HoodieTableMetaClient metaClient, String instantTime, String suffix) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, suffix);
  }

  public static void deleteCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void deleteRequestedCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  public static void deleteInflightCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  public static void deleteDeltaCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void deleteReplaceCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION);
  }

  public static void deleteRollbackCommit(HoodieTableMetaClient metaClient, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(metaClient.getTimelinePath().toUri().getPath(), instantTime, HoodieTimeline.ROLLBACK_EXTENSION);
  }

  public static Path renameFileToTemp(Path sourcePath, String instantTime) throws IOException {
    Path dummyFilePath = sourcePath.getParent().resolve(instantTime + ".temp");
    Files.move(sourcePath, dummyFilePath);
    return dummyFilePath;
  }

  public static void renameTempToMetaFile(Path tempFilePath, Path destPath) throws IOException {
    Files.move(tempFilePath, destPath);
  }

  public static long getTotalMarkerFileCount(HoodieTableMetaClient metaClient, String partitionPath, String instantTime, IOType ioType) throws IOException {
    Path parentPath = Paths.get(metaClient.getTempFolderPath(), instantTime, partitionPath);
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
  public static Map<String, Long> getBaseFileCountsForPaths(HoodieTableMetaClient metaClient, HoodieStorage storage,
                                                            String... paths) {
    Map<String, Long> toReturn = new HashMap<>();
    try {
      for (String path : paths) {
        TableFileSystemView.BaseFileOnlyView fileSystemView =
            new HoodieTableFileSystemView(metaClient,
                metaClient.getCommitsTimeline().filterCompletedInstants(),
                storage.globEntries(new StoragePath(path)));
        toReturn.put(path, fileSystemView.getLatestBaseFiles().count());
      }
      return toReturn;
    } catch (Exception e) {
      throw new HoodieException("Error reading hoodie table as a dataframe", e);
    }
  }

  public static void deleteDeltaCommit(HoodieTableMetaClient metaClient, String instantTime,
                                       HoodieStorage storage) throws IOException {
    deleteMetaFile(metaClient, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, storage);
  }

  public static void deleteSavepointCommit(HoodieTableMetaClient metaClient, String instantTime,
                                           HoodieStorage storage) throws IOException {
    deleteMetaFile(metaClient, instantTime, HoodieTimeline.INFLIGHT_SAVEPOINT_EXTENSION, storage);
    deleteMetaFile(metaClient, instantTime, HoodieTimeline.SAVEPOINT_EXTENSION, storage);
  }
}
