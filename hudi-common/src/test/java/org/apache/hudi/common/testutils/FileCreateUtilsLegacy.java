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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
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
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;

/**
 * Utils for creating dummy Hudi files in testing.
 * ****************************************************
 *      Use FileCreateUtils whenever possible!
 * ****************************************************
 * [HUDI-8924]: Remove this class entirely.
 */
public class FileCreateUtilsLegacy extends FileCreateUtilsBase {

  public static StoragePath getTimelinePath(StoragePath basePath) throws IOException {
    return new StoragePath(
        new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME),
        HoodieTableMetaClient.TIMELINEFOLDER_NAME);
  }

  private static StoragePath getMetaPath(String basePath) {
    return new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix,
                                     HoodieStorage storage) throws IOException {
    createMetaFileInMetaPath(getMetaPath(basePath), instantTime, suffix, storage, false);
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix) throws IOException {
    createMetaFile(basePath, instantTime, suffix, Option.empty());
  }

  private static <T> void createMetaFile(String basePath, String instantTime, String suffix, Option<T> metadata) throws IOException {
    createMetaFile(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime,
        InProcessTimeGenerator::createNewInstantTime, suffix, metadata.flatMap(COMMIT_METADATA_SER_DE::getInstantWriter));
  }

  private static void createMetaFile(String basePath, String instantTime, String suffix, HoodieInstantWriter writer) throws IOException {
    createMetaFile(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime,
        InProcessTimeGenerator::createNewInstantTime, suffix, Option.of(writer));
  }

  public static void createCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void createCommit(CommitMetadataSerDe commitMetadataSerDe, String basePath, String instantTime,
                                  Option<HoodieCommitMetadata> metadata) throws IOException {
    createCommit(commitMetadataSerDe, basePath, instantTime, Option.empty(), metadata);
  }

  public static void createCommit(CommitMetadataSerDe commitMetadataSerDe, String basePath, String instantTime,
                                  Option<String> completionTime, Option<HoodieCommitMetadata> metadata) throws IOException {
    final Supplier<String> completionTimeSupplier = () -> completionTime.isPresent() ? completionTime.get() : InProcessTimeGenerator.createNewInstantTime();
    String timelinePath = getTimelinePath(new StoragePath(basePath)).toUri().getPath();
    if (metadata.isPresent()) {
      HoodieCommitMetadata commitMetadata = metadata.get();
      createMetaFile(timelinePath, instantTime, completionTimeSupplier, HoodieTimeline.COMMIT_EXTENSION,
          COMMIT_METADATA_SER_DE.getInstantWriter(commitMetadata));
    } else {
      createMetaFile(timelinePath, instantTime, completionTimeSupplier, HoodieTimeline.COMMIT_EXTENSION, Option.empty());
    }
  }

  public static void createCommit(String basePath, String instantTime, HoodieStorage storage)
      throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.COMMIT_EXTENSION, storage);
  }

  public static void createInflightCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  public static void createDeltaCommit(CommitMetadataSerDe commitMetadataSerDe, String basePath, String instantTime,
                                       HoodieCommitMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, Option.of(metadata));
  }

  public static void createDeltaCommit(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void createDeltaCommit(String basePath, String instantTime,
                                       HoodieStorage storage) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, storage);
  }

  public static void createDeltaCommit(String basePath, String instantTime,
                                       HoodieStorage storage, boolean preTableVersion8) throws IOException {
    createMetaFileInMetaPath(getMetaPath(basePath), instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, storage, preTableVersion8);
  }

  public static void createInflightDeltaCommit(String basePath, String instantTime, HoodieStorage storage)
      throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION, storage);
  }

  public static void createReplaceCommit(CommitMetadataSerDe commitMetadataSerDe, String basePath,
                                         String instantTime, HoodieReplaceCommitMetadata metadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION, Option.of(metadata));
  }

  public static void createRequestedClusterCommit(String basePath, String instantTime,
                                                  HoodieRequestedReplaceMetadata requestedReplaceMetadata) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_CLUSTERING_COMMIT_EXTENSION, Option.of(requestedReplaceMetadata));
  }

  public static void createRequestedCompactionCommit(String basePath, String instantTime,
                                                     HoodieCompactionPlan requestedCompactionPlan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION, Option.of(requestedCompactionPlan));
  }

  public static void createRequestedRollbackFile(String basePath, String instantTime, HoodieRollbackPlan plan) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION, Option.of(plan));
  }

  public static void createRequestedRollbackFile(String basePath, String instantTime, byte[] content) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION,
        HoodieInstantWriter.convertByteArrayToWriter(content));
  }

  public static void createInflightCompaction(String basePath, String instantTime) throws IOException {
    createMetaFile(basePath, instantTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  public static String createBaseFile(String basePath, String partitionPath, String instantTime, String fileId)
      throws Exception {
    return createBaseFile(basePath, partitionPath, instantTime, fileId, 1);
  }

  public static String createBaseFile(String basePath, String partitionPath, String instantTime, String fileId, long length)
      throws Exception {
    return createBaseFile(basePath, partitionPath, instantTime, fileId, length, Instant.now().toEpochMilli());
  }

  public static String createBaseFile(String basePath, String partitionPath, String instantTime, String fileId, long length, long lastModificationTimeMilli)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
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

  public static String createLogFile(String basePath, String partitionPath, String instantTime, String fileId, int version)
      throws Exception {
    return createLogFile(basePath, partitionPath, instantTime, fileId, version, 0);
  }

  public static String createLogFile(String basePath, String partitionPath, String instantTime, String fileId, int version, int length)
      throws Exception {
    Path parentPath = Paths.get(basePath, partitionPath);
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

  public static String createMarkerFile(String basePath, String partitionPath, String instantTime, String fileId, IOType ioType)
      throws IOException {
    if (IOType.APPEND == ioType) {
      String logFileName = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime, HoodieLogFile.LOGFILE_BASE_VERSION, WRITE_TOKEN);
      String markerFileName = markerFileName(logFileName, ioType);
      return createMarkerFile(basePath, partitionPath, instantTime, markerFileName);
    }
    return createMarkerFile(basePath, partitionPath, instantTime, instantTime, fileId, ioType, WRITE_TOKEN);
  }

  public static String createMarkerFile(String basePath, String partitionPath, String commitInstant,
      String instantTime, String fileId, IOType ioType, String writeToken) throws IOException {
    return createMarkerFile(basePath, partitionPath, commitInstant, markerFileName(instantTime, fileId, ioType, BASE_FILE_EXTENSION, writeToken));
  }

  public static String createMarkerFile(String basePath, String partitionPath, String commitInstant, String markerFileName) throws IOException {
    Path parentPath = Paths.get(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME, commitInstant, partitionPath);
    Files.createDirectories(parentPath);
    Path markerFilePath = parentPath.resolve(markerFileName);
    if (Files.notExists(markerFilePath)) {
      Files.createFile(markerFilePath);
    }
    return markerFilePath.toAbsolutePath().toString();
  }

  private static void createMetaFile(String timelinePath, String instantTime, Supplier<String> completionTimeSupplier, String suffix,
                                     Option<HoodieInstantWriter> writerOption) throws IOException {
    try {
      Path parentPath = Paths.get(new StoragePath(timelinePath).makeQualified(new URI("file:///")).toUri());
      Files.createDirectories(parentPath);
      if (suffix.contains(HoodieTimeline.INFLIGHT_EXTENSION) || suffix.contains(HoodieTimeline.REQUESTED_EXTENSION)) {
        Path metaFilePath = parentPath.resolve(instantTime + suffix);
        if (Files.notExists(metaFilePath)) {
          if (writerOption.isEmpty()) {
            Files.createFile(metaFilePath);
          } else {
            try (OutputStream outputStream = Files.newOutputStream(metaFilePath)) {
              writerOption.get().writeToStream(outputStream);
            }
          }
        }
      } else {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(parentPath, instantTime + "*" + suffix)) {
          // The instant file is not exist
          if (!dirStream.iterator().hasNext()) {
            // doesn't contains completion time
            String instantTimeAndCompletionTime = instantTime + "_" + completionTimeSupplier.get();
            Path metaFilePath = parentPath.resolve(instantTimeAndCompletionTime + suffix);
            if (writerOption.isEmpty()) {
              Files.createFile(metaFilePath);
            } else {
              try (OutputStream outputStream = Files.newOutputStream(metaFilePath)) {
                writerOption.get().writeToStream(outputStream);
              }
            }
          }
        }
      }
    } catch (URISyntaxException ex) {
      throw new HoodieException(ex);
    }
  }

  public static void deleteCommit(String basePath, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  public static void deleteDeltaCommit(String basePath, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION);
  }

  public static void deleteReplaceCommit(String basePath, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime, HoodieTimeline.REPLACE_COMMIT_EXTENSION);
  }

  public static void deleteRollbackCommit(String basePath, String instantTime) throws IOException {
    removeMetaFileInTimelinePath(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime, HoodieTimeline.ROLLBACK_EXTENSION);
  }

  public static Path renameFileToTemp(Path sourcePath, String instantTime) throws IOException {
    Path dummyFilePath = sourcePath.getParent().resolve(instantTime + ".temp");
    Files.move(sourcePath, dummyFilePath);
    return dummyFilePath;
  }

  public static void renameTempToMetaFile(Path tempFilePath, Path destPath) throws IOException {
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

  public static void deleteDeltaCommit(String basePath, String instantTime,
                                       HoodieStorage storage) throws IOException {
    deleteMetaFileInTimeline(getTimelinePath(new StoragePath(basePath)).toUri().getPath(), instantTime, HoodieTimeline.DELTA_COMMIT_EXTENSION, storage);
  }
}
