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

package org.apache.hudi.client.timeline.versioning.v2;

import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLSMTimelineManifest;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetReader;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * A timeline writer which organizes the files as an LSM tree.
 */
@Slf4j
public class LSMTimelineWriter {

  public static final int FILE_LAYER_ZERO = 0;

  private final HoodieWriteConfig config;
  private final TaskContextSupplier taskContextSupplier;
  private final HoodieTableMetaClient metaClient;
  private final StoragePath archivePath;

  private HoodieWriteConfig writeConfig;

  private LSMTimelineWriter(HoodieWriteConfig config, HoodieTable<?, ?, ?, ?> table) {
    this(config, table.getTaskContextSupplier(), table.getMetaClient(), Option.empty());
  }

  private LSMTimelineWriter(HoodieWriteConfig config, HoodieTable<?, ?, ?, ?> table, Option<StoragePath> archivePath) {
    this(config, table.getTaskContextSupplier(), table.getMetaClient(), archivePath);
  }

  private LSMTimelineWriter(HoodieWriteConfig config, TaskContextSupplier taskContextSupplier, HoodieTableMetaClient metaClient, Option<StoragePath> archivePath) {
    this.config = config;
    this.taskContextSupplier = taskContextSupplier;
    this.metaClient = metaClient;
    this.archivePath = archivePath.orElse(metaClient.getArchivePath());
  }

  public static LSMTimelineWriter getInstance(HoodieWriteConfig config, HoodieTable<?, ?, ?, ?> table) {
    return new LSMTimelineWriter(config, table);
  }

  public static LSMTimelineWriter getInstance(HoodieWriteConfig config, HoodieTable<?, ?, ?, ?> table, Option<StoragePath> archivePath) {
    return new LSMTimelineWriter(config, table, archivePath);
  }

  public static LSMTimelineWriter getInstance(HoodieWriteConfig config, TaskContextSupplier taskContextSupplier, HoodieTableMetaClient metaClient) {
    return new LSMTimelineWriter(config, taskContextSupplier, metaClient, Option.empty());
  }

  /**
   * Writes the list of active actions into the timeline.
   *
   * @param activeActions    The active actions
   * @param preWriteCallback The callback before writing each action
   * @param exceptionHandler The handle for exception
   */
  public void write(
      List<ActiveAction> activeActions,
      Option<Consumer<ActiveAction>> preWriteCallback,
      Option<Consumer<Exception>> exceptionHandler) {
    ValidationUtils.checkArgument(!activeActions.isEmpty(), "The instant actions to write should not be empty");
    StoragePath filePath = new StoragePath(this.archivePath,
        newFileName(activeActions.get(0).getInstantTime(), activeActions.get(activeActions.size() - 1).getInstantTime(), FILE_LAYER_ZERO));
    try {
      if (this.metaClient.getStorage().exists(filePath)) {
        // there are 2 cases this could happen:
        // 1. the file was created but did not flush/close correctly and left as corrupt;
        // 2. the file was in complete state but the archiving fails during the deletion of active metadata files.
        if (isFileCommitted(filePath.getName())) {
          // case2: the last archiving succeeded for committing to the archive timeline, just returns early.
          log.info("Skip archiving for the redundant file: {}", filePath);
          return;
        } else {
          // case1: delete the corrupt file and retry.
          this.metaClient.getStorage().deleteFile(filePath);
        }
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Failed to check archiving file before write: " + filePath, ioe);
    }
    try (HoodieFileWriter writer = openWriter(filePath)) {
      Schema wrapperSchema = HoodieLSMTimelineInstant.getClassSchema();
      log.info("Writing schema " + wrapperSchema.toString());
      HoodieSchema schema = HoodieSchema.fromAvroSchema(wrapperSchema);
      for (ActiveAction activeAction : activeActions) {
        try {
          preWriteCallback.ifPresent(callback -> callback.accept(activeAction));
          // in local FS and HDFS, there could be empty completed instants due to crash.
          final HoodieLSMTimelineInstant metaEntry = MetadataConversionUtils.createLSMTimelineInstant(activeAction, metaClient);
          writer.write(metaEntry.getInstantTime(), new HoodieAvroIndexedRecord(metaEntry), schema);
        } catch (Exception e) {
          log.error("Failed to write instant: " + activeAction.getInstantTime(), e);
          exceptionHandler.ifPresent(handler -> handler.accept(e));
        }
      }
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to write commits", e);
    }
    try {
      updateManifest(filePath.getName());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to update archiving manifest", e);
    }
  }

  /**
   * Updates a manifest file.
   *
   * <p>3 steps:
   * <ol>
   *   <li>read the latest manifest version file;</li>
   *   <li>read the latest manifest file for valid files;</li>
   *   <li>add this new file to the existing file list from step2.</li>
   * </ol>
   *
   * @param fileToAdd New file name to add
   */
  public void updateManifest(String fileToAdd) throws IOException {
    updateManifest(Collections.emptyList(), fileToAdd);
  }

  /**
   * Updates a manifest file.
   *
   * <p>4 steps:
   * <ol>
   *   <li>read the latest manifest version file;</li>
   *   <li>read the latest manifest file for valid files;</li>
   *   <li>remove files to the existing file list from step2;</li>
   *   <li>add this new file to the existing file list from step2.</li>
   * </ol>
   *
   * @param filesToRemove File names to remove
   * @param fileToAdd     New file name to add
   */
  public void updateManifest(List<String> filesToRemove, String fileToAdd) throws IOException {
    int latestVersion = LSMTimeline.latestSnapshotVersion(metaClient, archivePath);
    HoodieLSMTimelineManifest latestManifest = LSMTimeline.latestSnapshotManifest(metaClient, latestVersion, archivePath);
    HoodieLSMTimelineManifest newManifest = latestManifest.copy(filesToRemove);
    newManifest.addFile(getFileEntry(fileToAdd));
    createManifestFile(newManifest, latestVersion);
  }

  private void createManifestFile(HoodieLSMTimelineManifest manifest, int currentVersion) throws IOException {
    byte[] content = getUTF8Bytes(manifest.toJsonString());
    // version starts from 1 and increases monotonically
    int newVersion = currentVersion < 0 ? 1 : currentVersion + 1;
    // create manifest file
    final StoragePath manifestFilePath = LSMTimeline.getManifestFilePath(newVersion, archivePath);
    // the version is basically the latest version plus 1, if the preceding failed write succeed
    // to write a manifest file but failed to write the version file, a corrupt manifest file was left with just the `newVersion`.
    deleteIfExists(manifestFilePath);
    metaClient.getStorage().createImmutableFileInPath(manifestFilePath, Option.of(HoodieInstantWriter.convertByteArrayToWriter(content)));
    // update version file
    updateVersionFile(newVersion);
  }

  private void updateVersionFile(int newVersion) throws IOException {
    byte[] content = getUTF8Bytes(String.valueOf(newVersion));
    final StoragePath versionFilePath = LSMTimeline.getVersionFilePath(archivePath);
    metaClient.getStorage().deleteFile(versionFilePath);
    // if the step fails here, either the writer or reader would list the manifest files to find the latest snapshot version.
    metaClient.getStorage().createImmutableFileInPath(versionFilePath, Option.of(HoodieInstantWriter.convertByteArrayToWriter(content)));
  }

  /**
   * Compacts the small parquet files.
   *
   * <p>The parquet naming convention is:
   *
   * <pre>${min_instant}_${max_instant}_${level}.parquet</pre>
   *
   * <p>The 'min_instant' and 'max_instant' represent the instant time range of the parquet file.
   * The 'level' represents the number of the level where the file is located, currently we
   * have no limit for the number of layers.
   *
   * <p>These parquet files composite as an LSM tree layout, one parquet file contains
   * instant metadata entries with consecutive timestamp. Different parquet files may have
   * overlapping with the instant time ranges.
   *
   * <pre>
   *   t1_t2_0.parquet, t3_t4_0.parquet, ... t5_t6_0.parquet       L0 layer
   *                          \            /
   *                             \     /
   *                                |
   *                                V
   *                          t3_t6_1.parquet                      L1 layer
   * </pre>
   *
   * <p>Compaction and cleaning: once the files number exceed a threshold(now constant 10) N,
   * the oldest N files are then replaced with a compacted file in the next layer.
   * A cleaning action is triggered right after the compaction.
   *
   * @param context HoodieEngineContext
   */
  @VisibleForTesting
  public void compactAndClean(HoodieEngineContext context) throws IOException {
    // 1. List all the latest snapshot files
    HoodieLSMTimelineManifest latestManifest = LSMTimeline.latestSnapshotManifest(metaClient, archivePath);
    int layer = 0;
    // 2. triggers the compaction for L0
    Option<String> compactedFileName = doCompact(latestManifest, layer);
    while (compactedFileName.isPresent()) {
      // 3. once a compaction had been executed for the current layer,
      // continues to trigger compaction for the next layer.
      latestManifest.addFile(getFileEntry(compactedFileName.get()));
      compactedFileName = doCompact(latestManifest, ++layer);
    }

    // cleaning
    clean(context, layer);
  }

  private Option<String> doCompact(HoodieLSMTimelineManifest manifest, int layer) throws IOException {
    // 1. list all the files that belong to current layer
    List<HoodieLSMTimelineManifest.LSMFileEntry> files = manifest.getFiles()
        .stream().filter(file -> LSMTimeline.isFileFromLayer(file.getFileName(), layer)).collect(Collectors.toList());

    int compactionBatchSize = config.getTimelineCompactionBatchSize();

    if (files.size() >= compactionBatchSize) {
      // 2. sort files by min instant time (implies ascending chronological order)
      files.sort(HoodieLSMTimelineManifest.LSMFileEntry::compareTo);
      List<String> candidateFiles = getCandidateFiles(files, compactionBatchSize);
      if (candidateFiles.size() < 2) {
        // the file is too large to compact, returns early.
        return Option.empty();
      }
      String compactedFileName = compactedFileName(candidateFiles);

      // 3. compaction
      compactFiles(candidateFiles, compactedFileName);
      // 4. update the manifest file
      updateManifest(candidateFiles, compactedFileName);
      log.info("Finishes compaction of source files: " + candidateFiles);
      return Option.of(compactedFileName);
    }
    return Option.empty();
  }

  public void compactFiles(List<String> candidateFiles, String compactedFileName) throws IOException {
    log.info("Starting to compact source files.");
    StoragePath compactedFilePath = new StoragePath(archivePath, compactedFileName);
    deleteIfExists(compactedFilePath);
    try (HoodieFileWriter writer = openWriter(compactedFilePath)) {
      for (String fileName : candidateFiles) {
        // Read the input source file
        try (HoodieAvroParquetReader reader = (HoodieAvroParquetReader) HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
            .getFileReader(config, new StoragePath(archivePath, fileName))) {
          // Read the meta entry
          //TODO boundary to revisit in later pr to use HoodieSchema directly
          HoodieSchema schema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
          try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(schema,
              schema)) {
            while (iterator.hasNext()) {
              IndexedRecord record = iterator.next();
              writer.write(record.get(0).toString(), new HoodieAvroIndexedRecord(record), schema);
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to compact source files", e);
    }
  }

  /**
   * Checks whether there is any unfinished compaction operation.
   *
   * @param context HoodieEngineContext used for parallelize to delete obsolete files if necessary.
   */
  public void clean(HoodieEngineContext context, int compactedVersions) throws IOException {
    // if there are more than 3 version of snapshots, clean the oldest files.
    List<Integer> allSnapshotVersions = LSMTimeline.allSnapshotVersions(metaClient, archivePath);
    int numVersionsToKeep = 3 + compactedVersions; // should make the threshold configurable.
    if (allSnapshotVersions.size() > numVersionsToKeep) {
      allSnapshotVersions.sort((v1, v2) -> v2 - v1);
      List<Integer> versionsToKeep = allSnapshotVersions.subList(0, numVersionsToKeep);
      Set<String> filesToKeep = versionsToKeep.stream()
          .flatMap(version -> LSMTimeline.latestSnapshotManifest(metaClient, version, archivePath).getFileNames().stream())
          .collect(Collectors.toSet());
      // delete the manifest file first
      List<String> manifestFilesToClean = new ArrayList<>();
      LSMTimeline.listAllManifestFiles(metaClient, archivePath).forEach(fileStatus -> {
        if (!versionsToKeep.contains(
            LSMTimeline.getManifestVersion(fileStatus.getPath().getName()))) {
          manifestFilesToClean.add(fileStatus.getPath().toString());
        }
      });
      FSUtils.deleteFilesParallelize(metaClient, manifestFilesToClean, context, config.getArchiveDeleteParallelism(),
          false);
      // delete the data files
      List<String> dataFilesToClean = LSMTimeline.listAllMetaFiles(metaClient, archivePath).stream()
          .filter(fileStatus -> !filesToKeep.contains(fileStatus.getPath().getName()))
          .map(fileStatus -> fileStatus.getPath().toString())
          .collect(Collectors.toList());
      FSUtils.deleteFilesParallelize(metaClient, dataFilesToClean, context,
          config.getArchiveDeleteParallelism(), false);
    }
  }

  /**
   * Returns whether the archiving file is committed(visible to the timeline reader).
   */
  private boolean isFileCommitted(String fileName) throws IOException {
    HoodieLSMTimelineManifest latestManifest = LSMTimeline.latestSnapshotManifest(metaClient, archivePath);
    return latestManifest.getFiles()
        .stream().anyMatch(fileEntry -> fileEntry.getFileName().equals(fileName));
  }

  private HoodieLSMTimelineManifest.LSMFileEntry getFileEntry(String fileName) throws IOException {
    long fileLen = metaClient.getStorage().getPathInfo(
        new StoragePath(archivePath, fileName)).getLength();
    return HoodieLSMTimelineManifest.LSMFileEntry.getInstance(fileName, fileLen);
  }

  /**
   * Returns at most {@code filesBatch} number of source files
   * restricted by the gross file size by 1GB.
   */
  private List<String> getCandidateFiles(List<HoodieLSMTimelineManifest.LSMFileEntry> files, int filesBatch) throws IOException {
    List<String> candidates = new ArrayList<>();
    long totalFileLen = 0L;
    // try to find at most one group of files to compact
    // 1. files num in the group should be at least 2
    // 2. files num in the group should not exceed the batch size
    // 3. the group's total file size should not exceed the threshold
    // 4. all files in the group should be consecutive in instant order
    for (int i = 0; i < files.size(); i++) {
      HoodieLSMTimelineManifest.LSMFileEntry fileEntry = files.get(i);
      // we may also need to consider a single file that is very close to the threshold in size,
      // to avoid the write amplification,
      // for e.g, two 800MB files compact into a 1.6GB file.
      totalFileLen += fileEntry.getFileLen();
      candidates.add(fileEntry.getFileName());
      if (candidates.size() >= filesBatch) {
        // stop once we reach the batch size
        break;
      }
      if (totalFileLen > writeConfig.getTimelineCompactionTargetFileMaxBytes()) {
        if (candidates.size() < 2) {
          // reset if we have not reached the minimum files num to compact
          totalFileLen = 0L;
          candidates.clear();
        } else {
          // stop once we reach the file size threshold
          break;
        }
      }
    }
    return candidates;
  }

  /**
   * Returns a new file name.
   */
  private static String newFileName(String minInstant, String maxInstant, int layer) {
    return String.format("%s_%s_%d%s", minInstant, maxInstant, layer, HoodieFileFormat.PARQUET.getFileExtension());
  }

  /**
   * Returns a new file name.
   */
  @VisibleForTesting
  public static String compactedFileName(List<String> files) {
    String minInstant = files.stream().map(LSMTimeline::getMinInstantTime)
        .min(Comparator.naturalOrder()).get();
    String maxInstant = files.stream().map(LSMTimeline::getMaxInstantTime)
        .max(Comparator.naturalOrder()).get();
    int currentLayer = LSMTimeline.getFileLayer(files.get(0));
    return newFileName(minInstant, maxInstant, currentLayer + 1);
  }

  private void deleteIfExists(StoragePath filePath) throws IOException {
    if (metaClient.getStorage().exists(filePath)) {
      // delete file if exists when try to overwrite file
      metaClient.getStorage().deleteFile(filePath);
      log.info("Delete corrupt file: {} left by failed write", filePath);
    }
  }

  /**
   * Get or create a writer config for parquet writer.
   */
  private HoodieWriteConfig getOrCreateWriterConfig() {
    if (this.writeConfig == null) {
      this.writeConfig = HoodieWriteConfig.newBuilder()
          .withProperties(this.config.getProps())
          .withPopulateMetaFields(false).build();
    }
    return this.writeConfig;
  }

  private HoodieFileWriter openWriter(StoragePath filePath) {
    try {
      return HoodieFileWriterFactory.getFileWriter("", filePath, metaClient.getStorage(), getOrCreateWriterConfig(),
          HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema()), taskContextSupplier, HoodieRecord.HoodieRecordType.AVRO);
    } catch (IOException e) {
      throw new HoodieException("Unable to initialize archiving writer", e);
    }
  }
}
