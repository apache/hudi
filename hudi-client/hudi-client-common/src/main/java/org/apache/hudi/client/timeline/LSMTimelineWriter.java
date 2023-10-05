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

package org.apache.hudi.client.timeline;

import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLSMTimelineManifest;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieAvroParquetReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
public class LSMTimelineWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LSMTimelineWriter.class);

  public static final int FILE_LAYER_ZERO = 0;

  public static final long MAX_FILE_SIZE_IN_BYTES = 1024 * 1024 * 1000;

  private final HoodieWriteConfig config;
  private final HoodieTable<?, ?, ?, ?> table;
  private final HoodieTableMetaClient metaClient;

  private HoodieWriteConfig writeConfig;

  private LSMTimelineWriter(HoodieWriteConfig config, HoodieTable<?, ?, ?, ?> table) {
    this.config = config;
    this.table = table;
    this.metaClient = table.getMetaClient();
  }

  public static LSMTimelineWriter getInstance(HoodieWriteConfig config, HoodieTable<?, ?, ?, ?> table) {
    return new LSMTimelineWriter(config, table);
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
      Option<Consumer<Exception>> exceptionHandler) throws HoodieCommitException {
    ValidationUtils.checkArgument(!activeActions.isEmpty(), "The instant actions to write should not be empty");
    Path filePath = new Path(metaClient.getArchivePath(),
        newFileName(activeActions.get(0).getInstantTime(), activeActions.get(activeActions.size() - 1).getInstantTime(), FILE_LAYER_ZERO));
    try (HoodieFileWriter writer = openWriter(filePath)) {
      Schema wrapperSchema = HoodieLSMTimelineInstant.getClassSchema();
      LOG.info("Writing schema " + wrapperSchema.toString());
      for (ActiveAction activeAction : activeActions) {
        try {
          preWriteCallback.ifPresent(callback -> callback.accept(activeAction));
          // in local FS and HDFS, there could be empty completed instants due to crash.
          final HoodieLSMTimelineInstant metaEntry = MetadataConversionUtils.createLSMTimelineInstant(activeAction, metaClient);
          writer.write(metaEntry.getInstantTime(), new HoodieAvroIndexedRecord(metaEntry), wrapperSchema);
        } catch (Exception e) {
          LOG.error("Failed to write instant: " + activeAction.getInstantTime(), e);
          exceptionHandler.ifPresent(handler -> handler.accept(e));
        }
      }
      updateManifest(filePath.getName());
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to write commits", e);
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
    int latestVersion = LSMTimeline.latestSnapshotVersion(metaClient);
    HoodieLSMTimelineManifest latestManifest = LSMTimeline.latestSnapshotManifest(metaClient, latestVersion);
    HoodieLSMTimelineManifest newManifest = latestManifest.copy(filesToRemove);
    newManifest.addFile(getFileEntry(fileToAdd));
    createManifestFile(newManifest, latestVersion);
  }

  private void createManifestFile(HoodieLSMTimelineManifest manifest, int currentVersion) throws IOException {
    byte[] content = getUTF8Bytes(manifest.toJsonString());
    // version starts from 1 and increases monotonically
    int newVersion = currentVersion < 0 ? 1 : currentVersion + 1;
    // create manifest file
    final Path manifestFilePath = LSMTimeline.getManifestFilePath(metaClient, newVersion);
    metaClient.getFs().createImmutableFileInPath(manifestFilePath, Option.of(content));
    // update version file
    updateVersionFile(newVersion);
  }

  private void updateVersionFile(int newVersion) throws IOException {
    byte[] content = getUTF8Bytes(String.valueOf(newVersion));
    final Path versionFilePath = LSMTimeline.getVersionFilePath(metaClient);
    metaClient.getFs().delete(versionFilePath, false);
    metaClient.getFs().createImmutableFileInPath(versionFilePath, Option.of(content));
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
    HoodieLSMTimelineManifest latestManifest = LSMTimeline.latestSnapshotManifest(metaClient);
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
      LOG.info("Finishes compaction of source files: " + candidateFiles);
      return Option.of(compactedFileName);
    }
    return Option.empty();
  }

  public void compactFiles(List<String> candidateFiles, String compactedFileName) {
    LOG.info("Starting to compact source files.");
    try (HoodieFileWriter writer = openWriter(new Path(metaClient.getArchivePath(), compactedFileName))) {
      for (String fileName : candidateFiles) {
        // Read the input source file
        try (HoodieAvroParquetReader reader = (HoodieAvroParquetReader) HoodieFileReaderFactory.getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
            .getFileReader(metaClient.getHadoopConf(), new Path(metaClient.getArchivePath(), fileName))) {
          // Read the meta entry
          try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieLSMTimelineInstant.getClassSchema(), HoodieLSMTimelineInstant.getClassSchema())) {
            while (iterator.hasNext()) {
              IndexedRecord record = iterator.next();
              writer.write(record.get(0).toString(), new HoodieAvroIndexedRecord(record), HoodieLSMTimelineInstant.getClassSchema());
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
    List<Integer> allSnapshotVersions = LSMTimeline.allSnapshotVersions(metaClient);
    int numVersionsToKeep = 3 + compactedVersions; // should make the threshold configurable.
    if (allSnapshotVersions.size() > numVersionsToKeep) {
      allSnapshotVersions.sort((v1, v2) -> v2 - v1);
      List<Integer> versionsToKeep = allSnapshotVersions.subList(0, numVersionsToKeep);
      Set<String> filesToKeep = versionsToKeep.stream()
          .flatMap(version -> LSMTimeline.latestSnapshotManifest(metaClient, version).getFileNames().stream())
          .collect(Collectors.toSet());
      // delete the manifest file first
      List<String> manifestFilesToClean = new ArrayList<>();
      Arrays.stream(LSMTimeline.listAllManifestFiles(metaClient)).forEach(fileStatus -> {
        if (!versionsToKeep.contains(LSMTimeline.getManifestVersion(fileStatus.getPath().getName()))) {
          manifestFilesToClean.add(fileStatus.getPath().toString());
        }
      });
      FSUtils.deleteFilesParallelize(metaClient, manifestFilesToClean, context, config.getArchiveDeleteParallelism(), false);
      // delete the data files
      List<String> dataFilesToClean = Arrays.stream(LSMTimeline.listAllMetaFiles(metaClient))
          .filter(fileStatus -> !filesToKeep.contains(fileStatus.getPath().getName()))
          .map(fileStatus -> fileStatus.getPath().toString())
          .collect(Collectors.toList());
      FSUtils.deleteFilesParallelize(metaClient, dataFilesToClean, context, config.getArchiveDeleteParallelism(), false);
    }
  }

  private HoodieLSMTimelineManifest.LSMFileEntry getFileEntry(String fileName) throws IOException {
    long fileLen = metaClient.getFs().getFileStatus(new Path(metaClient.getArchivePath(), fileName)).getLen();
    return HoodieLSMTimelineManifest.LSMFileEntry.getInstance(fileName, fileLen);
  }

  /**
   * Returns at most {@code filesBatch} number of source files
   * restricted by the gross file size by 1GB.
   */
  private List<String> getCandidateFiles(List<HoodieLSMTimelineManifest.LSMFileEntry> files, int filesBatch) throws IOException {
    List<String> candidates = new ArrayList<>();
    long totalFileLen = 0L;
    for (int i = 0; i < filesBatch; i++) {
      HoodieLSMTimelineManifest.LSMFileEntry fileEntry = files.get(i);
      if (totalFileLen > MAX_FILE_SIZE_IN_BYTES) {
        return candidates;
      }
      // we may also need to consider a single file that is very close to the threshold in size,
      // to avoid the write amplification,
      // for e.g, two 800MB files compact into a 1.6GB file.
      totalFileLen += fileEntry.getFileLen();
      candidates.add(fileEntry.getFileName());
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

  private HoodieFileWriter openWriter(Path filePath) {
    try {
      return HoodieFileWriterFactory.getFileWriter("", filePath, metaClient.getHadoopConf(), getOrCreateWriterConfig(),
          HoodieLSMTimelineInstant.getClassSchema(), table.getTaskContextSupplier(), HoodieRecord.HoodieRecordType.AVRO);
    } catch (IOException e) {
      throw new HoodieException("Unable to initialize archiving writer", e);
    }
  }
}
