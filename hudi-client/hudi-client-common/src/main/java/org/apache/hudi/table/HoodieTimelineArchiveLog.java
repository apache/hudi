/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;

/**
 * Archiver to bound the growth of files under .hoodie meta path.
 */
public class HoodieTimelineArchiveLog<T extends HoodieAvroPayload, I, K, O> {

  private static final Logger LOG = LogManager.getLogger(HoodieTimelineArchiveLog.class);

  private final Path archiveFilePath;
  private final HoodieWriteConfig config;
  private Writer writer;
  private final int maxInstantsToKeep;
  private final int minInstantsToKeep;
  private final HoodieTable<T, I, K, O> table;
  private final HoodieTableMetaClient metaClient;

  public HoodieTimelineArchiveLog(HoodieWriteConfig config, HoodieTable<T, I, K, O> table) {
    this.config = config;
    this.table = table;
    this.metaClient = table.getMetaClient();
    this.archiveFilePath = HoodieArchivedTimeline.getArchiveLogPath(metaClient.getArchivePath());
    this.maxInstantsToKeep = config.getMaxCommitsToKeep();
    this.minInstantsToKeep = config.getMinCommitsToKeep();
  }

  private Writer openWriter() {
    try {
      if (this.writer == null) {
        return HoodieLogFormat.newWriterBuilder().onParentPath(archiveFilePath.getParent())
            .withFileId(archiveFilePath.getName()).withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
            .withFs(metaClient.getFs()).overBaseCommit("").build();
      } else {
        return this.writer;
      }
    } catch (IOException e) {
      throw new HoodieException("Unable to initialize HoodieLogFormat writer", e);
    }
  }

  private void close() {
    try {
      if (this.writer != null) {
        this.writer.close();
      }
    } catch (IOException e) {
      throw new HoodieException("Unable to close HoodieLogFormat writer", e);
    }
  }

  /**
   * Check if commits need to be archived. If yes, archive commits.
   */
  public boolean archiveIfRequired(HoodieEngineContext context) throws IOException {
    try {
      List<HoodieInstant> instantsToArchive = getInstantsToArchive().collect(Collectors.toList());

      boolean success = true;
      if (!instantsToArchive.isEmpty()) {
        this.writer = openWriter();
        LOG.info("Archiving instants " + instantsToArchive);
        archive(context, instantsToArchive);
        LOG.info("Deleting archived instants " + instantsToArchive);
        success = deleteArchivedInstants(instantsToArchive);
      } else {
        LOG.info("No Instants to archive");
      }

      return success;
    } finally {
      close();
    }
  }

  private Stream<HoodieInstant> getCleanInstantsToArchive() {
    HoodieTimeline cleanAndRollbackTimeline = table.getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.CLEAN_ACTION, HoodieTimeline.ROLLBACK_ACTION)).filterCompletedInstants();
    return cleanAndRollbackTimeline.getInstants()
        .collect(Collectors.groupingBy(HoodieInstant::getAction)).values().stream()
        .map(hoodieInstants -> {
          if (hoodieInstants.size() > this.maxInstantsToKeep) {
            return hoodieInstants.subList(0, hoodieInstants.size() - this.minInstantsToKeep);
          } else {
            return new ArrayList<HoodieInstant>();
          }
        }).flatMap(Collection::stream);
  }

  private Stream<HoodieInstant> getCommitInstantsToArchive() {
    // TODO (na) : Add a way to return actions associated with a timeline and then merge/unify
    // with logic above to avoid Stream.concats
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();
    Option<HoodieInstant> oldestPendingCompactionInstant =
        table.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();
    Option<HoodieInstant> oldestInflightCommitInstant =
        table.getActiveTimeline()
            .getTimelineOfActions(CollectionUtils.createSet(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION))
            .filterInflights().firstInstant();

    // We cannot have any holes in the commit timeline. We cannot archive any commits which are
    // made after the first savepoint present.
    Option<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
    if (!commitTimeline.empty() && commitTimeline.countInstants() > maxInstantsToKeep) {
      // Actually do the commits
      Stream<HoodieInstant> instantToArchiveStream = commitTimeline.getInstants()
          .filter(s -> {
            // if no savepoint present, then dont filter
            return !(firstSavepoint.isPresent() && HoodieTimeline.compareTimestamps(firstSavepoint.get().getTimestamp(), LESSER_THAN_OR_EQUALS, s.getTimestamp()));
          }).filter(s -> {
            // Ensure commits >= oldest pending compaction commit is retained
            return oldestPendingCompactionInstant
                .map(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), GREATER_THAN, s.getTimestamp()))
                .orElse(true);
          });
      // We need this to ensure that when multiple writers are performing conflict resolution, eligible instants don't
      // get archived, i.e, instants after the oldestInflight are retained on the timeline
      if (config.getFailedWritesCleanPolicy() == HoodieFailedWritesCleaningPolicy.LAZY) {
        instantToArchiveStream = instantToArchiveStream.filter(s -> oldestInflightCommitInstant.map(instant ->
            HoodieTimeline.compareTimestamps(instant.getTimestamp(), GREATER_THAN, s.getTimestamp()))
            .orElse(true));
      }
      return instantToArchiveStream.limit(commitTimeline.countInstants() - minInstantsToKeep);
    } else {
      return Stream.empty();
    }
  }

  private Stream<HoodieInstant> getInstantsToArchive() {
    Stream<HoodieInstant> instants = Stream.concat(getCleanInstantsToArchive(), getCommitInstantsToArchive());

    // For archiving and cleaning instants, we need to include intermediate state files if they exist
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    Map<Pair<String, String>, List<HoodieInstant>> groupByTsAction = rawActiveTimeline.getInstants()
        .collect(Collectors.groupingBy(i -> Pair.of(i.getTimestamp(),
            HoodieInstant.getComparableAction(i.getAction()))));

    // If metadata table is enabled, do not archive instants which are more recent that the last compaction on the
    // metadata table.
    if (config.isMetadataTableEnabled()) {
      try (HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(table.getContext(), config.getMetadataConfig(),
          config.getBasePath(), FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue())) {
        Option<String> latestCompactionTime = tableMetadata.getLatestCompactionTime();
        if (!latestCompactionTime.isPresent()) {
          LOG.info("Not archiving as there is no compaction yet on the metadata table");
          instants = Stream.empty();
        } else {
          LOG.info("Limiting archiving of instants to latest compaction on metadata table at " + latestCompactionTime.get());
          instants = instants.filter(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.LESSER_THAN,
              latestCompactionTime.get()));
        }
      } catch (Exception e) {
        throw new HoodieException("Error limiting instant archival based on metadata table", e);
      }
    }

    return instants.flatMap(hoodieInstant ->
        groupByTsAction.get(Pair.of(hoodieInstant.getTimestamp(),
            HoodieInstant.getComparableAction(hoodieInstant.getAction()))).stream());
  }

  private boolean deleteArchivedInstants(List<HoodieInstant> archivedInstants) throws IOException {
    LOG.info("Deleting instants " + archivedInstants);
    boolean success = true;
    for (HoodieInstant archivedInstant : archivedInstants) {
      Path commitFile = new Path(metaClient.getMetaPath(), archivedInstant.getFileName());
      try {
        if (metaClient.getFs().exists(commitFile)) {
          success &= metaClient.getFs().delete(commitFile, false);
          LOG.info("Archived and deleted instant file " + commitFile);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to delete archived instant " + archivedInstant, e);
      }
    }

    // Remove older meta-data from auxiliary path too
    Option<HoodieInstant> latestCommitted = Option.fromJavaOptional(archivedInstants.stream().filter(i -> i.isCompleted() && (i.getAction().equals(HoodieTimeline.COMMIT_ACTION)
        || (i.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)))).max(Comparator.comparing(HoodieInstant::getTimestamp)));
    LOG.info("Latest Committed Instant=" + latestCommitted);
    if (latestCommitted.isPresent()) {
      success &= deleteAllInstantsOlderorEqualsInAuxMetaFolder(latestCommitted.get());
    }
    return success;
  }

  /**
   * Remove older instants from auxiliary meta folder.
   *
   * @param thresholdInstant Hoodie Instant
   * @return success if all eligible file deleted successfully
   * @throws IOException in case of error
   */
  private boolean deleteAllInstantsOlderorEqualsInAuxMetaFolder(HoodieInstant thresholdInstant) throws IOException {
    List<HoodieInstant> instants = null;
    boolean success = true;
    try {
      instants =
          metaClient.scanHoodieInstantsFromFileSystem(
              new Path(metaClient.getMetaAuxiliaryPath()),
              HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE,
              false);
    } catch (FileNotFoundException e) {
      /*
       * On some FSs deletion of all files in the directory can auto remove the directory itself.
       * GCS is one example, as it doesn't have real directories and subdirectories. When client
       * removes all the files from a "folder" on GCS is has to create a special "/" to keep the folder
       * around. If this doesn't happen (timeout, misconfigured client, ...) folder will be deleted and
       * in this case we should not break when aux folder is not found.
       * GCS information: (https://cloud.google.com/storage/docs/gsutil/addlhelp/HowSubdirectoriesWork)
       */
      LOG.warn("Aux path not found. Skipping: " + metaClient.getMetaAuxiliaryPath());
      return success;
    }

    List<HoodieInstant> instantsToBeDeleted =
        instants.stream().filter(instant1 -> HoodieTimeline.compareTimestamps(instant1.getTimestamp(),
            LESSER_THAN_OR_EQUALS, thresholdInstant.getTimestamp())).collect(Collectors.toList());

    for (HoodieInstant deleteInstant : instantsToBeDeleted) {
      LOG.info("Deleting instant " + deleteInstant + " in auxiliary meta path " + metaClient.getMetaAuxiliaryPath());
      Path metaFile = new Path(metaClient.getMetaAuxiliaryPath(), deleteInstant.getFileName());
      if (metaClient.getFs().exists(metaFile)) {
        success &= metaClient.getFs().delete(metaFile, false);
        LOG.info("Deleted instant file in auxiliary metapath : " + metaFile);
      }
    }
    return success;
  }

  public void archive(HoodieEngineContext context, List<HoodieInstant> instants) throws HoodieCommitException {
    try {
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      LOG.info("Wrapper schema " + wrapperSchema.toString());
      List<IndexedRecord> records = new ArrayList<>();
      for (HoodieInstant hoodieInstant : instants) {
        try {
          deleteAnyLeftOverMarkers(context, hoodieInstant);
          records.add(convertToAvroRecord(hoodieInstant));
          if (records.size() >= this.config.getCommitArchivalBatchSize()) {
            writeToFile(wrapperSchema, records);
          }
        } catch (Exception e) {
          LOG.error("Failed to archive commits, .commit file: " + hoodieInstant.getFileName(), e);
          if (this.config.isFailOnTimelineArchivingEnabled()) {
            throw e;
          }
        }
      }
      writeToFile(wrapperSchema, records);
    } catch (Exception e) {
      throw new HoodieCommitException("Failed to archive commits", e);
    }
  }

  private void deleteAnyLeftOverMarkers(HoodieEngineContext context, HoodieInstant instant) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), table, instant.getTimestamp());
    if (writeMarkers.deleteMarkerDir(context, config.getMarkersDeleteParallelism())) {
      LOG.info("Cleaned up left over marker directory for instant :" + instant);
    }
  }

  private void writeToFile(Schema wrapperSchema, List<IndexedRecord> records) throws Exception {
    if (records.size() > 0) {
      Map<HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, wrapperSchema.toString());
      HoodieAvroDataBlock block = new HoodieAvroDataBlock(records, header);
      writer.appendBlock(block);
      records.clear();
    }
  }

  private IndexedRecord convertToAvroRecord(HoodieInstant hoodieInstant)
      throws IOException {
    return MetadataConversionUtils.createMetaWrapper(hoodieInstant, metaClient);
  }
}
