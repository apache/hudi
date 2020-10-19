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

import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
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
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class HoodieTimelineArchiveLog {

  private static final Logger LOG = LogManager.getLogger(HoodieTimelineArchiveLog.class);

  private final Path archiveFilePath;
  private final HoodieWriteConfig config;
  private Writer writer;
  private final int maxInstantsToKeep;
  private final int minInstantsToKeep;
  private final HoodieTable<?> table;
  private final HoodieTableMetaClient metaClient;

  public HoodieTimelineArchiveLog(HoodieWriteConfig config, Configuration configuration) {
    this.config = config;
    this.table = HoodieTable.create(config, configuration);
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
    } catch (InterruptedException | IOException e) {
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
  public boolean archiveIfRequired(JavaSparkContext jsc) throws IOException {
    try {
      List<HoodieInstant> instantsToArchive = getInstantsToArchive().collect(Collectors.toList());

      boolean success = true;
      if (!instantsToArchive.isEmpty()) {
        this.writer = openWriter();
        LOG.info("Archiving instants " + instantsToArchive);
        archive(jsc, instantsToArchive);
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
        .getTimelineOfActions(Collections.singleton(HoodieTimeline.CLEAN_ACTION)).filterCompletedInstants();
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

    // We cannot have any holes in the commit timeline. We cannot archive any commits which are
    // made after the first savepoint present.
    Option<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
    if (!commitTimeline.empty() && commitTimeline.countInstants() > maxInstantsToKeep) {
      // Actually do the commits
      return commitTimeline.getInstants()
          .filter(s -> {
            // if no savepoint present, then dont filter
            return !(firstSavepoint.isPresent() && HoodieTimeline.compareTimestamps(firstSavepoint.get().getTimestamp(), LESSER_THAN_OR_EQUALS, s.getTimestamp()));
          }).filter(s -> {
            // Ensure commits >= oldest pending compaction commit is retained
            return oldestPendingCompactionInstant
                .map(instant -> HoodieTimeline.compareTimestamps(instant.getTimestamp(), GREATER_THAN, s.getTimestamp()))
                .orElse(true);
          }).limit(commitTimeline.countInstants() - minInstantsToKeep);
    } else {
      return Stream.empty();
    }
  }

  private Stream<HoodieInstant> getInstantsToArchive() {
    // TODO: Handle ROLLBACK_ACTION in future
    Stream<HoodieInstant> instants = Stream.concat(getCleanInstantsToArchive(), getCommitInstantsToArchive());

    // For archiving and cleaning instants, we need to include intermediate state files if they exist
    HoodieActiveTimeline rawActiveTimeline = new HoodieActiveTimeline(metaClient, false);
    Map<Pair<String, String>, List<HoodieInstant>> groupByTsAction = rawActiveTimeline.getInstants()
        .collect(Collectors.groupingBy(i -> Pair.of(i.getTimestamp(),
            HoodieInstant.getComparableAction(i.getAction()))));

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

  public void archive(JavaSparkContext jsc, List<HoodieInstant> instants) throws HoodieCommitException {
    try {
      HoodieTimeline commitTimeline = metaClient.getActiveTimeline().getAllCommitsTimeline().filterCompletedInstants();
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      LOG.info("Wrapper schema " + wrapperSchema.toString());
      List<IndexedRecord> records = new ArrayList<>();
      for (HoodieInstant hoodieInstant : instants) {
        try {
          deleteAnyLeftOverMarkerFiles(jsc, hoodieInstant);
          records.add(convertToAvroRecord(commitTimeline, hoodieInstant));
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

  private void deleteAnyLeftOverMarkerFiles(JavaSparkContext jsc, HoodieInstant instant) {
    MarkerFiles markerFiles = new MarkerFiles(table, instant.getTimestamp());
    if (markerFiles.deleteMarkerDir(jsc, config.getMarkersDeleteParallelism())) {
      LOG.info("Cleaned up left over marker directory for instant :" + instant);
    }
  }

  private void writeToFile(Schema wrapperSchema, List<IndexedRecord> records) throws Exception {
    if (records.size() > 0) {
      Map<HeaderMetadataType, String> header = new HashMap<>();
      header.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, wrapperSchema.toString());
      HoodieAvroDataBlock block = new HoodieAvroDataBlock(records, header);
      this.writer = writer.appendBlock(block);
      records.clear();
    }
  }

  private IndexedRecord convertToAvroRecord(HoodieTimeline commitTimeline, HoodieInstant hoodieInstant)
      throws IOException {
    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION: {
        if (hoodieInstant.isCompleted()) {
          archivedMetaWrapper.setHoodieCleanMetadata(CleanerUtils.getCleanerMetadata(metaClient, hoodieInstant));
        } else {
          archivedMetaWrapper.setHoodieCleanerPlan(CleanerUtils.getCleanerPlan(metaClient, hoodieInstant));
        }
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      }
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(convertCommitMetadata(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        archivedMetaWrapper.setHoodieRollbackMetadata(TimelineMetadataUtils.deserializeAvroMetadata(
            commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieRollbackMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      }
      case HoodieTimeline.SAVEPOINT_ACTION: {
        archivedMetaWrapper.setHoodieSavePointMetadata(TimelineMetadataUtils.deserializeAvroMetadata(
            commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieSavepointMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.savepoint.name());
        break;
      }
      case HoodieTimeline.COMPACTION_ACTION: {
        HoodieCompactionPlan plan = CompactionUtils.getCompactionPlan(metaClient, hoodieInstant.getTimestamp());
        archivedMetaWrapper.setHoodieCompactionPlan(plan);
        archivedMetaWrapper.setActionType(ActionType.compaction.name());
        break;
      }
      default: {
        throw new UnsupportedOperationException("Action not fully supported yet");
      }
    }
    return archivedMetaWrapper;
  }

  public static org.apache.hudi.avro.model.HoodieCommitMetadata convertCommitMetadata(
      HoodieCommitMetadata hoodieCommitMetadata) {
    ObjectMapper mapper = new ObjectMapper();
    // Need this to ignore other public get() methods
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    org.apache.hudi.avro.model.HoodieCommitMetadata avroMetaData =
        mapper.convertValue(hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
    // Do not archive Rolling Stats, cannot set to null since AVRO will throw null pointer
    avroMetaData.getExtraMetadata().put(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, "");
    return avroMetaData;
  }
}
