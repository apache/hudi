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

package org.apache.hudi.io;

import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieArchivedLogFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Archiver to bound the growth of <action>.commit files.
 */
public class HoodieCommitArchiveLog {

  private static Logger log = LogManager.getLogger(HoodieCommitArchiveLog.class);

  private final Path archiveFilePath;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig config;
  private Writer writer;

  public HoodieCommitArchiveLog(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.metaClient = metaClient;
    this.archiveFilePath = HoodieArchivedTimeline.getArchiveLogPath(metaClient.getArchivePath());
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
  public boolean archiveIfRequired(final JavaSparkContext jsc) throws IOException {
    try {
      List<HoodieInstant> instantsToArchive = getInstantsToArchive(jsc).collect(Collectors.toList());
      boolean success = true;
      if (instantsToArchive.iterator().hasNext()) {
        this.writer = openWriter();
        log.info("Archiving instants " + instantsToArchive);
        archive(instantsToArchive);
        success = deleteArchivedInstants(instantsToArchive);
      } else {
        log.info("No Instants to archive");
      }
      return success;
    } finally {
      close();
    }
  }

  private Stream<HoodieInstant> getInstantsToArchive(JavaSparkContext jsc) {

    // TODO : rename to max/minInstantsToKeep
    int maxCommitsToKeep = config.getMaxCommitsToKeep();
    int minCommitsToKeep = config.getMinCommitsToKeep();

    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    // GroupBy each action and limit each action timeline to maxCommitsToKeep
    // TODO: Handle ROLLBACK_ACTION in future
    // ROLLBACK_ACTION is currently not defined in HoodieActiveTimeline
    HoodieTimeline cleanAndRollbackTimeline = table.getActiveTimeline()
        .getTimelineOfActions(Sets.newHashSet(HoodieTimeline.CLEAN_ACTION)).filterCompletedInstants();
    Stream<HoodieInstant> instants = cleanAndRollbackTimeline.getInstants()
        .collect(Collectors.groupingBy(s -> s.getAction())).entrySet().stream().map(i -> {
          if (i.getValue().size() > maxCommitsToKeep) {
            return i.getValue().subList(0, i.getValue().size() - minCommitsToKeep);
          } else {
            return new ArrayList<HoodieInstant>();
          }
        }).flatMap(i -> i.stream());

    // TODO (na) : Add a way to return actions associated with a timeline and then merge/unify
    // with logic above to avoid Stream.concats
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();
    Option<HoodieInstant> oldestPendingCompactionInstant =
        table.getActiveTimeline().filterPendingCompactionTimeline().firstInstant();

    // We cannot have any holes in the commit timeline. We cannot archive any commits which are
    // made after the first savepoint present.
    Option<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
    if (!commitTimeline.empty() && commitTimeline.countInstants() > maxCommitsToKeep) {
      // Actually do the commits
      instants = Stream.concat(instants, commitTimeline.getInstants().filter(s -> {
        // if no savepoint present, then dont filter
        return !(firstSavepoint.isPresent() && HoodieTimeline.compareTimestamps(firstSavepoint.get().getTimestamp(),
            s.getTimestamp(), HoodieTimeline.LESSER_OR_EQUAL));
      }).filter(s -> {
        // Ensure commits >= oldest pending compaction commit is retained
        return oldestPendingCompactionInstant.map(instant -> {
          return HoodieTimeline.compareTimestamps(instant.getTimestamp(), s.getTimestamp(), HoodieTimeline.GREATER);
        }).orElse(true);
      }).limit(commitTimeline.countInstants() - minCommitsToKeep));
    }

    return instants;
  }

  private boolean deleteArchivedInstants(List<HoodieInstant> archivedInstants) throws IOException {
    log.info("Deleting instants " + archivedInstants);
    boolean success = true;
    for (HoodieInstant archivedInstant : archivedInstants) {
      Path commitFile = new Path(metaClient.getMetaPath(), archivedInstant.getFileName());
      try {
        if (metaClient.getFs().exists(commitFile)) {
          success &= metaClient.getFs().delete(commitFile, false);
          log.info("Archived and deleted instant file " + commitFile);
        }
      } catch (IOException e) {
        throw new HoodieIOException("Failed to delete archived instant " + archivedInstant, e);
      }
    }

    // Remove older meta-data from auxiliary path too
    Option<HoodieInstant> latestCommitted = Option.fromJavaOptional(archivedInstants.stream().filter(i -> {
      return i.isCompleted() && (i.getAction().equals(HoodieTimeline.COMMIT_ACTION)
          || (i.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)));
    }).max(Comparator.comparing(HoodieInstant::getTimestamp)));
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
    List<HoodieInstant> instants = HoodieTableMetaClient.scanHoodieInstantsFromFileSystem(metaClient.getFs(),
        new Path(metaClient.getMetaAuxiliaryPath()), HoodieActiveTimeline.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE);

    List<HoodieInstant> instantsToBeDeleted =
        instants.stream().filter(instant1 -> HoodieTimeline.compareTimestamps(instant1.getTimestamp(),
            thresholdInstant.getTimestamp(), HoodieTimeline.LESSER_OR_EQUAL)).collect(Collectors.toList());

    boolean success = true;
    for (HoodieInstant deleteInstant : instantsToBeDeleted) {
      log.info("Deleting instant " + deleteInstant + " in auxiliary meta path " + metaClient.getMetaAuxiliaryPath());
      Path metaFile = new Path(metaClient.getMetaAuxiliaryPath(), deleteInstant.getFileName());
      if (metaClient.getFs().exists(metaFile)) {
        success &= metaClient.getFs().delete(metaFile, false);
        log.info("Deleted instant file in auxiliary metapath : " + metaFile);
      }
    }
    return success;
  }

  public void archive(List<HoodieInstant> instants) throws HoodieCommitException {
    try {
      HoodieTimeline commitTimeline = metaClient.getActiveTimeline().getAllCommitsTimeline().filterCompletedInstants();
      Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
      log.info("Wrapper schema " + wrapperSchema.toString());
      List<IndexedRecord> records = new ArrayList<>();
      for (HoodieInstant hoodieInstant : instants) {
        try {
          records.add(convertToAvroRecord(commitTimeline, hoodieInstant));
          if (records.size() >= this.config.getCommitArchivalBatchSize()) {
            writeToFile(wrapperSchema, records);
          }
        } catch (Exception e) {
          log.error("Failed to archive commits, .commit file: " + hoodieInstant.getFileName(), e);
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

  public Path getArchiveFilePath() {
    return archiveFilePath;
  }

  private void writeToFile(Schema wrapperSchema, List<IndexedRecord> records) throws Exception {
    if (records.size() > 0) {
      Map<HeaderMetadataType, String> header = Maps.newHashMap();
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
    switch (hoodieInstant.getAction()) {
      case HoodieTimeline.CLEAN_ACTION: {
        archivedMetaWrapper.setHoodieCleanMetadata(AvroUtils
            .deserializeAvroMetadata(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieCleanMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.clean.name());
        break;
      }
      case HoodieTimeline.COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(commitMetadataConverter(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      case HoodieTimeline.ROLLBACK_ACTION: {
        archivedMetaWrapper.setHoodieRollbackMetadata(AvroUtils.deserializeAvroMetadata(
            commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieRollbackMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.rollback.name());
        break;
      }
      case HoodieTimeline.SAVEPOINT_ACTION: {
        archivedMetaWrapper.setHoodieSavePointMetadata(AvroUtils.deserializeAvroMetadata(
            commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieSavepointMetadata.class));
        archivedMetaWrapper.setActionType(ActionType.savepoint.name());
        break;
      }
      case HoodieTimeline.DELTA_COMMIT_ACTION: {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieCommitMetadata.class);
        archivedMetaWrapper.setHoodieCommitMetadata(commitMetadataConverter(commitMetadata));
        archivedMetaWrapper.setActionType(ActionType.commit.name());
        break;
      }
      default:
        throw new UnsupportedOperationException("Action not fully supported yet");
    }
    return archivedMetaWrapper;
  }

  private org.apache.hudi.avro.model.HoodieCommitMetadata commitMetadataConverter(
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
