/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.uber.hoodie.avro.model.HoodieArchivedMetaEntry;
import com.uber.hoodie.avro.model.HoodieCleanMetadata;
import com.uber.hoodie.avro.model.HoodieRollbackMetadata;
import com.uber.hoodie.avro.model.HoodieSavepointMetadata;
import com.uber.hoodie.common.model.ActionType;
import com.uber.hoodie.common.model.HoodieArchivedLogFile;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieCompactionMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/**
 * Archiver to bound the growth of <action>.commit files
 */
public class HoodieCommitArchiveLog {
    private static Logger log = LogManager.getLogger(HoodieCommitArchiveLog.class);

    private final Path archiveFilePath;
    private final FileSystem fs;
    private final HoodieWriteConfig config;
    private HoodieLogFormat.Writer writer;

    public HoodieCommitArchiveLog(HoodieWriteConfig config, FileSystem fs) {
        this.fs = fs;
        this.config = config;
        this.archiveFilePath = HoodieArchivedTimeline
            .getArchiveLogPath(config.getBasePath() + "/" + HoodieTableMetaClient.METAFOLDER_NAME);
    }

    private HoodieLogFormat.Writer openWriter() {
        try {
            if(this.writer == null) {
                return HoodieLogFormat.newWriterBuilder()
                        .onParentPath(archiveFilePath.getParent())
                        .withFileId(archiveFilePath.getName())
                        .withFileExtension(HoodieArchivedLogFile.ARCHIVE_EXTENSION)
                        .withFs(fs)
                        .overBaseCommit("").build();
            } else {
                return this.writer;
            }
        } catch(InterruptedException | IOException e) {
            throw new HoodieException("Unable to initialize HoodieLogFormat writer", e);
        }
    }

    private void close() {
        try {
            if(this.writer != null) {
                this.writer.close();
            }
        } catch(IOException e) {
            throw new HoodieException("Unable to close HoodieLogFormat writer", e);
        }
    }

    /**
     * Check if commits need to be archived. If yes, archive commits.
     */
    public boolean archiveIfRequired() {
        try {
            List<HoodieInstant> instantsToArchive = getInstantsToArchive().collect(Collectors.toList());
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

    private Stream<HoodieInstant> getInstantsToArchive() {

        // TODO : rename to max/minInstantsToKeep
        int maxCommitsToKeep = config.getMaxCommitsToKeep();
        int minCommitsToKeep = config.getMinCommitsToKeep();

        HoodieTable table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);

        // GroupBy each action and limit each action timeline to maxCommitsToKeep
        HoodieTimeline cleanAndRollbackTimeline = table.getActiveTimeline().getTimelineOfActions(Sets.newHashSet(HoodieTimeline.CLEAN_ACTION,
                HoodieTimeline.ROLLBACK_ACTION));
        Stream<HoodieInstant> instants = cleanAndRollbackTimeline.getInstants()
            .collect(Collectors.groupingBy(s -> s.getAction()))
            .entrySet()
            .stream()
            .map(i -> {
                if (i.getValue().size() > maxCommitsToKeep) {
                    return i.getValue().subList(0, i.getValue().size() - minCommitsToKeep);
                } else {
                    return new ArrayList<HoodieInstant>();
                }
            })
            .flatMap(i -> i.stream());

        //TODO (na) : Add a way to return actions associated with a timeline and then merge/unify with logic above to avoid Stream.concats
        HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();
        // We cannot have any holes in the commit timeline. We cannot archive any commits which are made after the first savepoint present.
        Optional<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
        if (!commitTimeline.empty() && commitTimeline.countInstants() > maxCommitsToKeep) {
            // Actually do the commits
            instants = Stream.concat(instants, commitTimeline.getInstants().filter(s -> {
                // if no savepoint present, then dont filter
                return !(firstSavepoint.isPresent() && HoodieTimeline
                    .compareTimestamps(firstSavepoint.get().getTimestamp(), s.getTimestamp(),
                        HoodieTimeline.LESSER_OR_EQUAL));
            }).limit(commitTimeline.countInstants() - minCommitsToKeep));
        }

        return instants;
    }

    private boolean deleteArchivedInstants(List<HoodieInstant> archivedInstants) {
        log.info("Deleting instants " + archivedInstants);
        HoodieTableMetaClient metaClient =
                new HoodieTableMetaClient(fs, config.getBasePath(), true);

        boolean success = true;
        for (HoodieInstant archivedInstant : archivedInstants) {
            Path commitFile =
                new Path(metaClient.getMetaPath(), archivedInstant.getFileName());
            try {
                if (fs.exists(commitFile)) {
                    success &= fs.delete(commitFile, false);
                    log.info("Archived and deleted instant file " + commitFile);
                }
            } catch (IOException e) {
                throw new HoodieIOException("Failed to delete archived instant " + archivedInstant,
                    e);
            }
        }
        return success;
    }

    public void archive(List<HoodieInstant> instants) throws HoodieCommitException {

        try {
            HoodieTableMetaClient metaClient =
                    new HoodieTableMetaClient(fs, config.getBasePath(), true);
            HoodieTimeline commitTimeline =
                    metaClient.getActiveTimeline().getAllCommitsTimeline().filterCompletedInstants();

            Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
            log.info("Wrapper schema " + wrapperSchema.toString());
            List<IndexedRecord> records = new ArrayList<>();
            for (HoodieInstant hoodieInstant : instants) {
                records.add(convertToAvroRecord(commitTimeline, hoodieInstant));
            }
            HoodieAvroDataBlock block = new HoodieAvroDataBlock(records, wrapperSchema);
            this.writer = writer.appendBlock(block);
        } catch(Exception e) {
            throw new HoodieCommitException("Failed to archive commits", e);
        }
    }

    public Path getArchiveFilePath() {
        return archiveFilePath;
    }

    private IndexedRecord convertToAvroRecord(HoodieTimeline commitTimeline, HoodieInstant hoodieInstant) throws IOException {
        HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
        archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
        switch(hoodieInstant.getAction()) {
            case HoodieTimeline.CLEAN_ACTION:{
                archivedMetaWrapper.setHoodieCleanMetadata(AvroUtils.deserializeAvroMetadata(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieCleanMetadata.class));
                archivedMetaWrapper.setActionType(ActionType.clean.name());
                break;
            }
            case HoodieTimeline.COMMIT_ACTION:{
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                        .fromBytes(commitTimeline.getInstantDetails(hoodieInstant).get());
                archivedMetaWrapper.setHoodieCommitMetadata(commitMetadataConverter(commitMetadata));
                archivedMetaWrapper.setActionType(ActionType.commit.name());
                break;
            }
            case HoodieTimeline.COMPACTION_ACTION:{
                com.uber.hoodie.common.model.HoodieCompactionMetadata compactionMetadata = com.uber.hoodie.common.model.HoodieCompactionMetadata
                        .fromBytes(commitTimeline.getInstantDetails(hoodieInstant).get());
                archivedMetaWrapper.setHoodieCompactionMetadata(compactionMetadataConverter(compactionMetadata));
                archivedMetaWrapper.setActionType(ActionType.compaction.name());
                break;
            }
            case HoodieTimeline.ROLLBACK_ACTION:{
                archivedMetaWrapper.setHoodieRollbackMetadata(AvroUtils.deserializeAvroMetadata(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieRollbackMetadata.class));
                archivedMetaWrapper.setActionType(ActionType.rollback.name());
                break;
            }
            case HoodieTimeline.SAVEPOINT_ACTION:{
                archivedMetaWrapper.setHoodieSavePointMetadata(AvroUtils.deserializeAvroMetadata(commitTimeline.getInstantDetails(hoodieInstant).get(), HoodieSavepointMetadata.class));
                archivedMetaWrapper.setActionType(ActionType.savepoint.name());
                break;
            }
            case HoodieTimeline.DELTA_COMMIT_ACTION:{
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                        .fromBytes(commitTimeline.getInstantDetails(hoodieInstant).get());
                archivedMetaWrapper.setHoodieCommitMetadata(commitMetadataConverter(commitMetadata));
                archivedMetaWrapper.setActionType(ActionType.commit.name());
                break;
            }
        }
        return archivedMetaWrapper;
    }

    private com.uber.hoodie.avro.model.HoodieCommitMetadata commitMetadataConverter(HoodieCommitMetadata hoodieCommitMetadata) {
        ObjectMapper mapper = new ObjectMapper();
        //Need this to ignore other public get() methods
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        com.uber.hoodie.avro.model.HoodieCommitMetadata avroMetaData =
                mapper.convertValue(hoodieCommitMetadata, com.uber.hoodie.avro.model.HoodieCommitMetadata.class);
        return avroMetaData;
    }

    private com.uber.hoodie.avro.model.HoodieCompactionMetadata compactionMetadataConverter(HoodieCompactionMetadata hoodieCompactionMetadata) {
         ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        com.uber.hoodie.avro.model.HoodieCompactionMetadata avroMetaData = mapper.convertValue(hoodieCompactionMetadata,
                 com.uber.hoodie.avro.model.HoodieCompactionMetadata.class);
        return avroMetaData;
    }
}
