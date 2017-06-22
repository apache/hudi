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

import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.file.HoodieAppendLog;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.table.HoodieTable;
import java.util.Optional;
import java.util.function.Function;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Log to hold older historical commits, to bound the growth of .commit files
 */
public class HoodieCommitArchiveLog {
    private static Logger log = LogManager.getLogger(HoodieCommitArchiveLog.class);

    private final Path archiveFilePath;
    private final FileSystem fs;
    private final HoodieWriteConfig config;

    public HoodieCommitArchiveLog(HoodieWriteConfig config, FileSystem fs) {
        this.fs = fs;
        this.config = config;
        this.archiveFilePath = HoodieArchivedTimeline
            .getArchiveLogPath(config.getBasePath() + "/" + HoodieTableMetaClient.METAFOLDER_NAME);
    }

    /**
     * Check if commits need to be archived. If yes, archive commits.
     */
    public boolean archiveIfRequired() {
        List<HoodieInstant> commitsToArchive = getCommitsToArchive().collect(Collectors.toList());
        boolean success = true;
        if (commitsToArchive.iterator().hasNext()) {
            log.info("Archiving commits " + commitsToArchive);
            archive(commitsToArchive);
            success = deleteInstants(commitsToArchive);
        } else {
            log.info("No Commits to archive");
        }
        return success & deleteOtherInstants();
    }

    private boolean deleteOtherInstants() {
        // Delete clean and rollback files
        List<HoodieInstant> toDelete = getInstantsToDelete().collect(Collectors.toList());
        if(!toDelete.isEmpty()) {
            log.info("Deleting actions " + toDelete);
            return deleteInstants(toDelete);
        }
        return true;
    }

    private Stream<HoodieInstant> getInstantsToDelete() {

        int maxCommitsToKeep = config.getMaxCommitsToKeep();
        int minCommitsToKeep = config.getMinCommitsToKeep();

        HoodieTable table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieTimeline cleanTimeline = table.getActiveTimeline().getTimelineOfActions(Sets.newHashSet(HoodieTimeline.CLEAN_ACTION));
        if (!cleanTimeline.empty() && cleanTimeline.countInstants() > maxCommitsToKeep) {
            // Actually do the commits
            return cleanTimeline.getInstants().limit(cleanTimeline.countInstants() - minCommitsToKeep);
        }
        return Stream.empty();
    }

    private Stream<HoodieInstant> getCommitsToArchive() {

        int maxCommitsToKeep = config.getMaxCommitsToKeep();
        int minCommitsToKeep = config.getMinCommitsToKeep();

        HoodieTable table = HoodieTable.getHoodieTable(new HoodieTableMetaClient(fs, config.getBasePath(), true), config);
        HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();
        // We cannot have any holes in the commit timeline. We cannot archive any commits which are made after the first savepoint present.
        Optional<HoodieInstant> firstSavepoint = table.getCompletedSavepointTimeline().firstInstant();
        if (!commitTimeline.empty() && commitTimeline.countInstants() > maxCommitsToKeep) {
            // Actually do the commits
            return commitTimeline.getInstants().filter(s -> {
                // if no savepoint present, then dont filter
                return !(firstSavepoint.isPresent() && HoodieTimeline
                    .compareTimestamps(firstSavepoint.get().getTimestamp(), s.getTimestamp(),
                        HoodieTimeline.LESSER_OR_EQUAL));
            }).limit(commitTimeline.countInstants() - minCommitsToKeep);
        }
        return Stream.empty();
    }

    private boolean deleteInstants(List<HoodieInstant> commitsToArchive) {
        log.info("Deleting instant " + commitsToArchive);
        HoodieTableMetaClient metaClient =
            new HoodieTableMetaClient(fs, config.getBasePath(), true);

        boolean success = true;
        for (HoodieInstant commitToArchive : commitsToArchive) {
            Path commitFile =
                new Path(metaClient.getMetaPath(), commitToArchive.getFileName());
            try {
                if (fs.exists(commitFile)) {
                    success &= fs.delete(commitFile, false);
                    log.info("Archived and deleted instant file " + commitFile);
                }
            } catch (IOException e) {
                throw new HoodieIOException("Failed to delete archived instant " + commitToArchive,
                    e);
            }
        }
        return success;
    }

    private HoodieAppendLog.Writer openWriter() throws IOException {
        log.info("Opening archive file at path: " + archiveFilePath);
        return HoodieAppendLog
            .createWriter(fs.getConf(), HoodieAppendLog.Writer.file(archiveFilePath),
                HoodieAppendLog.Writer.keyClass(Text.class),
                HoodieAppendLog.Writer.appendIfExists(true),
                HoodieAppendLog.Writer.valueClass(Text.class), HoodieAppendLog.Writer
                    .compression(HoodieAppendLog.CompressionType.RECORD, new BZip2Codec()));
    }

    private void archive(List<HoodieInstant> commits) throws HoodieCommitException {
        HoodieTableMetaClient metaClient =
            new HoodieTableMetaClient(fs, config.getBasePath(), true);
        HoodieTimeline commitTimeline =
            metaClient.getActiveTimeline().getCommitsAndCompactionsTimeline().filterCompletedInstants();

        HoodieAppendLog.Writer writer = null;
        try {
            writer = openWriter();
            for (HoodieInstant commitTime : commits) {
                Text k = new Text(commitTime.getTimestamp());
                HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                    .fromBytes(commitTimeline.getInstantDetails(commitTime).get());
                Text v = new Text(commitMetadata.toJsonString());
                writer.append(k, v);
                log.info("Wrote " + k);
            }
        } catch (IOException e) {
            throw new HoodieCommitException("Could not archive commits " + commits, e);
        } finally {
            if (writer != null) {
                try {
                    writer.hsync();
                    writer.close();
                } catch (IOException e) {
                    throw new HoodieCommitException(
                        "Could not close the archive commits writer " + commits, e);
                }
            }
        }
    }

    public Path getArchiveFilePath() {
        return archiveFilePath;
    }
}
