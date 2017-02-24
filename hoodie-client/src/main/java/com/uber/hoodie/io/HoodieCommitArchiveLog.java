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

import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieArchivedTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.file.HoodieAppendLog;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
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
        if (commitsToArchive.iterator().hasNext()) {
            log.info("Archiving commits " + commitsToArchive);
            archive(commitsToArchive);
            return deleteCommits(commitsToArchive);
        } else {
            log.info("No Commits to archive");
            return true;
        }
    }

    private Stream<HoodieInstant> getCommitsToArchive() {
        int maxCommitsToKeep = config.getMaxCommitsToKeep();
        int minCommitsToKeep = config.getMinCommitsToKeep();

        HoodieTableMetaClient metaClient =
            new HoodieTableMetaClient(fs, config.getBasePath(), true);
        HoodieTimeline commitTimeline =
            metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

        if (!commitTimeline.empty() && commitTimeline.countInstants() > maxCommitsToKeep) {
            // Actually do the commits
            return commitTimeline.getInstants()
                .limit(commitTimeline.countInstants() - minCommitsToKeep);
        }
        return Stream.empty();
    }

    private boolean deleteCommits(List<HoodieInstant> commitsToArchive) {
        log.info("Deleting commits " + commitsToArchive);
        HoodieTableMetaClient metaClient =
            new HoodieTableMetaClient(fs, config.getBasePath(), true);
        HoodieTimeline commitTimeline =
            metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

        boolean success = true;
        for (HoodieInstant commitToArchive : commitsToArchive) {
            Path commitFile =
                new Path(metaClient.getMetaPath(), commitToArchive.getFileName());
            try {
                if (fs.exists(commitFile)) {
                    success &= fs.delete(commitFile, false);
                    log.info("Archived and deleted commit file " + commitFile);
                }
            } catch (IOException e) {
                throw new HoodieIOException("Failed to delete archived commit " + commitToArchive,
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
            metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

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
