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

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.file.HoodieAppendLog;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Log to hold older historical commits, to bound the growth of .commit files
 */
public class HoodieCommitArchiveLog {
    private static Logger log = LogManager.getLogger(HoodieCommitArchiveLog.class);
    private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE = "commits.archived";

    private final Path archiveFilePath;
    private final FileSystem fs;
    private final HoodieWriteConfig config;

    public HoodieCommitArchiveLog(HoodieWriteConfig config) {
        this.archiveFilePath =
            new Path(config.getBasePath(),
                    HoodieTableMetadata.METAFOLDER_NAME + "/" +HOODIE_COMMIT_ARCHIVE_LOG_FILE);
        this.fs = FSUtils.getFs();
        this.config = config;
    }

    /**
     * Check if commits need to be archived. If yes, archive commits.
     */
    public boolean archiveIfRequired() {
        HoodieTableMetadata metadata = new HoodieTableMetadata(fs, config.getBasePath());
        List<String> commitsToArchive = getCommitsToArchive(metadata);
        if (!commitsToArchive.isEmpty()) {
            log.info("Archiving commits " + commitsToArchive);
            archive(metadata, commitsToArchive);
            return deleteCommits(metadata, commitsToArchive);
        } else {
            log.info("No Commits to archive");
            return true;
        }
    }

    private List<String> getCommitsToArchive(HoodieTableMetadata metadata) {
        int maxCommitsToKeep = config.getMaxCommitsToKeep();
        int minCommitsToKeep = config.getMinCommitsToKeep();

        List<String> commits = metadata.getAllCommits().getCommitList();
        List<String> commitsToArchive = new ArrayList<String>();
        if (commits.size() > maxCommitsToKeep) {
            // Actually do the commits
            commitsToArchive = commits.subList(0, commits.size() - minCommitsToKeep);
        }
        return commitsToArchive;
    }

    private boolean deleteCommits(HoodieTableMetadata metadata, List<String> commitsToArchive) {
        log.info("Deleting commits " + commitsToArchive);
        boolean success = true;
        for(String commitToArchive:commitsToArchive) {
            Path commitFile =
                new Path(metadata.getBasePath() + "/" +
                        HoodieTableMetadata.METAFOLDER_NAME + "/" +
                        FSUtils.makeCommitFileName(commitToArchive));
            try {
                if (fs.exists(commitFile)) {
                    success &= fs.delete(commitFile, false);
                    log.info("Archived and deleted commit file " + commitFile);
                }
            } catch (IOException e) {
                throw new HoodieIOException(
                    "Failed to delete archived commit " + commitToArchive, e);
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

    private void archive(HoodieTableMetadata metadata, List<String> commits)
        throws HoodieCommitException {
        HoodieAppendLog.Writer writer = null;
        try {
            writer = openWriter();
            for (String commitTime : commits) {
                Text k = new Text(commitTime);
                Text v = new Text(metadata.getCommitMetadata(commitTime).toJsonString());
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
