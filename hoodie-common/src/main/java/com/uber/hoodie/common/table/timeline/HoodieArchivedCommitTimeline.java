/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.timeline;

import com.google.common.io.Closeables;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Archived commit timeline. These commits are usually cleaned up and the meta data is archived for
 * future triaging
 *
 * @since 0.3.0
 */
public class HoodieArchivedCommitTimeline extends HoodieDefaultTimeline {
    private static final String HOODIE_COMMIT_ARCHIVE_LOG_FILE = "commits.archived";
    private transient Map<String, byte[]> readCommits = new HashMap<>();

    public HoodieArchivedCommitTimeline(FileSystem fs, String metaPath) {
        // Read back the commits to make sure
        Path archiveLogPath = getArchiveLogPath(metaPath);
        try {
            SequenceFile.Reader reader =
                new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(archiveLogPath));
            try {
                Text key = new Text();
                Text val = new Text();
                while (reader.next(key, val)) {
                    // TODO - limit the number of commits loaded in memory. this could get very large.
                    // This is okay because only tooling will load the archived commit timeline today
                    readCommits.put(key.toString(), Arrays.copyOf(val.getBytes(), val.getLength()));
                }
                this.instants = new ArrayList<>(readCommits.keySet());
                this.inflights = new ArrayList<>(0);
            } finally {
                Closeables.closeQuietly(reader);
            }
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not load archived commit timeline from path " + archiveLogPath, e);
        }
    }

    @Override
    public void saveInstantAsInflight(String instant) {
        throw new UnsupportedOperationException(
            "Could not save inflight instant in ArchivedTimeline " + instant);
    }

    @Override
    public void saveInstantAsComplete(String instant, Optional<byte[]> data) {
        throw new UnsupportedOperationException(
            "Could not save instant as complete in ArchivedTimeline " + instant);
    }

    @Override
    public void revertInstantToInflight(String instant) {
        throw new UnsupportedOperationException(
            "Could not revert instant in ArchivedTimeline " + instant);
    }

    @Override
    public void removeInflightFromTimeline(String instant) {
        throw new UnsupportedOperationException(
            "Could not delete inflight instant from ArchivedTimeline " + instant);
    }

    @Override
    public HoodieTimeline reload() throws IOException {
        return new HoodieArchivedCommitTimeline(fs, metaPath);
    }

    @Override
    public Optional<byte[]> readInstantDetails(String instant) {
        return Optional.ofNullable(readCommits.get(instant));
    }

    @Override
    protected String getInflightFileName(String instant) {
        throw new UnsupportedOperationException("No inflight filename for archived commits");
    }

    @Override
    protected String getCompletedFileName(String instant) {
        throw new UnsupportedOperationException("No inflight filename for archived commits");
    }

    @Override
    protected String getTimelineName() {
        return "archived-commits";
    }

    public static Path getArchiveLogPath(String metaPath) {
        return new Path(metaPath, HOODIE_COMMIT_ARCHIVE_LOG_FILE);
    }
}
