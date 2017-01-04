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
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HoodieTimeline allows representation of meta-data events as a timeline.
 * Instants are specific points in time represented as strings.
 * in this format YYYYMMDDHHmmSS. e.g. 20170101193218
 * Any operation on the timeline starts with the inflight instant and then when complete marks
 * the completed instant and removes the inflight instant.
 * Completed instants are plainly referred to as just instants
 * <p>
 * Timelines as immutable once created. Any operation to change the timeline (like create/delete instants)
 * will not be reflected unless explicitly reloaded using the reload()
 *
 * @see com.uber.hoodie.common.table.HoodieTableMetaClient
 * @see HoodieTimeline
 * @since 0.3.0
 */
public abstract class HoodieDefaultTimeline implements HoodieTimeline {
    private final transient static Logger log = LogManager.getLogger(HoodieDefaultTimeline.class);

    public static final String INFLIGHT_EXTENSION = ".inflight";
    protected String metaPath;
    protected transient FileSystem fs;
    protected List<String> inflights;
    protected List<String> instants;

    public HoodieDefaultTimeline(FileSystem fs, String metaPath, String fileExtension) {
        String completedInstantExtension = fileExtension;
        String inflightInstantExtension = fileExtension + INFLIGHT_EXTENSION;

        FileStatus[] fileStatuses;
        try {
            fileStatuses = HoodieTableMetaClient.scanFiles(fs, new Path(metaPath),
                path -> path.toString().endsWith(completedInstantExtension) || path.toString()
                    .endsWith(inflightInstantExtension));
        } catch (IOException e) {
            throw new HoodieIOException("Failed to scan metadata", e);
        }
        this.instants = Arrays.stream(fileStatuses)
            .filter(status -> status.getPath().getName().endsWith(completedInstantExtension))
            .map(fileStatus -> fileStatus.getPath().getName().replaceAll(completedInstantExtension, ""))
            .sorted().collect(Collectors.toList());
        this.inflights = Arrays.stream(fileStatuses).filter(
            status -> status.getPath().getName().endsWith(inflightInstantExtension)).map(
            fileStatus -> fileStatus.getPath().getName()
                .replaceAll(inflightInstantExtension, "")).sorted()
            .collect(Collectors.toList());
        this.fs = fs;
        this.metaPath = metaPath;
    }

    public HoodieDefaultTimeline(Stream<String> instants, Stream<String> inflights) {
        this.instants = instants.collect(Collectors.toList());
        this.inflights = inflights.collect(Collectors.toList());
    }

    /**
     * This constructor only supports backwards compatibility in inflight commits in ActiveCommitTimeline.
     * This should never be used.
     *
     * @param fs
     * @param metaPath
     * @deprecated
     */
    public HoodieDefaultTimeline(FileSystem fs, String metaPath) {
        this.fs = fs;
        this.metaPath = metaPath;
    }

    /**
     * For serailizing and de-serializing
     * @deprecated
     */
    public HoodieDefaultTimeline() {
    }


    /**
     * This method is only used when this object is deserialized in a spark executor.
     * @deprecated
     */
    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.fs = FSUtils.getFs();
    }

    @Override
    public Stream<String> findInstantsInRange(String startTs, String endTs) {
        return instants.stream().filter(
            s -> compareInstants(s, startTs, GREATER) && compareInstants(s, endTs,
                LESSER_OR_EQUAL));
    }

    @Override
    public Stream<String> findInstantsAfter(String commitTime, int numCommits) {
        return instants.stream().filter(s -> compareInstants(s, commitTime, GREATER))
            .limit(numCommits);
    }

    @Override
    public boolean hasInstants() {
        return instants.stream().count() != 0;
    }

    @Override
    public boolean hasInflightInstants() {
        return inflights.stream().count() != 0;
    }

    @Override
    public int getTotalInstants() {
        return new Long(instants.stream().count()).intValue();
    }

    @Override
    public Optional<String> firstInstant() {
        return instants.stream().findFirst();
    }

    @Override
    public Optional<String> nthInstant(int n) {
        if(!hasInstants() || n >= getTotalInstants()) {
            return Optional.empty();
        }
        return Optional.of(instants.get(n));
    }

    @Override
    public Optional<String> lastInstant() {
        return hasInstants() ? nthInstant(getTotalInstants() - 1) : Optional.empty();
    }

    @Override
    public Optional<String> nthFromLastInstant(int n) {
        if(getTotalInstants() < n + 1) {
            return Optional.empty();
        }
        return nthInstant(getTotalInstants() - 1 - n);
    }

    @Override
    public boolean containsInstant(String instant) {
        return instants.stream().anyMatch(s -> s.equals(instant));
    }

    @Override
    public boolean containsOrBeforeTimelineStarts(String instant) {
        return containsInstant(instant) || isInstantBeforeTimelineStarts(instant);
    }

    @Override
    public Stream<String> getInstants() {
        return instants.stream();
    }

    @Override
    public Stream<String> getInflightInstants() {
        return inflights.stream();
    }

    @Override
    public boolean isInstantBeforeTimelineStarts(String instant) {
        Optional<String> firstCommit = firstInstant();
        return firstCommit.isPresent() && compareInstants(instant, firstCommit.get(), LESSER);
    }

    @Override
    public void saveInstantAsInflight(String instant) {
        log.info("Creating a new in-flight " + getTimelineName() + " " + instant);
        // Create the in-flight file
        createFileInMetaPath(getInflightFileName(instant), Optional.empty());
    }

    @Override
    public void saveInstantAsComplete(String instant, Optional<byte[]> data) {
        log.info("Marking complete " + getTimelineName() + " " + instant);
        moveInflightToComplete(instant, data, getCompletedFileName(instant),
            HoodieTableMetaClient.makeInflightCommitFileName(instant));
        log.info("Completed " + getTimelineName() + " " + instant);
    }

    @Override
    public void revertInstantToInflight(String instant) {
        log.info("Reverting instant to inflight " + getTimelineName() + " " + instant);
        moveCompleteToInflight(instant, getCompletedFileName(instant),
            getInflightFileName(instant));
        log.info("Reverted " + getTimelineName() + " " + instant + " to inflight");
    }

    @Override
    public void removeInflightFromTimeline(String instant) {
        log.info("Removing in-flight " + getTimelineName() + " " + instant);
        String inFlightCommitFileName = getInflightFileName(instant);
        Path inFlightCommitFilePath = new Path(metaPath, inFlightCommitFileName);
        try {
            fs.delete(inFlightCommitFilePath, false);
            log.info("Removed in-flight " + getTimelineName() + " " + instant);
        } catch (IOException e) {
            throw new HoodieIOException(
                "Could not remove inflight commit " + inFlightCommitFilePath, e);
        }
    }

    @Override
    public Optional<byte[]> readInstantDetails(String instant) {
        Path detailPath = new Path(metaPath, getCompletedFileName(instant));
        return readDataFromPath(detailPath);
    }


    /**
     * Get the in-flight instant file name
     *
     * @param instant
     * @return
     */
    protected abstract String getInflightFileName(String instant);

    /**
     * Get the completed instant file name
     *
     * @param instant
     * @return
     */
    protected abstract String getCompletedFileName(String instant);

    /**
     * Get the timeline name
     *
     * @return
     */
    protected abstract String getTimelineName();


    protected void moveInflightToComplete(String instant, Optional<byte[]> data,
        String commitFileName, String inflightFileName) {
        Path commitFilePath = new Path(metaPath, commitFileName);
        try {
            // open a new file and write the commit metadata in
            Path inflightCommitFile = new Path(metaPath, inflightFileName);
            createFileInMetaPath(inflightFileName, data);
            boolean success = fs.rename(inflightCommitFile, commitFilePath);
            if (!success) {
                throw new HoodieIOException(
                    "Could not rename " + inflightCommitFile + " to " + commitFilePath);
            }
        } catch (IOException e) {
            throw new HoodieIOException("Could not complete commit " + instant, e);
        }
    }

    protected void moveCompleteToInflight(String instant, String commitFileName,
        String inflightFileName) {
        Path inFlightCommitFilePath = new Path(metaPath, inflightFileName);
        try {
            if (!fs.exists(inFlightCommitFilePath)) {
                Path commitFilePath = new Path(metaPath, commitFileName);
                boolean success = fs.rename(commitFilePath, inFlightCommitFilePath);
                if (!success) {
                    throw new HoodieIOException(
                        "Could not rename " + commitFilePath + " to " + inFlightCommitFilePath);
                }
            }
        } catch (IOException e) {
            throw new HoodieIOException("Could not complete commit revert " + instant, e);
        }
    }

    protected void createFileInMetaPath(String filename, Optional<byte[]> content) {
        Path fullPath = new Path(metaPath, filename);
        try {
            if (!content.isPresent()) {
                if (fs.createNewFile(fullPath)) {
                    log.info("Created a new file in meta path: " + fullPath);
                    return;
                }
            } else {
                FSDataOutputStream fsout = fs.create(fullPath, true);
                fsout.write(content.get());
                fsout.close();
                return;
            }
            throw new HoodieIOException("Failed to create file " + fullPath);
        } catch (IOException e) {
            throw new HoodieIOException("Failed to create file " + fullPath, e);
        }
    }

    protected Optional<byte[]> readDataFromPath(Path detailPath) {
        FSDataInputStream is = null;
        try {
            is = fs.open(detailPath);
            return Optional.of(IOUtils.toByteArray(is));
        } catch (IOException e) {
            throw new HoodieIOException("Could not read commit details from " + detailPath, e);
        } finally {
            if (is != null) {
                Closeables.closeQuietly(is);
            }
        }
    }


    @Override
    public String toString() {
        return this.getClass().getName() + ": " + instants.stream().map(Object::toString)
            .collect(Collectors.joining(","));
    }

}
