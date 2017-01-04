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

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Active commit timeline. Much optimized for reading than the ArchivedTimeline.
 */
public class HoodieActiveCommitTimeline extends HoodieDefaultTimeline {
    public HoodieActiveCommitTimeline(FileSystem fs, String metaPath) {
        super(fs, metaPath);
        String completedInstantExtension = HoodieTableMetaClient.COMMIT_EXTENSION;
        String inflightInstantExtension = INFLIGHT_EXTENSION;

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
    }

    @Override
    public String getInflightFileName(String instant) {
        return HoodieTableMetaClient.makeInflightCommitFileName(instant);
    }

    @Override
    public String getCompletedFileName(String instant) {
        return HoodieTableMetaClient.makeCommitFileName(instant);
    }

    @Override
    protected String getTimelineName() {
        return "commit";
    }

    @Override
    public HoodieTimeline reload() throws IOException {
        return new HoodieActiveCommitTimeline(fs, metaPath);
    }
}
