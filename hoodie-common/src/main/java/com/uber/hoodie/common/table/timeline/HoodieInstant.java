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

import com.google.common.io.Files;
import com.uber.hoodie.common.table.HoodieTimeline;
import org.apache.hadoop.fs.FileStatus;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * A Hoodie Instant represents a action done on a hoodie dataset.
 * All actions start with a inflight instant and then create a completed instant after done.
 *
 * @see HoodieTimeline
 */
public class HoodieInstant implements Serializable {
    private boolean isInflight = false;
    private String action;
    private String timestamp;

    /**
     * Load the instant from the meta FileStatus
     * @param fileStatus
     */
    public HoodieInstant(FileStatus fileStatus) {
        // First read the instant timestamp. [==>20170101193025<==].commit
        String fileName = fileStatus.getPath().getName();
        String fileExtension = Files.getFileExtension(fileName);
        timestamp = fileName.replace("." + fileExtension, "");

        // Next read the action for this marker
        action = fileExtension;
        if(action.equals("inflight")) {
            // This is to support backwards compatibility on how in-flight commit files were written
            // General rule is inflight extension is .<action>.inflight, but for commit it is .inflight
            action = "commit";
            isInflight = true;
        } else if (action.contains(HoodieTimeline.INFLIGHT_EXTENSION)) {
            isInflight = true;
            action = action.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
        }
    }

    public HoodieInstant(boolean isInflight, String action, String timestamp) {
        this.isInflight = isInflight;
        this.action = action;
        this.timestamp = timestamp;
    }

    public boolean isInflight() {
        return isInflight;
    }

    public String getAction() {
        return action;
    }

    public String getTimestamp() {
        return timestamp;
    }

    /**
     * Get the filename for this instant
     * @return
     */
    public String getFileName() {
        if (HoodieTimeline.COMMIT_ACTION.equals(action)) {
            return isInflight ?
                HoodieTimeline.makeInflightCommitFileName(timestamp) :
                HoodieTimeline.makeCommitFileName(timestamp);
        } else if (HoodieTimeline.CLEAN_ACTION.equals(action)) {
            return isInflight ?
                HoodieTimeline.makeInflightCleanerFileName(timestamp) :
                HoodieTimeline.makeCleanerFileName(timestamp);
        } else if (HoodieTimeline.SAVEPOINT_ACTION.equals(action)) {
            return isInflight ?
                HoodieTimeline.makeInflightSavePointFileName(timestamp) :
                HoodieTimeline.makeSavePointFileName(timestamp);
        }
        throw new IllegalArgumentException("Cannot get file name for unknown action " + action);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        HoodieInstant that = (HoodieInstant) o;
        return isInflight == that.isInflight &&
            Objects.equals(action, that.action) &&
            Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isInflight, action, timestamp);
    }

    @Override
    public String toString() {
        return  "[" + ((isInflight) ? "==>" : "") + timestamp + "__" + action + "]";
    }
}
