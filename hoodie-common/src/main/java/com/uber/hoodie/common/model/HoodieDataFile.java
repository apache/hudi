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

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.Serializable;
import java.util.Comparator;

public class HoodieDataFile implements Serializable {
    private FileStatus fileStatus;

    public HoodieDataFile(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public String getFileId() {
        return FSUtils.getFileId(fileStatus.getPath().getName());
    }

    public String getCommitTime() {
        return FSUtils.getCommitTime(fileStatus.getPath().getName());
    }

    public String getPath() {
        return fileStatus.getPath().toString();
    }

    public String getFileName() {
        return fileStatus.getPath().getName();
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }

    public static Comparator<HoodieDataFile> getCommitTimeComparator() {
        return (o1, o2) -> {
            // reverse the order
            return o2.getCommitTime().compareTo(o1.getCommitTime());
        };
    }

    public long getFileSize() {
        return fileStatus.getLen();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HoodieDataFile {");
        sb.append("fileStatus=").append(fileStatus);
        sb.append('}');
        return sb.toString();
    }
}
