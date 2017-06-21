/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.common.model;

import com.uber.hoodie.common.util.FSUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;

/**
 * Abstracts a single log file. Contains methods to extract metadata like the fileId, version and
 * extension from the log file path.
 *
 * Also contains logic to roll-over the log file
 */
public class HoodieLogFile {
    public static final String DELTA_EXTENSION = ".log";

    private final Path path;
    private Optional<FileStatus> fileStatus;

    public HoodieLogFile(FileStatus fileStatus) {
        this(fileStatus.getPath());
        this.fileStatus = Optional.of(fileStatus);
    }

    public HoodieLogFile(Path logPath) {
        this.path = logPath;
        this.fileStatus = Optional.empty();
    }

    public String getFileId() {
        return FSUtils.getFileIdFromLogPath(path);
    }

    public String getBaseCommitTime() {
        return FSUtils.getBaseCommitTimeFromLogPath(path);
    }

    public int getLogVersion() {
        return FSUtils.getFileVersionFromLog(path);
    }

    public String getFileExtension() {
        return FSUtils.getFileExtensionFromLog(path);
    }

    public Path getPath() {
        return path;
    }

    public String getFileName() {
        return path.getName();
    }

    public Optional<FileStatus> getFileStatus() {
        return fileStatus;
    }

    public Optional<Long> getFileSize() {
        return fileStatus.map(FileStatus::getLen);
    }

    public HoodieLogFile rollOver(FileSystem fs) throws IOException {
        String fileId = getFileId();
        String baseCommitTime = getBaseCommitTime();
        String extension = "." + FSUtils.getFileExtensionFromLog(path);
        int newVersion = FSUtils
            .computeNextLogVersion(fs, path.getParent(), fileId,
                    extension, baseCommitTime);
        return new HoodieLogFile(new Path(path.getParent(),
            FSUtils.makeLogFileName(fileId, extension, baseCommitTime, newVersion)));
    }

    public static Comparator<HoodieLogFile> getLogVersionComparator() {
        return (o1, o2) -> {
            // reverse the order
            return new Integer(o2.getLogVersion()).compareTo(o1.getLogVersion());
        };
    }

    @Override
    public String toString() {
        return "HoodieLogFile {" + path + '}';
    }
}
