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
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Abstracts a single log file. Contains methods to extract metadata like the fileId, version and extension from the log
 * file path.
 * <p>
 * Also contains logic to roll-over the log file
 */
public class HoodieLogFile implements Serializable {

  public static final String DELTA_EXTENSION = ".log";
  public static final Integer LOGFILE_BASE_VERSION = 1;

  private transient FileStatus fileStatus;
  private final String pathStr;
  private long fileLen;

  public HoodieLogFile(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
    this.pathStr = fileStatus.getPath().toString();
    this.fileLen = fileStatus.getLen();
  }

  public HoodieLogFile(Path logPath) {
    this.fileStatus = null;
    this.pathStr = logPath.toString();
    this.fileLen = 0;
  }

  public HoodieLogFile(String logPathStr) {
    this.fileStatus = null;
    this.pathStr = logPathStr;
    this.fileLen = -1;
  }

  public String getFileId() {
    return FSUtils.getFileIdFromLogPath(getPath());
  }

  public String getBaseCommitTime() {
    return FSUtils.getBaseCommitTimeFromLogPath(getPath());
  }

  public int getLogVersion() {
    return FSUtils.getFileVersionFromLog(getPath());
  }

  public String getFileExtension() {
    return FSUtils.getFileExtensionFromLog(getPath());
  }

  public Path getPath() {
    return new Path(pathStr);
  }

  public String getFileName() {
    return getPath().getName();
  }

  public void setFileLen(long fileLen) {
    this.fileLen = fileLen;
  }

  public long getFileSize() {
    return fileLen;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public HoodieLogFile rollOver(FileSystem fs) throws IOException {
    String fileId = getFileId();
    String baseCommitTime = getBaseCommitTime();
    Path path = getPath();
    String extension = "." + FSUtils.getFileExtensionFromLog(path);
    int newVersion = FSUtils
        .computeNextLogVersion(fs, path.getParent(), fileId,
            extension, baseCommitTime);
    return new HoodieLogFile(new Path(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, baseCommitTime, newVersion)));
  }

  public static Comparator<HoodieLogFile> getBaseInstantAndLogVersionComparator() {
    return new BaseInstantAndLogVersionComparator();
  }

  /**
   * Comparator to order log-files
   */
  private static class BaseInstantAndLogVersionComparator implements Comparator<HoodieLogFile>, Serializable {

    @Override
    public int compare(HoodieLogFile o1, HoodieLogFile o2) {
      String baseInstantTime1 = o1.getBaseCommitTime();
      String baseInstantTime2 = o2.getBaseCommitTime();
      if (baseInstantTime1.equals(baseInstantTime2)) {
        // reverse the order by log-version when base-commit is same
        return new Integer(o2.getLogVersion()).compareTo(o1.getLogVersion());
      }
      // reverse the order by base-commits
      return baseInstantTime2.compareTo(baseInstantTime1);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieLogFile that = (HoodieLogFile) o;
    return Objects.equals(pathStr, that.pathStr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathStr);
  }

  @Override
  public String toString() {
    return "HoodieLogFile{"
        + "pathStr='" + pathStr + '\''
        + ", fileLen=" + fileLen
        + '}';
  }
}
