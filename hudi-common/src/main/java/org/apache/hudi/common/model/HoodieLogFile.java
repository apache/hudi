/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * Abstracts a single log file. Contains methods to extract metadata like the fileId, version and extension from the log
 * file path.
 * <p>
 * Also contains logic to roll-over the log file
 */
public class HoodieLogFile implements Serializable {

  private static final long serialVersionUID = 1L;
  public static final String DELTA_EXTENSION = ".log";
  public static final Integer LOGFILE_BASE_VERSION = 1;

  private static final Comparator<HoodieLogFile> LOG_FILE_COMPARATOR = new LogFileComparator();
  private static final Comparator<HoodieLogFile> LOG_FILE_COMPARATOR_REVERSED = new LogFileComparator().reversed();

  private transient FileStatus fileStatus;
  private final String pathStr;
  private long fileLen;

  public HoodieLogFile(HoodieLogFile logFile) {
    this.fileStatus = logFile.fileStatus;
    this.pathStr = logFile.pathStr;
    this.fileLen = logFile.fileLen;
  }

  public HoodieLogFile(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
    this.pathStr = fileStatus.getPath().toString();
    this.fileLen = fileStatus.getLen();
  }

  public HoodieLogFile(Path logPath) {
    this.fileStatus = null;
    this.pathStr = logPath.toString();
    this.fileLen = -1;
  }

  public HoodieLogFile(Path logPath, Long fileLen) {
    this.fileStatus = null;
    this.pathStr = logPath.toString();
    this.fileLen = fileLen;
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

  public String getLogWriteToken() {
    return FSUtils.getWriteTokenFromLogPath(getPath());
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

  public void setFileStatus(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
  }

  public HoodieLogFile rollOver(FileSystem fs, String logWriteToken) throws IOException {
    String fileId = getFileId();
    String baseCommitTime = getBaseCommitTime();
    Path path = getPath();
    String extension = "." + FSUtils.getFileExtensionFromLog(path);
    int newVersion = FSUtils.computeNextLogVersion(fs, path.getParent(), fileId, extension, baseCommitTime);
    return new HoodieLogFile(new Path(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, baseCommitTime, newVersion, logWriteToken)));
  }

  public static Comparator<HoodieLogFile> getLogFileComparator() {
    return LOG_FILE_COMPARATOR;
  }

  public static Comparator<HoodieLogFile> getReverseLogFileComparator() {
    return LOG_FILE_COMPARATOR_REVERSED;
  }

  /**
   * Comparator to order log-files.
   */
  public static class LogFileComparator implements Comparator<HoodieLogFile>, Serializable {

    private static final long serialVersionUID = 1L;
    private transient Comparator<String> writeTokenComparator;

    private Comparator<String> getWriteTokenComparator() {
      if (null == writeTokenComparator) {
        // writeTokenComparator is not serializable. Hence, lazy loading
        writeTokenComparator = Comparator.nullsFirst(Comparator.naturalOrder());
      }
      return writeTokenComparator;
    }

    @Override
    public int compare(HoodieLogFile o1, HoodieLogFile o2) {
      String baseInstantTime1 = o1.getBaseCommitTime();
      String baseInstantTime2 = o2.getBaseCommitTime();

      if (baseInstantTime1.equals(baseInstantTime2)) {

        if (o1.getLogVersion() == o2.getLogVersion()) {
          // Compare by write token when base-commit and log-version is same
          return getWriteTokenComparator().compare(o1.getLogWriteToken(), o2.getLogWriteToken());
        }

        // compare by log-version when base-commit is same
        return Integer.compare(o1.getLogVersion(), o2.getLogVersion());
      }

      // compare by base-commits
      return baseInstantTime1.compareTo(baseInstantTime2);
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
    return "HoodieLogFile{pathStr='" + pathStr + '\'' + ", fileLen=" + fileLen + '}';
  }
}
