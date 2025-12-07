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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.exception.InvalidHoodiePathException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;

import static org.apache.hudi.common.fs.FSUtils.LOG_FILE_PATTERN;

/**
 * Abstracts a single log file. Contains methods to extract metadata like
 * the fileId, version and extension from the log file path.
 *
 * <p>Also contains logic to roll over the log file.
 */
public class HoodieLogFile implements Serializable {

  private static final long serialVersionUID = 1L;
  public static final String DELTA_EXTENSION = ".log";
  public static final String LOG_FILE_PREFIX = ".";
  public static final Integer LOGFILE_BASE_VERSION = 1;

  private static final Comparator<HoodieLogFile> LOG_FILE_COMPARATOR = new LogFileComparator();
  private static final Comparator<HoodieLogFile> LOG_FILE_COMPARATOR_REVERSED = new LogFileComparator().reversed();

  @Setter
  @Getter
  private transient StoragePathInfo pathInfo;
  private transient StoragePath path;
  private final String pathStr;
  private String fileId;
  private String deltaCommitTime;
  private int logVersion;
  private String logWriteToken;
  private String fileExtension;
  private String suffix;
  @Setter
  private long fileLen;

  public HoodieLogFile(HoodieLogFile logFile) {
    this.pathInfo = logFile.getPathInfo();
    this.path = logFile.getPath();
    this.pathStr = logFile.pathStr;
    this.fileId = logFile.getFileId();
    this.deltaCommitTime = logFile.getDeltaCommitTime();
    this.logVersion = logFile.getLogVersion();
    this.logWriteToken = logFile.getLogWriteToken();
    this.fileExtension = logFile.getFileExtension();
    this.suffix = logFile.getSuffix();
    this.fileLen = logFile.getFileSize();
  }

  public HoodieLogFile(StoragePathInfo pathInfo) {
    this(pathInfo, pathInfo.getPath(), pathInfo.getPath().toString(), pathInfo.getLength());
  }

  public HoodieLogFile(StoragePath logPath) {
    this(null, logPath, logPath.toString(), -1);
  }

  public HoodieLogFile(StoragePath logPath, long fileLen) {
    this(null, logPath, logPath.toString(), fileLen);
  }

  public HoodieLogFile(String logPathStr) {
    this(null, null, logPathStr, -1);
  }

  private HoodieLogFile(StoragePathInfo pathInfo, StoragePath logPath, String logPathStr, long fileLen) {
    this.pathInfo = pathInfo;
    this.pathStr = logPathStr;
    this.fileLen = fileLen;
    this.logVersion = -1; // mark version as uninitialized
    this.path = logPath;
  }

  private void parseFieldsFromPath() {
    Matcher matcher = LOG_FILE_PATTERN.matcher(getPath().getName());
    if (!matcher.matches()) {
      throw new InvalidHoodiePathException(path, "LogFile");
    }
    this.fileId = matcher.group(1);
    this.deltaCommitTime = matcher.group(2);
    this.fileExtension = matcher.group(3);
    this.logVersion = Integer.parseInt(matcher.group(4));
    this.logWriteToken = matcher.group(6);
    this.suffix = matcher.group(10) == null ? "" : matcher.group(10);
  }

  public String getFileId() {
    if (fileId == null) {
      parseFieldsFromPath();
    }
    return fileId;
  }

  public String getDeltaCommitTime() {
    if (deltaCommitTime == null) {
      parseFieldsFromPath();
    }
    return deltaCommitTime;
  }

  public int getLogVersion() {
    if (logVersion == -1) {
      parseFieldsFromPath();
    }
    return logVersion;
  }

  public String getLogWriteToken() {
    if (logWriteToken == null) {
      parseFieldsFromPath();
    }
    return logWriteToken;
  }

  public String getFileExtension() {
    if (fileExtension == null) {
      parseFieldsFromPath();
    }
    return fileExtension;
  }

  public boolean isCDC() {
    return getSuffix().equals(HoodieCDCUtils.CDC_LOGFILE_SUFFIX);
  }

  public String getSuffix() {
    if (suffix == null) {
      parseFieldsFromPath();
    }
    return suffix;
  }

  public StoragePath getPath() {
    if (path == null) {
      path = new StoragePath(pathStr);
    }
    return path;
  }

  public String getFileName() {
    return getPath().getName();
  }

  public long getFileSize() {
    return fileLen;
  }

  public HoodieLogFile rollOver(String logWriteToken) {
    String fileId = getFileId();
    String deltaCommitTime = getDeltaCommitTime();
    StoragePath path = getPath();
    String extension = "." + fileExtension;
    return new HoodieLogFile(new StoragePath(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, deltaCommitTime, logVersion + 1, logWriteToken)));
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
      String deltaCommitTime1 = o1.getDeltaCommitTime();
      String deltaCommitTime2 = o2.getDeltaCommitTime();

      if (deltaCommitTime1.equals(deltaCommitTime2)) {

        if (o1.getLogVersion() == o2.getLogVersion()) {

          int compareWriteToken = getWriteTokenComparator().compare(o1.getLogWriteToken(), o2.getLogWriteToken());
          if (compareWriteToken == 0) {

            // Compare by suffix when write token is same
            return o1.getSuffix().compareTo(o2.getSuffix());
          }

          // Compare by write token when delta-commit and log-version is same
          return compareWriteToken;
        }

        // compare by log-version when delta-commit is same
        return Integer.compare(o1.getLogVersion(), o2.getLogVersion());
      }

      // compare by delta-commits
      return deltaCommitTime1.compareTo(deltaCommitTime2);
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
