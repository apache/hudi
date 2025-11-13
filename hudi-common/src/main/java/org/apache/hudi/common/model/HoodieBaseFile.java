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

import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.CachingPath;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hudi.hadoop.CachingPath.createRelativePathUnsafe;

/**
 * Hoodie base file - Represents metadata about Hudi file in DFS.
 * Supports APIs to get Hudi FileId, Commit Time and bootstrap file (if any).
 */
public class HoodieBaseFile extends BaseFile {
  private static final long serialVersionUID = 1L;
  private static final char UNDERSCORE = '_';
  private static final char DOT = '.';
  private final String fileId;
  private final String commitTime;

  private Option<BaseFile> bootstrapBaseFile;

  public HoodieBaseFile(HoodieBaseFile dataFile) {
    super(dataFile);
    this.bootstrapBaseFile = dataFile.bootstrapBaseFile;
    this.fileId = dataFile.getFileId();
    this.commitTime = dataFile.getCommitTime();
  }

  public HoodieBaseFile(FileStatus fileStatus) {
    this(fileStatus, null);
  }

  public HoodieBaseFile(FileStatus fileStatus, BaseFile bootstrapBaseFile) {
    this(fileStatus, getFileIdAndCommitTimeFromFileName(fileStatus.getPath().getName()), bootstrapBaseFile);
  }

  public HoodieBaseFile(String filePath) {
    this(filePath, null);
  }

  public HoodieBaseFile(String filePath, BaseFile bootstrapBaseFile) {
    super(filePath);
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    String[] fileIdAndCommitTime = getFileIdAndCommitTimeFromFileName(getFileName());
    this.fileId = fileIdAndCommitTime[0];
    this.commitTime = fileIdAndCommitTime[1];
  }

  public HoodieBaseFile(String filePath, String fileId, String commitTime, BaseFile bootstrapBaseFile) {
    super(filePath);
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    this.fileId = fileId;
    this.commitTime = commitTime;
  }

  private HoodieBaseFile(FileStatus fileStatus, String[] fileIdAndCommitTime, BaseFile bootstrapBaseFile) {
    this(fileStatus, fileIdAndCommitTime[0], fileIdAndCommitTime[1], fileIdAndCommitTime[2], bootstrapBaseFile);
  }

  public HoodieBaseFile(FileStatus fileStatus, String fileId, String commitTime, BaseFile bootstrapBaseFile) {
    this(fileStatus, fileId, commitTime, fileId, bootstrapBaseFile);
  }

  public HoodieBaseFile(FileStatus fileStatus, String fileId, String commitTime, String originalFileName, BaseFile bootstrapBaseFile) {
    super(maybeHandleExternallyGeneratedFileName(fileStatus, originalFileName));
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
    this.fileId = fileId;
    this.commitTime = commitTime;
  }

  /**
   * Parses the file ID and commit time from the fileName.
   * @param fileName Name of the file
   * @return String array of size 2 with fileId as the first and commitTime as the second element.
   */
  private static String[] getFileIdAndCommitTimeFromFileName(String fileName) {
    return ExternalFilePathUtil.isExternallyCreatedFile(fileName) ? handleExternallyGeneratedFile(fileName) : handleHudiGeneratedFile(fileName);
  }

  private static String[] handleHudiGeneratedFile(String fileName) {
    String[] values = new String[3];
    short underscoreCount = 0;
    short lastUnderscoreIndex = 0;
    for (int i = 0; i < fileName.length(); i++) {
      char c = fileName.charAt(i);
      if (c == UNDERSCORE) {
        if (underscoreCount == 0) {
          values[0] = fileName.substring(0, i);
        }
        lastUnderscoreIndex = (short) i;
        underscoreCount++;
      } else if (c == DOT) {
        if (underscoreCount == 2) {
          values[1] = fileName.substring(lastUnderscoreIndex + 1, i);
          return values;
        }
      }
    }
    // case where there is no '.' in file name (no file suffix like .parquet)
    values[1] = fileName.substring(lastUnderscoreIndex + 1);
    values[2] = fileName;
    return values;
  }

  private static String[] handleExternallyGeneratedFile(String fileName) {
    String[] values = new String[3];
    // file name has format <originalFileName>_<commitTime>_hudiext and originalFileName is used as fileId
    int lastUnderscore = fileName.lastIndexOf(UNDERSCORE);
    int secondToLastUnderscore = fileName.lastIndexOf(UNDERSCORE, lastUnderscore - 1);
    int firstUnderscore = fileName.indexOf(UNDERSCORE);
    values[0] = fileName.substring(0, firstUnderscore);
    values[1] = fileName.substring(secondToLastUnderscore + 1, lastUnderscore);
    values[2] = fileName.substring(0, secondToLastUnderscore);
    return values;
  }

  /**
   * If the file was created externally, the original file path will have a '_[commitTime]_hudiext' suffix when stored in the metadata table. That suffix needs to be removed from the FileStatus so
   * that the actual file can be found and read.
   * @param fileStatus an input file status that may require updating
   * @param fileId the fileId for the file
   * @return the original file status if it was not externally created, or a new FileStatus with the original file name if it was externally created
   */
  private static FileStatus maybeHandleExternallyGeneratedFileName(FileStatus fileStatus, String fileId) {
    if (fileStatus == null) {
      return null;
    }
    if (ExternalFilePathUtil.isExternallyCreatedFile(fileStatus.getPath().getName())) {
      // fileId is the same as the original file name for externally created files
      Path parent = fileStatus.getPath().getParent();
      return new FileStatus(fileStatus.getLen(), fileStatus.isDirectory(), fileStatus.getReplication(),
          fileStatus.getBlockSize(), fileStatus.getModificationTime(), fileStatus.getAccessTime(),
          fileStatus.getPermission(), fileStatus.getOwner(), fileStatus.getGroup(),
          new CachingPath(parent, createRelativePathUnsafe(fileId)));
    } else {
      return fileStatus;
    }
  }

  public String getFileId() {
    return fileId;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public Option<BaseFile> getBootstrapBaseFile() {
    return bootstrapBaseFile;
  }

  public void setBootstrapBaseFile(BaseFile bootstrapBaseFile) {
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
  }

  @Override
  public String toString() {
    return "HoodieBaseFile{fullPath=" + getPath() + ", fileLen=" + getFileLen()
        + ", BootstrapBaseFile=" + bootstrapBaseFile.orElse(null) + '}';
  }
}
