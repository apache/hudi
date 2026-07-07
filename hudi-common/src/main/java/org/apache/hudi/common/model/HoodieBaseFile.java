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

import org.apache.hudi.common.fs.FileNameParser;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.InvalidHoodiePathException;
import org.apache.hudi.storage.StoragePathInfo;

import lombok.Getter;
import lombok.ToString;

/**
 * Hoodie base file - Represents metadata about Hudi file in DFS.
 * Supports APIs to get Hudi FileId, Commit Time and bootstrap file (if any).
 */
@Getter
@ToString
public class HoodieBaseFile extends BaseFile {

  private static final long serialVersionUID = 1L;
  private final String fileId;
  private final String commitTime;

  private Option<BaseFile> bootstrapBaseFile;

  public HoodieBaseFile(HoodieBaseFile dataFile) {
    super(dataFile);
    this.bootstrapBaseFile = dataFile.bootstrapBaseFile;
    this.fileId = dataFile.getFileId();
    this.commitTime = dataFile.getCommitTime();
  }

  public HoodieBaseFile(StoragePathInfo pathInfo) {
    this(pathInfo, null);
  }

  public HoodieBaseFile(StoragePathInfo pathInfo, BaseFile bootstrapBaseFile) {
    this(pathInfo, getFileIdAndCommitTimeFromFileName(pathInfo.getPath().getName()),
        bootstrapBaseFile);
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

  private HoodieBaseFile(StoragePathInfo pathInfo, String[] fileIdAndCommitTime,
                         BaseFile bootstrapBaseFile) {
    this(pathInfo, fileIdAndCommitTime[0], fileIdAndCommitTime[1], bootstrapBaseFile);
  }

  public HoodieBaseFile(StoragePathInfo pathInfo, String fileId, String commitTime,
                        BaseFile bootstrapBaseFile) {
    super(ExternalFilePathUtil.maybeHandleExternallyGeneratedFileName(pathInfo, fileId));
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
    Option<FileNameParser.BaseFileName> parsedBaseFileName = FileNameParser.parseBaseFile(fileName);
    if (!parsedBaseFileName.isPresent()) {
      throw new InvalidHoodiePathException(fileName, "BaseFile");
    }

    return new String[] {
        parsedBaseFileName.get().getFileId(),
        parsedBaseFileName.get().getCommitTime()
    };
  }

  public void setBootstrapBaseFile(BaseFile bootstrapBaseFile) {
    this.bootstrapBaseFile = Option.ofNullable(bootstrapBaseFile);
  }
}
