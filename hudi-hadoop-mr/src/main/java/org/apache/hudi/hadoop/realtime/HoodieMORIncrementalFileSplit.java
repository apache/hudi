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

package org.apache.hudi.hadoop.realtime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;

/**
 * Filesplit that wraps a HoodieFileGroup involved in Incremental queries.
 */
public class HoodieMORIncrementalFileSplit extends FileSplit {

  private String basePath;
  private HoodieFileGroupId fileGroupId;
  private List<FileSlice> fileGroupSlices;
  String maxCommitTime;

  public HoodieMORIncrementalFileSplit(HoodieFileGroup hoodieFileGroup, String basePath, String maxCommitTime) {
    this.fileGroupId = hoodieFileGroup.getFileGroupId();
    this.fileGroupSlices = hoodieFileGroup.getAllFileSlices().collect(Collectors.toList());
    this.basePath = basePath;
    this.maxCommitTime = maxCommitTime;
  }

  public HoodieFileGroupId getFileGroupId() {
    return fileGroupId;
  }

  public List<FileSlice> getFileSlices() {
    return fileGroupSlices;
  }

  public String getBasePath() {
    return basePath;
  }

  public String getMaxCommitTime() {
    return maxCommitTime;
  }

  // returns the most recent Parquet file path from the group of file slices
  public String getLatestBaseFilePath() {
    Option<FileSlice> fileSlice = Option.fromJavaOptional(fileGroupSlices.stream().filter(slice -> slice.getBaseFile().isPresent()).findFirst());
    return fileSlice.map(slice -> slice.getBaseFile().get().getPath()).orElse(null);
  }

  // returns the most recent log file paths from the group of file slices
  public List<String> getLatestLogFilePaths() {
    Option<FileSlice> fileSlice = Option.fromJavaOptional(fileGroupSlices.stream().filter(slice -> slice.getLogFiles().findAny().isPresent()).findFirst());
    return fileSlice.map(slice -> slice.getLogFiles().map(logFile -> logFile.getPath().toString()).collect(Collectors.toList())).orElse(new ArrayList<>());
  }

  private static void writeString(String str, DataOutput out) throws IOException {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static void writeFileGroupId(HoodieFileGroupId fileGroupId, DataOutput out) throws IOException {
    writeString(fileGroupId.getPartitionPath(), out);
    writeString(fileGroupId.getFileId(), out);
  }

  private static void writeFileStatus(FileStatus fileStatus, DataOutput out) throws IOException {
    out.writeLong(fileStatus.getLen());
    writeString(fileStatus.getPath().toString(), out);
  }

  private static void writeFileSlice(FileSlice slice, DataOutput out) throws IOException {
    boolean isBaseFilePresent = slice.getBaseFile().isPresent();
    out.writeBoolean(isBaseFilePresent);
    if (isBaseFilePresent) {
      writeFileStatus(slice.getBaseFile().get().getFileStatus(), out);
    }
    List<String> logFilePaths = slice.getLogFiles()
        .map(f -> f.getPath().toString())
        .collect(Collectors.toList());
    out.writeInt(logFilePaths.size());
    for (String logFilePath : logFilePaths) {
      writeString(logFilePath, out);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeString(basePath, out);
    writeString(maxCommitTime, out);
    writeFileGroupId(fileGroupId, out);
    out.writeInt(fileGroupSlices.size());
    for (FileSlice fileSlice: fileGroupSlices) {
      writeFileSlice(fileSlice, out);
    }
  }

  private static String readString(DataInput in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static HoodieFileGroupId readFileGroupId(DataInput in) throws IOException {
    return new HoodieFileGroupId(readString(in), readString(in));
  }

  private static FileStatus readFileStatus(DataInput in) throws IOException {
    return new FileStatus(in.readLong(), false, 0, 0, 0, new Path(readString(in)));
  }

  private static FileSlice readFileSlice(HoodieFileGroupId fileGroupId, DataInput in) throws IOException {
    boolean isBaseFilePresent =  in.readBoolean();
    Option<HoodieBaseFile> baseFile = isBaseFilePresent ? Option.of(new HoodieBaseFile(readFileStatus(in))) : Option.empty();
    int numLogFiles = in.readInt();
    List<HoodieLogFile> logFiles = new ArrayList<>();
    Option<String> baseInstantTime = Option.empty();
    for (int i = 0; i < numLogFiles; i++) {
      HoodieLogFile logFile = new HoodieLogFile(readString(in));
      logFiles.add(logFile);
      if (!baseInstantTime.isPresent()) {
        baseInstantTime = Option.of(logFile.getBaseCommitTime());
      }
    }
    FileSlice fileSlice = new FileSlice(fileGroupId, baseInstantTime.get());
    baseFile.ifPresent(fileSlice::setBaseFile);
    for (HoodieLogFile logFile: logFiles) {
      fileSlice.addLogFile(logFile);
    }
    return fileSlice;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    basePath = readString(in);
    maxCommitTime = readString(in);
    fileGroupId = readFileGroupId(in);
    int totalSlices = in.readInt();
    fileGroupSlices = new ArrayList<>();
    for (int i = 0; i < totalSlices; i++) {
      fileGroupSlices.add(readFileSlice(fileGroupId, in));
    }
  }

  @Override
  public String toString() {
    return "HoodieMORIncrementalFileSplit{BasePath=" + basePath + ", FileGroupId=" + fileGroupId
        + ", maxcommitTime='" + maxCommitTime
        + ", numFileSlices='" + fileGroupSlices.size() + '}';
  }
}
