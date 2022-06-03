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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.InputSplitUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Realtime Input Split Interface.
 */
public interface RealtimeSplit extends InputSplitWithLocationInfo {

  /**
   * Return Log File Paths.
   * @return
   */
  default List<String> getDeltaLogPaths() {
    return getDeltaLogFiles().stream().map(entry -> entry.getPath().toString()).collect(Collectors.toList());
  }

  List<HoodieLogFile> getDeltaLogFiles();

  void setDeltaLogFiles(List<HoodieLogFile> deltaLogFiles);

  /**
   * Return Max Instant Time.
   * @return
   */
  String getMaxCommitTime();

  /**
   * Return Base Path of the dataset.
   * @return
   */
  String getBasePath();

  /**
   * Returns Virtual key info if meta fields are disabled.
   * @return
   */
  Option<HoodieVirtualKeyInfo> getVirtualKeyInfo();

  /**
   * Returns the flag whether this split belongs to an Incremental Query
   */
  boolean getBelongsToIncrementalQuery();

  /**
   * Update Maximum valid instant time.
   * @param maxCommitTime
   */
  void setMaxCommitTime(String maxCommitTime);

  /**
   * Set Base Path.
   * @param basePath
   */
  void setBasePath(String basePath);

  /**
   * Sets the flag whether this split belongs to an Incremental Query
   */
  void setBelongsToIncrementalQuery(boolean belongsToIncrementalQuery);

  void setVirtualKeyInfo(Option<HoodieVirtualKeyInfo> virtualKeyInfo);

  default void writeToOutput(DataOutput out) throws IOException {
    InputSplitUtils.writeString(getBasePath(), out);
    InputSplitUtils.writeString(getMaxCommitTime(), out);
    InputSplitUtils.writeBoolean(getBelongsToIncrementalQuery(), out);

    out.writeInt(getDeltaLogFiles().size());
    for (HoodieLogFile logFile : getDeltaLogFiles()) {
      InputSplitUtils.writeString(logFile.getPath().toString(), out);
      out.writeLong(logFile.getFileSize());
    }

    Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt = getVirtualKeyInfo();
    if (!virtualKeyInfoOpt.isPresent()) {
      InputSplitUtils.writeBoolean(false, out);
    } else {
      InputSplitUtils.writeBoolean(true, out);
      InputSplitUtils.writeString(virtualKeyInfoOpt.get().getRecordKeyField(), out);
      InputSplitUtils.writeBoolean(virtualKeyInfoOpt.get().getPartitionPathField().isPresent(), out);
      if (virtualKeyInfoOpt.get().getPartitionPathField().isPresent()) {
        InputSplitUtils.writeString(virtualKeyInfoOpt.get().getPartitionPathField().get(), out);
      }
      InputSplitUtils.writeString(String.valueOf(virtualKeyInfoOpt.get().getRecordKeyFieldIndex()), out);
      // if partition path field exists, partition path field index should also exists. So, don't need another boolean
      if (virtualKeyInfoOpt.get().getPartitionPathFieldIndex().isPresent()) {
        InputSplitUtils.writeString(String.valueOf(virtualKeyInfoOpt.get().getPartitionPathFieldIndex()), out);
      }
    }
  }

  default void readFromInput(DataInput in) throws IOException {
    setBasePath(InputSplitUtils.readString(in));
    setMaxCommitTime(InputSplitUtils.readString(in));
    setBelongsToIncrementalQuery(InputSplitUtils.readBoolean(in));

    int totalLogFiles = in.readInt();
    List<HoodieLogFile> deltaLogPaths = new ArrayList<>(totalLogFiles);
    for (int i = 0; i < totalLogFiles; i++) {
      String logFilePath = InputSplitUtils.readString(in);
      long logFileSize = in.readLong();
      deltaLogPaths.add(new HoodieLogFile(new Path(logFilePath), logFileSize));
    }
    setDeltaLogFiles(deltaLogPaths);

    boolean hoodieVirtualKeyPresent = InputSplitUtils.readBoolean(in);
    if (hoodieVirtualKeyPresent) {
      String recordKeyField = InputSplitUtils.readString(in);
      boolean isPartitionPathFieldPresent = InputSplitUtils.readBoolean(in);
      Option<String> partitionPathField = isPartitionPathFieldPresent ? Option.of(InputSplitUtils.readString(in)) : Option.empty();
      int recordFieldIndex = Integer.parseInt(InputSplitUtils.readString(in));
      Option<Integer> partitionPathIndex = isPartitionPathFieldPresent ? Option.of(Integer.parseInt(InputSplitUtils.readString(in))) : Option.empty();
      setVirtualKeyInfo(Option.of(new HoodieVirtualKeyInfo(recordKeyField, partitionPathField, recordFieldIndex, partitionPathIndex)));
    }
  }

  /**
   * The file containing this split's data.
   */
  Path getPath();

  /**
   * The position of the first byte in the file to process.
   */
  long getStart();

  /**
   * The number of bytes in the file to process.
   */
  long getLength();
}
