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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.Option;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hoodie SourceSplit implementation for source V2. It has the same semantic to the {@link org.apache.hudi.table.format.mor.MergeOnReadInputSplit}.
 */
@Getter
public class HoodieSourceSplit implements SourceSplit, Serializable {
  public static AtomicInteger SPLIT_COUNTER = new AtomicInteger(0);
  private static final long serialVersionUID = 1L;
  private static final long NUM_NO_CONSUMPTION = 0L;

  // the split number
  private final int splitNum;
  // the base file of a file slice
  private final Option<String> basePath;
  // change log files of a file slice
  private final Option<List<String>> logPaths;
  // the base table path
  private final String tablePath;
  // source merge type
  private final String mergeType;
  // file id of file splice
  @Setter
  protected String fileId;

  // for streaming reader to record the consumed offset,
  // which is the start of next round reading.
  private long consumed = NUM_NO_CONSUMPTION;

  // for failure recovering
  private int fileOffset;
  private long recordOffset;

  public HoodieSourceSplit(
      int splitNum,
      @Nullable String basePath,
      Option<List<String>> logPaths,
      String tablePath,
      String mergeType,
      String fileId) {
    this.splitNum = splitNum;
    this.basePath = Option.ofNullable(basePath);
    this.logPaths = logPaths;
    this.tablePath = tablePath;
    this.mergeType = mergeType;
    this.fileId = fileId;
    this.fileOffset = 0;
    this.recordOffset = 0L;
  }

  @Override
  public String splitId() {
    return toString();
  }

  public void consume() {
    this.consumed += 1L;
  }

  public boolean isConsumed() {
    return this.consumed != NUM_NO_CONSUMPTION;
  }

  public void updatePosition(int newFileOffset, long newRecordOffset) {
    fileOffset = newFileOffset;
    recordOffset = newRecordOffset;
  }

  @Override
  public String toString() {
    return "HoodieSourceSplit{"
        + "splitNum=" + splitNum
        + ", basePath=" + basePath
        + ", logPaths=" + logPaths
        + ", tablePath='" + tablePath + '\''
        + ", mergeType='" + mergeType + '\''
        + '}';
  }
}
