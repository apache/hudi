/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.format.mor;

import org.apache.hudi.common.util.Option;

import org.apache.flink.core.io.InputSplit;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Represents an input split of source, actually a data bucket.
 */
public class MergeOnReadInputSplit implements InputSplit {
  private static final long serialVersionUID = 1L;

  private final int splitNum;
  private final Option<String> basePath;
  private final Option<List<String>> logPaths;
  private final String latestCommit;
  private final String tablePath;
  private final long maxCompactionMemoryInBytes;
  private final String mergeType;

  public MergeOnReadInputSplit(
      int splitNum,
      @Nullable String basePath,
      Option<List<String>> logPaths,
      String latestCommit,
      String tablePath,
      long maxCompactionMemoryInBytes,
      String mergeType) {
    this.splitNum = splitNum;
    this.basePath = Option.ofNullable(basePath);
    this.logPaths = logPaths;
    this.latestCommit = latestCommit;
    this.tablePath = tablePath;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.mergeType = mergeType;
  }

  public Option<String> getBasePath() {
    return basePath;
  }

  public Option<List<String>> getLogPaths() {
    return logPaths;
  }

  public String getLatestCommit() {
    return latestCommit;
  }

  public String getTablePath() {
    return tablePath;
  }

  public long getMaxCompactionMemoryInBytes() {
    return maxCompactionMemoryInBytes;
  }

  public String getMergeType() {
    return mergeType;
  }

  @Override
  public int getSplitNumber() {
    return this.splitNum;
  }
}
