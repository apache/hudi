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

package org.apache.hudi.source.rebalance.selector;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.List;

public class StreamReadBucketIndexKeySelector implements KeySelector<MergeOnReadInputSplit, Pair<String, String>> {

  private final StoragePath basePath;

  public StreamReadBucketIndexKeySelector(String tablePath) {
    this.basePath = new StoragePath(tablePath);
  }

  @Override
  public Pair<String, String> getKey(MergeOnReadInputSplit mergeOnReadInputSplit) throws Exception {
    Option<String> validFilePath = getValidFilePathFromInputSplit(mergeOnReadInputSplit);
    String relPartitionPath = "";
    if (validFilePath.isPresent()) {
      StoragePath fullPartitionPath = new StoragePath(validFilePath.get()).getParent();
      relPartitionPath = FSUtils.getRelativePartitionPath(fullPartitionPath, basePath);
    }

    return Pair.of(relPartitionPath, mergeOnReadInputSplit.getFileId());
  }

  /**
   * Get a valid file path from MergeOnReadInputSplit
   *
   */
  private Option<String> getValidFilePathFromInputSplit(MergeOnReadInputSplit mergeOnReadInputSplit) {
    if (mergeOnReadInputSplit.getBasePath().isPresent()) {
      return mergeOnReadInputSplit.getBasePath();
    }

    Option<List<String>> logPaths = mergeOnReadInputSplit.getLogPaths();
    if (logPaths.isPresent() && logPaths.get().size() > 0) {
      return Option.of(logPaths.get().get(0));
    }

    return Option.empty();
  }
}
