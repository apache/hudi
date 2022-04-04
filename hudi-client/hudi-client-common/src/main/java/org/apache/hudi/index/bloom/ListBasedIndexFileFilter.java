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

package org.apache.hudi.index.bloom;

import org.apache.hudi.common.util.collection.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple implementation of {@link IndexFileFilter}. Sequentially goes through every index file in a given partition to
 * search for potential index files to be searched for a given record key.
 */
public class ListBasedIndexFileFilter implements IndexFileFilter {

  final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo;

  /**
   * Instantiates {@link ListBasedIndexFileFilter}.
   *
   * @param partitionToFileIndexInfo Map of partition to List of {@link BloomIndexFileInfo}
   */
  public ListBasedIndexFileFilter(final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo) {
    this.partitionToFileIndexInfo = partitionToFileIndexInfo;
  }

  @Override
  public Set<Pair<String, String>> getMatchingFilesAndPartition(String partitionPath, String recordKey) {
    List<BloomIndexFileInfo> indexInfos = partitionToFileIndexInfo.get(partitionPath);
    Set<Pair<String, String>> toReturn = new HashSet<>();
    if (indexInfos != null) { // could be null, if there are no files in a given partition yet.
      // for each candidate file in partition, that needs to be compared.
      for (BloomIndexFileInfo indexInfo : indexInfos) {
        if (shouldCompareWithFile(indexInfo, recordKey)) {
          toReturn.add(Pair.of(partitionPath, indexInfo.getFileId()));
        }
      }
    }
    return toReturn;
  }

  /**
   * if we dont have key ranges, then also we need to compare against the file. no other choice if we do, then only
   * compare the file if the record key falls in range.
   */
  protected boolean shouldCompareWithFile(BloomIndexFileInfo indexInfo, String recordKey) {
    return !indexInfo.hasKeyRanges() || indexInfo.isKeyInRange(recordKey);
  }
}
