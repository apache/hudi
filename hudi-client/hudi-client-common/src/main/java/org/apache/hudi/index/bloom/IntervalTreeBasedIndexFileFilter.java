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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interval Tree based index look up. Builds an {@link KeyRangeLookupTree} for every partition and uses it to search for
 * matching index files for any given recordKey that needs to be looked up.
 */
class IntervalTreeBasedIndexFileFilter implements IndexFileFilter {

  private final Map<String, KeyRangeLookupTree> partitionToFileIndexLookUpTree = new HashMap<>();
  private final Map<String, Set<String>> partitionToFilesWithNoRanges = new HashMap<>();
  private final Map<String, String> fileIdToFilenameMap = new HashMap<>();

  /**
   * Instantiates {@link IntervalTreeBasedIndexFileFilter}.
   *
   * @param partitionToFileIndexInfo Map of partition to List of {@link BloomIndexFileInfo}s
   */
  IntervalTreeBasedIndexFileFilter(final Map<String, Map<String, BloomIndexFileInfo>> partitionToFileIndexInfo) {
    partitionToFileIndexInfo.forEach((partition, bloomIndexFileInfoMap) -> {
      // Note that the interval tree implementation doesn't have auto-balancing to ensure logN search time.
      // So, we are shuffling the input here hoping the tree will not have any skewness. If not, the tree could be
      // skewed which could result in N search time instead of logN.
      List<BloomIndexFileInfo> bloomIndexFiles = new ArrayList<>(bloomIndexFileInfoMap.values());
      Collections.shuffle(bloomIndexFiles);
      KeyRangeLookupTree lookUpTree = new KeyRangeLookupTree();
      bloomIndexFiles.forEach(indexFileInfo -> {
        fileIdToFilenameMap.put(indexFileInfo.getFileId(), indexFileInfo.getFilename());
        if (indexFileInfo.hasKeyRanges()) {
          lookUpTree.insert(new KeyRangeNode(indexFileInfo.getMinRecordKey(), indexFileInfo.getMaxRecordKey(),
              indexFileInfo.getFileId()));
        } else {
          if (!partitionToFilesWithNoRanges.containsKey(partition)) {
            partitionToFilesWithNoRanges.put(partition, new HashSet<>());
          }
          partitionToFilesWithNoRanges.get(partition).add(indexFileInfo.getFileId());
        }
      });
      partitionToFileIndexLookUpTree.put(partition, lookUpTree);
    });
  }

  @Override
  public Set<Pair<String, Pair<String, String>>> getMatchingFilesAndPartition(String partitionPath, String recordKey) {
    Set<Pair<String, Pair<String, String>>> toReturn = new HashSet<>();
    // could be null, if there are no files in a given partition yet or if all index files have no ranges
    if (partitionToFileIndexLookUpTree.containsKey(partitionPath)) {
      partitionToFileIndexLookUpTree.get(partitionPath).getMatchingIndexFiles(recordKey).forEach(fileId ->
          toReturn.add(Pair.of(partitionPath, Pair.of(fileId, fileIdToFilenameMap.get(fileId)))));
    }
    if (partitionToFilesWithNoRanges.containsKey(partitionPath)) {
      partitionToFilesWithNoRanges.get(partitionPath).forEach(fileId ->
          toReturn.add(Pair.of(partitionPath, Pair.of(fileId, fileIdToFilenameMap.get(fileId)))));
    }
    return toReturn;
  }
}
