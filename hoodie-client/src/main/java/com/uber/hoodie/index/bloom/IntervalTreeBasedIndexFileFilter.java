/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.index.bloom;

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
  private final Map<String, Set<String>> filesWithNoRanges = new HashMap<>();

  /**
   * Instantiates {@link IntervalTreeBasedIndexFileFilter}
   *
   * @param partitionToFileIndexInfo Map of partition to List of {@link BloomIndexFileInfo}s
   */
  IntervalTreeBasedIndexFileFilter(final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo) {
    partitionToFileIndexInfo.forEach((partition, bloomIndexFiles) -> {
      // Note that the interval tree implementation doesn't have auto-balancing to ensure logN search time.
      // So, we are shuffling the input here hoping the tree will not have any skewness. If not, the tree could be
      // skewed which could result in N search time instead of logN.
      Collections.shuffle(bloomIndexFiles);
      KeyRangeLookupTree lookUpTree = new KeyRangeLookupTree();
      bloomIndexFiles.forEach(indexFileInfo -> {
        if (indexFileInfo.hasKeyRanges()) {
          lookUpTree.insert(new KeyRangeNode(indexFileInfo.getMinRecordKey(),
              indexFileInfo.getMaxRecordKey(), indexFileInfo.getFileName()));
        } else {
          if (!filesWithNoRanges.containsKey(partition)) {
            filesWithNoRanges.put(partition, new HashSet<>());
          }
          filesWithNoRanges.get(partition).add(indexFileInfo.getFileName());
        }
      });
      partitionToFileIndexLookUpTree.put(partition, lookUpTree);
    });
  }

  @Override
  public Set<String> getMatchingFiles(String partitionPath, String recordKey) {
    Set<String> toReturn = new HashSet<>();
    if (partitionToFileIndexLookUpTree
        .containsKey(partitionPath)) { // could be null, if there are no files in a given partition yet or if all
      // index files has no ranges
      partitionToFileIndexLookUpTree.get(partitionPath).getMatchingIndexFiles(recordKey)
          .forEach(matchingFileName -> {
            toReturn.add(matchingFileName);
          });
    }
    if (filesWithNoRanges.containsKey(partitionPath)) {
      filesWithNoRanges.get(partitionPath).forEach(fileName -> {
        toReturn.add(fileName);
      });
    }
    return toReturn;
  }
}
