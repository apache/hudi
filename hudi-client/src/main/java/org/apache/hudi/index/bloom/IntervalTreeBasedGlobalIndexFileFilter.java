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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Interval Tree based index look up for Global Index. Builds an {@link KeyRangeLookupTree} for all index files (across
 * all partitions)  and uses it to search for matching index files for any given recordKey that needs to be looked up.
 */
class IntervalTreeBasedGlobalIndexFileFilter implements IndexFileFilter {

  private final KeyRangeLookupTree indexLookUpTree = new KeyRangeLookupTree();
  private final Set<String> filesWithNoRanges = new HashSet<>();

  /**
   * Instantiates {@link IntervalTreeBasedGlobalIndexFileFilter}
   *
   * @param partitionToFileIndexInfo Map of partition to List of {@link BloomIndexFileInfo}s
   */
  IntervalTreeBasedGlobalIndexFileFilter(final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo) {
    List<BloomIndexFileInfo> allIndexFiles = partitionToFileIndexInfo.values().stream().flatMap(Collection::stream)
        .collect(Collectors.toList());
    // Note that the interval tree implementation doesn't have auto-balancing to ensure logN search time.
    // So, we are shuffling the input here hoping the tree will not have any skewness. If not, the tree could be skewed
    // which could result in N search time instead of NlogN.
    Collections.shuffle(allIndexFiles);
    allIndexFiles.forEach(indexFile -> {
      if (indexFile.hasKeyRanges()) {
        indexLookUpTree.insert(new KeyRangeNode(indexFile.getMinRecordKey(),
            indexFile.getMaxRecordKey(), indexFile.getFileId()));
      } else {
        filesWithNoRanges.add(indexFile.getFileId());
      }
    });
  }

  @Override
  public Set<String> getMatchingFiles(String partitionPath, String recordKey) {
    Set<String> toReturn = new HashSet<>();
    toReturn.addAll(indexLookUpTree.getMatchingIndexFiles(recordKey));
    toReturn.addAll(filesWithNoRanges);
    return toReturn;
  }
}
