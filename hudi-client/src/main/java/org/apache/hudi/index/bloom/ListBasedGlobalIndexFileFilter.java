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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ListBasedGlobalIndexFileFilter extends ListBasedIndexFileFilter {

  /**
   * Instantiates {@link ListBasedGlobalIndexFileFilter}.
   *
   * @param partitionToFileIndexInfo Map of partition to List of {@link BloomIndexFileInfo}
   */
  ListBasedGlobalIndexFileFilter(Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo) {
    super(partitionToFileIndexInfo);
  }

  @Override
  public Set<String> getMatchingFiles(String partitionPath, String recordKey) {
    Set<String> toReturn = new HashSet<>();
    partitionToFileIndexInfo.values().forEach(indexInfos -> {
      if (indexInfos != null) { // could be null, if there are no files in a given partition yet.
        // for each candidate file in partition, that needs to be compared.
        for (BloomIndexFileInfo indexInfo : indexInfos) {
          if (shouldCompareWithFile(indexInfo, recordKey)) {
            toReturn.add(indexInfo.getFileId());
          }
        }
      }
    });
    return toReturn;
  }
}
