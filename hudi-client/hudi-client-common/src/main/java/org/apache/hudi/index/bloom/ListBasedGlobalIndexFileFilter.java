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
  public Set<Pair<String, String>> getMatchingFilesAndPartition(String partitionPath, String recordKey) {
    Set<Pair<String, String>> toReturn = new HashSet<>();
    partitionToFileIndexInfo.forEach((partition, bloomIndexFileInfoList) -> bloomIndexFileInfoList.forEach(file -> {
      if (shouldCompareWithFile(file, recordKey)) {
        toReturn.add(Pair.of(partition, file.getFileId()));
      }
    }));
    return toReturn;
  }
}
