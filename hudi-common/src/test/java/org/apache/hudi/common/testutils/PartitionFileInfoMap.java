/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PartitionFileInfoMap {
  Map<String, Map<String, List<Pair<String, Integer>>>> partitionToFileIdMap = new HashMap<>();

  public PartitionFileInfoMap addPartitionAndBasefiles(String commitTime, String partitionPath, List<Integer> lengths) {

    if (!partitionToFileIdMap.containsKey(commitTime)) {
      partitionToFileIdMap.put(commitTime, new HashMap<>());
    }
    if (!this.partitionToFileIdMap.get(commitTime).containsKey(partitionPath)) {
      this.partitionToFileIdMap.get(commitTime).put(partitionPath, new ArrayList<>());
    }

    List<Pair<String, Integer>> fileInfos = new ArrayList<>();
    for (int length : lengths) {
      fileInfos.add(Pair.of(UUID.randomUUID().toString(), length));
    }
    this.partitionToFileIdMap.get(commitTime).get(partitionPath).addAll(fileInfos);
    return this;
  }

  public Map<String, List<Pair<String, Integer>>> getPartitionToFileIdMap(String commitTime) {
    return this.partitionToFileIdMap.get(commitTime);
  }
}