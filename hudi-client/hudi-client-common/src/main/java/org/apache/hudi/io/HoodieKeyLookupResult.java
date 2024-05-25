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

package org.apache.hudi.io;

import org.apache.hudi.common.util.collection.Pair;

import java.util.List;

/**
 * Encapsulates the result from a key lookup.
 */
public class HoodieKeyLookupResult {

  private final String fileId;
  private final String baseInstantTime;
  private final List<Pair<String, Long>> matchingRecordKeysAndPositions;
  private final String partitionPath;

  public HoodieKeyLookupResult(String fileId, String partitionPath, String baseInstantTime,
                               List<Pair<String, Long>> matchingRecordKeysAndPositions) {
    this.fileId = fileId;
    this.partitionPath = partitionPath;
    this.baseInstantTime = baseInstantTime;
    this.matchingRecordKeysAndPositions = matchingRecordKeysAndPositions;
  }

  public String getFileId() {
    return fileId;
  }

  public String getBaseInstantTime() {
    return baseInstantTime;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public List<Pair<String, Long>> getMatchingRecordKeysAndPositions() {
    return matchingRecordKeysAndPositions;
  }
}

