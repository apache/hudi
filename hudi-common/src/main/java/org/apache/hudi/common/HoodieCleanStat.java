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

package org.apache.hudi.common;

import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.util.CollectionUtils;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.List;

/**
 * Collects stats about a single partition clean operation.
 */
@Builder(setterPrefix = "with")
@Value
public class HoodieCleanStat implements Serializable {

  // Policy used
  HoodieCleaningPolicy policy;
  // Partition path cleaned
  String partitionPath;
  // The patterns that were generated for the delete operation
  @Builder.Default
  List<String> deletePathPatterns = CollectionUtils.createImmutableList();
  @Builder.Default
  List<String> successDeleteFiles = CollectionUtils.createImmutableList();
  // Files that could not be deleted
  @Builder.Default
  List<String> failedDeleteFiles = CollectionUtils.createImmutableList();
  // Earliest commit that was retained in this clean
  String earliestCommitToRetain;
  // Last completed commit timestamp before clean
  String lastCompletedCommitTimestamp;
  // Bootstrap Base Path patterns that were generated for the delete operation
  @Builder.Default
  List<String> deleteBootstrapBasePathPatterns = CollectionUtils.createImmutableList();
  @Builder.Default
  List<String> successDeleteBootstrapBaseFiles = CollectionUtils.createImmutableList();
  // Files that could not be deleted
  @Builder.Default
  List<String> failedDeleteBootstrapBaseFiles = CollectionUtils.createImmutableList();
  // set to true if partition is deleted
  boolean isPartitionDeleted;
}
