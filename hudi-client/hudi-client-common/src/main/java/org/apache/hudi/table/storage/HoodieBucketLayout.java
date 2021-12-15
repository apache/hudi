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

package org.apache.hudi.table.storage;

import org.apache.hudi.common.model.WriteOperationType;

import java.util.HashSet;
import java.util.Set;

public class HoodieBucketLayout extends HoodieStorageLayout {

  public static final Set<WriteOperationType> SUPPORTED_OPERATIONS = new HashSet<WriteOperationType>() {{
      add(WriteOperationType.INSERT);
      add(WriteOperationType.INSERT_PREPPED);
      add(WriteOperationType.UPSERT);
      add(WriteOperationType.UPSERT_PREPPED);
      add(WriteOperationType.INSERT_OVERWRITE);
      add(WriteOperationType.DELETE);
      add(WriteOperationType.COMPACT);
      add(WriteOperationType.DELETE_PARTITION);
      // TODO: HUDI-2155 bulk insert support bucket index.
      // TODO: HUDI-2156 cluster the table with bucket index.
    }};

  public HoodieBucketLayout() {
    super();
    partitionClass = "org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner";
  }

  @Override
  public boolean requireOneFileForBucket() {
    return true;
  }

  @Override
  public boolean isUniqueDistribution() {
    return true;
  }

  @Override
  public boolean operationConstraint(WriteOperationType operationType) {
    return !SUPPORTED_OPERATIONS.contains(operationType);
  }
}
