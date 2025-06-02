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

package org.apache.hudi.metadata.model;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ValidationUtils;

/**
 * Data class representing initial partition data for an index.
 */
@Getter
@Accessors(fluent = true)
public class IndexPartitionInitialization {
  private final int numFileGroups;
  private final IndexPartitionData indexData;

  private IndexPartitionInitialization(int numFileGroups,
                                       String partitionName,
                                       HoodieData<HoodieRecord> records) {
    this.numFileGroups = numFileGroups;
    this.indexData = new IndexPartitionData(partitionName, records);
  }

  public static IndexPartitionInitialization of(int numFileGroup,
                                                String partitionName,
                                                HoodieData<HoodieRecord> records) {
    ValidationUtils.checkArgument(numFileGroup > 0,
        "The number of file groups of the index data should be positive");
    return new IndexPartitionInitialization(numFileGroup, partitionName, records);
  }
} 