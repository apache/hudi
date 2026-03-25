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

package org.apache.hudi.metadata.index.model;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Holds the initialization records and data partition if the index is a partitioned index, e.g., partitioned record level index.
 * <p>
 * For non-partitioned indexes, {@code dataPartition} is empty and the records represent the entire index records.
 */
@AllArgsConstructor
@Getter
@Accessors(fluent = true)
public class DataPartitionAndRecords {
  private final int numFileGroups;
  private final Option<String> dataPartition;
  private final HoodieData<HoodieRecord> indexRecords;
}
