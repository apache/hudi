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

package org.apache.hudi.metadata.index.model;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.DefaultMetadataTableFileGroupIndexParser;
import org.apache.hudi.metadata.MetadataTableFileGroupIndexParser;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;

/**
 * Represents the initialization data for a single metadata index partition,
 * including file-group count, optional source data partition, destination metadata
 * partition path, and records to commit.
 */
@AllArgsConstructor
@Getter
@Accessors(fluent = true)
public class IndexPartitionInitialization {
  private final int totalFileGroups;
  private final String indexPartitionName;
  private final MetadataTableFileGroupIndexParser indexParser;
  private final List<DataPartitionAndRecords> dataPartitionAndRecords;

  /**
   * Creates initialization data for a non-partitioned index from a single {@link DataPartitionAndRecords}
   * payload and a default file-group parser.
   *
   * @param indexPartitionName      metadata index partition path to initialize
   * @param dataPartitionAndRecords records and metadata for one data partition (or non-partitioned payload)
   * @return an {@link IndexPartitionInitialization} instance configured with file-group count inferred
   *         from {@code dataPartitionAndRecords}
   */
  public static IndexPartitionInitialization of(String indexPartitionName, DataPartitionAndRecords dataPartitionAndRecords) {
    return of(dataPartitionAndRecords.numFileGroups(), indexPartitionName, dataPartitionAndRecords.indexRecords());
  }

  /**
   * Creates initialization data for a non-partitioned index using a single records payload and the default
   * {@link MetadataTableFileGroupIndexParser} implementation.
   *
   * @param totalFileGroups    total number of file groups to pre-create for this index partition
   * @param indexPartitionName metadata index partition path/name to initialize
   * @param indexRecords       records to be written into the target index partition
   * @return an {@link IndexPartitionInitialization} containing one {@link DataPartitionAndRecords}
   *         entry without source data-partition binding
   */
  public static IndexPartitionInitialization of(int totalFileGroups, String indexPartitionName, HoodieData<HoodieRecord> indexRecords) {
    return of(totalFileGroups, indexPartitionName, new DefaultMetadataTableFileGroupIndexParser(totalFileGroups),
        Collections.singletonList(new DataPartitionAndRecords(totalFileGroups, Option.empty(), indexRecords)));
  }

  /**
   * Creates initialization data for a partitioned index with a provided file-group parser and
   * one or more per-data-partition record payloads.
   *
   * @param totalFileGroups         total number of file groups to pre-create for this index partition
   * @param indexPartitionName      metadata index partition path to initialize
   * @param indexParser             parser used to map records to metadata table file-group indexes
   * @param dataPartitionAndRecords input record payloads grouped by source data partition
   * @return an {@link IndexPartitionInitialization} with the provided parser and partitioned payloads
   */
  public static IndexPartitionInitialization of(int totalFileGroups, String indexPartitionName, MetadataTableFileGroupIndexParser indexParser, List<DataPartitionAndRecords> dataPartitionAndRecords) {
    return new IndexPartitionInitialization(totalFileGroups, indexPartitionName, indexParser, dataPartitionAndRecords);
  }
}
