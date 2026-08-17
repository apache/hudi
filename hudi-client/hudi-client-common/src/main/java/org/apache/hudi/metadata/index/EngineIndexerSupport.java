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

package org.apache.hudi.metadata.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.model.FileInfoAndPartition;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.List;
import java.util.function.Function;

/**
 * Engine-specific support required by metadata indexes.
 */
public interface EngineIndexerSupport {
  EngineType getEngineType();

  /**
   * Converts expression-index column range metadata produced by an engine into metadata records.
   * Keep this as a named type so the engine support interface exposes a capability instead of
   * leaking the full expression-index update workflow into engine implementations.
   */
  interface PartitionStatsRecordsFunction extends Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>> {
  }

  /**
   * Generates expression index records for the provided files.
   *
   * @param filesToIndex    information for files to index, including partition, file path and file size
   * @param indexDefinition definition of the expression index
   * @param metaClient      {@link HoodieTableMetaClient} instance
   * @param parallelism     parallelism to use for engine operations
   * @param tableSchema     table schema
   * @param readerSchema    reader schema
   * @param storageConf     storage config
   * @param instantTime     instant time
   * @param partitionRecordsFunctionOpt function used to convert generated column range metadata into partition stat records
   * @return expression index records
   */
  HoodieData<HoodieRecord> generateExpressionIndexRecords(
      List<FileInfoAndPartition> filesToIndex,
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient,
      int parallelism,
      HoodieSchema tableSchema,
      HoodieSchema readerSchema,
      StorageConfiguration<?> storageConf,
      String instantTime,
      Option<PartitionStatsRecordsFunction> partitionRecordsFunctionOpt);

  /**
   * Loads the expression-index column range metadata for files that were not modified by the
   * given commit but belong to partitions affected by the commit. The common indexer owns the
   * update flow and merges these with newly generated column range metadata to produce partition
   * stat records.
   *
   * @param dataTableMetaClient {@link HoodieTableMetaClient} for the data table
   * @param tableMetadata       metadata table reader used to load existing index content
   * @param commitMetadata      commit metadata
   * @param indexDefinition     definition of the expression index
   * @param indexPartition      metadata partition path for the expression index
   * @param instantTime         completed data-table instant time being indexed
   * @return existing expression-index column range metadata grouped by data partition
   */
  HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> loadExpressionIndexPartitionStats(
      HoodieTableMetaClient dataTableMetaClient,
      HoodieTableMetadata tableMetadata,
      HoodieCommitMetadata commitMetadata,
      HoodieIndexDefinition indexDefinition,
      String indexPartition,
      String instantTime);
}
