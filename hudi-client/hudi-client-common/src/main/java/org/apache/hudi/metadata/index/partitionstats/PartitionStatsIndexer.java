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

package org.apache.hudi.metadata.index.partitionstats;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.BaseIndexer;
import org.apache.hudi.metadata.index.model.IndexCleanContext;
import org.apache.hudi.metadata.index.model.IndexInitializationContext;
import org.apache.hudi.metadata.index.model.IndexInitializationPlan;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.index.model.IndexUpdateContext;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getFilesToFetchColumnStats;
import static org.apache.hudi.metadata.HoodieMetadataWriteUtils.getMaxInstantTime;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.generateColumnStatsKeys;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnsToIndex;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.translateWriteStatToFileStats;
import static org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS;

/**
 * Implementation of {@link MetadataPartitionType#PARTITION_STATS} metadata
 */
@Slf4j
public class PartitionStatsIndexer extends BaseIndexer {
  public PartitionStatsIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                                  HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexInitializationPlan> buildInitialization(IndexInitializationContext context) throws IOException {
    // Partition stats index cannot be enabled for a non-partitioned table
    if (!dataTableMetaClient.getTableConfig().isTablePartitioned()) {
      return Collections.emptyList();
    }

    // For PARTITION_STATS, COLUMN_STATS should also be enabled
    if (!dataTableWriteConfig.isMetadataColumnStatsIndexEnabled()) {
      log.debug("Skipping partition stats initialization as column stats index is not enabled. Please enable {}",
          HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      return Collections.emptyList();
    }

    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(
        engineContext, context.latestFileSlices().get(), dataTableWriteConfig.getMetadataConfig(),
        dataTableMetaClient, context.tableSchema(), Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()));
    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getPartitionStatsIndexFileGroupCount();

    return Collections.singletonList(IndexInitializationPlan.of(fileGroupCount, PARTITION_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildUpdate(IndexUpdateContext context) {
    checkState(MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(dataTableMetaClient),
        "Column stats partition must be enabled to generate partition stats. Please enable: " + HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
    // Generate Hoodie Pair data of partition name and list of column range metadata for all the files in that partition
    boolean isDeletePartition = context.commitMetadata().getOperationType().equals(WriteOperationType.DELETE_PARTITION);
    final HoodieData<HoodieRecord> records = convertMetadataToPartitionStatsRecords(
        context.commitMetadata(), context.instantTime(), engineContext, dataTableWriteConfig,
        dataTableMetaClient, context.tableMetadata(), dataTableWriteConfig.getMetadataConfig(),
        Option.of(dataTableWriteConfig.getRecordMerger().getRecordType()), isDeletePartition);
    return Collections.singletonList(IndexPartitionAndRecords.of(PARTITION_STATS.getPartitionPath(), records));
  }

  @Override
  public List<IndexPartitionAndRecords> buildClean(IndexCleanContext context) {
    return Collections.emptyList();
  }

  @VisibleForTesting
  public static HoodieData<HoodieRecord> convertMetadataToPartitionStatsRecords(HoodieCommitMetadata commitMetadata, String instantTime,
                                                                                HoodieEngineContext engineContext, HoodieWriteConfig dataWriteConfig,
                                                                                HoodieTableMetaClient dataMetaClient,
                                                                                HoodieTableMetadata tableMetadata, HoodieMetadataConfig metadataConfig,
                                                                                Option<HoodieRecord.HoodieRecordType> recordTypeOpt, boolean isDeletePartition) {
    try {
      Option<HoodieSchema> writerSchema =
          Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
              .flatMap(writerSchemaStr ->
                  isNullOrEmpty(writerSchemaStr)
                      ? Option.empty()
                      : Option.of(HoodieSchema.parse(writerSchemaStr)));
      HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();
      Option<HoodieSchema> tableSchema = writerSchema.map(schema -> tableConfig.populateMetaFields() ? HoodieSchemaUtils.addMetadataFields(schema) : schema);

      if (tableSchema.isEmpty()) {
        return engineContext.emptyHoodieData();
      }
      HoodieIndexVersion partitionStatsIndexVersion = existingIndexVersionOrDefault(PARTITION_NAME_PARTITION_STATS, dataMetaClient);
      Lazy<Option<HoodieSchema>> writerSchemaOpt = Lazy.eagerly(tableSchema);
      Map<String, HoodieSchema> columnsToIndexSchemaMap = getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig, writerSchemaOpt, false, recordTypeOpt, partitionStatsIndexVersion);
      if (columnsToIndexSchemaMap.isEmpty()) {
        return engineContext.emptyHoodieData();
      }

      // if this is DELETE_PARTITION, then create delete metadata payload for all columns for partition_stats
      if (isDeletePartition) {
        HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
        Map<String, List<String>> partitionToReplaceFileIds = replaceCommitMetadata.getPartitionToReplaceFileIds();
        List<String> partitionsToDelete = new ArrayList<>(partitionToReplaceFileIds.keySet());
        if (partitionToReplaceFileIds.isEmpty()) {
          return engineContext.emptyHoodieData();
        }
        return engineContext.parallelize(partitionsToDelete, partitionsToDelete.size()).flatMap(partition -> {
          Stream<HoodieRecord> columnRangeMetadata = columnsToIndexSchemaMap.keySet().stream()
              .flatMap(column -> HoodieMetadataPayload.createPartitionStatsRecords(
                  partition,
                  Collections.singletonList(HoodieColumnRangeMetadata.stub("", column, partitionStatsIndexVersion)),
                  true, true, Option.empty()));
          return columnRangeMetadata.iterator();
        });
      }

      // In this function we fetch column range metadata for all new files part of commit metadata along with all the other files
      // of the affected partitions. The column range metadata is grouped by partition name to generate HoodiePairData of partition name
      // and list of column range metadata for that partition files. This pair data is then used to generate partition stat records.
      List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(Collection::stream).collect(Collectors.toList());
      if (allWriteStats.isEmpty()) {
        return engineContext.emptyHoodieData();
      }

      List<String> colsToIndex = new ArrayList<>(columnsToIndexSchemaMap.keySet());
      log.debug("Indexing following columns for partition stats index: {}", columnsToIndexSchemaMap.keySet());
      // Group by partitionPath and then gather write stats lists,
      // where each inner list contains HoodieWriteStat objects that have the same partitionPath.
      List<List<HoodieWriteStat>> partitionedWriteStats = new ArrayList<>(allWriteStats.stream()
          .collect(Collectors.groupingBy(HoodieWriteStat::getPartitionPath))
          .values());
      Map<String, Set<String>> fileGroupIdsToReplaceMap = (commitMetadata instanceof HoodieReplaceCommitMetadata)
          ? ((HoodieReplaceCommitMetadata) commitMetadata).getPartitionToReplaceFileIds()
          .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())))
          : Collections.emptyMap();

      int parallelism = Math.max(Math.min(partitionedWriteStats.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
      String maxInstantTime = getMaxInstantTime(dataMetaClient, instantTime);
      HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> columnRangeMetadata =
          engineContext.parallelize(partitionedWriteStats, parallelism).mapToPair(partitionedWriteStat -> {
            final String partitionName = partitionedWriteStat.get(0).getPartitionPath();
            checkState(tableMetadata != null, "tableMetadata should not be null when scanning metadata table");

            // Collect column metadata for each file part of the latest merged file slice before the current instant time
            List<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata = partitionedWriteStat.stream()
                .flatMap(writeStat -> translateWriteStatToFileStats(writeStat, dataMetaClient, colsToIndex, partitionStatsIndexVersion).stream()).collect(toList());
            // Collect column metadata of each file that does not have column stats provided by the write stat in the commit metadata
            Set<String> filesToFetchColumnStats = getFilesToFetchColumnStats(partitionedWriteStat, dataMetaClient, tableMetadata, dataWriteConfig, partitionName, maxInstantTime,
                instantTime, fileGroupIdsToReplaceMap, colsToIndex, partitionStatsIndexVersion);
            // Fetch metadata table COLUMN_STATS partition records for the above files
            List<HoodieColumnRangeMetadata<Comparable>> partitionColumnMetadata = tableMetadata
                .getRecordsByKeyPrefixes(
                    HoodieListData.lazy(generateColumnStatsKeys(colsToIndex, partitionName)),
                    MetadataPartitionType.COLUMN_STATS.getPartitionPath(), false)
                // schema and properties are ignored in getInsertValue, so simply pass as null
                .map(record -> ((HoodieMetadataPayload) record.getData()).getColumnStatMetadata())
                .filter(Option::isPresent)
                .map(colStatsOpt -> colStatsOpt.get())
                .filter(stats -> filesToFetchColumnStats.contains(stats.getFileName()))
                .map(HoodieColumnRangeMetadata::fromColumnStats).collectAsList();
            // fileColumnMetadata already contains stats for the files from the current inflight commit.
            // Here it adds the stats for the committed files part of the latest merged file slices
            fileColumnMetadata.addAll(partitionColumnMetadata);
            return Pair.of(partitionName, fileColumnMetadata);
          });

      return HoodieTableMetadataUtil.convertMetadataToPartitionStatsRecords(
          columnRangeMetadata, dataMetaClient, columnsToIndexSchemaMap, partitionStatsIndexVersion);
    } catch (Exception e) {
      throw new HoodieException("Failed to generate partition stats records for metadata table", e);
    }
  }
}
