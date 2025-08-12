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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.IncrementalPartitionAwareStrategy;
import org.apache.hudi.table.action.cluster.ClusteringPlanActionExecutor;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilter;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scheduling strategy with restriction that clustering groups can only contain files from same partition.
 */
public abstract class PartitionAwareClusteringPlanStrategy<T,I,K,O> extends ClusteringPlanStrategy<T,I,K,O> implements IncrementalPartitionAwareStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareClusteringPlanStrategy.class);

  public PartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Create Clustering group based on files eligible for clustering in the partition.
   * return stream of HoodieClusteringGroup and boolean partial Scheduled indicating whether all given fileSlices in the current partition have been processed.
   * For example, if some file slices will not be processed due to writeConfig.getClusteringMaxNumGroups(), then return false
   */
  protected Pair<Stream<HoodieClusteringGroup>, Boolean> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    HoodieWriteConfig writeConfig = getWriteConfig();

    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();

    // Sort fileSlices before dividing, which makes dividing more compact
    List<FileSlice> sortedFileSlices = new ArrayList<>(fileSlices);
    sortedFileSlices.sort((o1, o2) -> (int)
        ((o2.getBaseFile().isPresent() ? o2.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize())
            - (o1.getBaseFile().isPresent() ? o1.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize())));

    long totalSizeSoFar = 0;
    boolean partialScheduled = false;
    
    // Only group by schema if schema evolution is disabled
    // When schema evolution is disabled, files must have the same schema to be grouped together
    boolean enableSchemaGrouping = !writeConfig.isFileStitchingBinaryCopySchemaEvolutionEnabled();
    LOG.info("Schema grouping enabled: {}", enableSchemaGrouping);
    Integer currentGroupSchemaHash = null; // Track schema hash for current group

    for (FileSlice currentSlice : sortedFileSlices) {
      long currentSize = currentSlice.getBaseFile().isPresent() ? currentSlice.getBaseFile().get().getFileSize() : writeConfig.getParquetMaxFileSize();
      
      // Get schema hash from the file only if schema grouping is enabled
      Integer currentFileSchemaHash = enableSchemaGrouping ? getFileSchemaHash(currentSlice) : 0;
      
      // Check if we need to create a new group due to schema mismatch (only if schema grouping is enabled)
      boolean schemaMismatch = enableSchemaGrouping && currentGroupSchemaHash != null && !currentGroupSchemaHash.equals(currentFileSchemaHash);
      
      // check if max size is reached OR (schema is different and schema grouping is enabled), and create new group if needed
      if ((totalSizeSoFar + currentSize > writeConfig.getClusteringMaxBytesInGroup() || schemaMismatch) && !currentGroup.isEmpty()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, writeConfig.getClusteringTargetFileMaxBytes());
        LOG.info("Adding one clustering group " + totalSizeSoFar + " max bytes: "
            + writeConfig.getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size() + " output groups: " + numOutputGroups
            + (schemaMismatch ? " (schema change detected)" : ""));
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
        currentGroup = new ArrayList<>();
        totalSizeSoFar = 0;
        currentGroupSchemaHash = null;

        // if fileSliceGroups's size reach the max group, stop loop
        if (fileSliceGroups.size() >= writeConfig.getClusteringMaxNumGroups()) {
          LOG.info("Having generated the maximum number of groups : " + writeConfig.getClusteringMaxNumGroups());
          partialScheduled = true;
          break;
        }
      }

      // Add to the current file-group
      currentGroup.add(currentSlice);
      // Set schema hash for the group if not set and schema grouping is enabled
      if (enableSchemaGrouping && currentGroupSchemaHash == null) {
        currentGroupSchemaHash = currentFileSchemaHash;
      }
      // assume each file group size is ~= parquet.max.file.size
      totalSizeSoFar += currentSize;
    }

    if (!currentGroup.isEmpty()) {
      if (currentGroup.size() > 1 || writeConfig.shouldClusteringSingleGroup()) {
        int numOutputGroups = getNumberOfOutputFileGroups(totalSizeSoFar, writeConfig.getClusteringTargetFileMaxBytes());
        LOG.info("Adding final clustering group " + totalSizeSoFar + " max bytes: "
            + writeConfig.getClusteringMaxBytesInGroup() + " num input slices: " + currentGroup.size() + " output groups: " + numOutputGroups);
        fileSliceGroups.add(Pair.of(currentGroup, numOutputGroups));
      }
    }

    return Pair.of(fileSliceGroups.stream().map(fileSliceGroup ->
        HoodieClusteringGroup.newBuilder()
            .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
            .setNumOutputFileGroups(fileSliceGroup.getRight())
            .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
            .build()), partialScheduled);
  }

  /**
   * Return list of partition paths to be considered for clustering.
   */
  public Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> partitions) {
    return ClusteringPlanPartitionFilter.filter(partitions, getWriteConfig());
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan(ClusteringPlanActionExecutor executor, Lazy<List<String>> partitions) {
    if (!checkPrecondition()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for {}", metaClient.getBasePath());
    HoodieWriteConfig config = getWriteConfig();

    String partitionSelected = config.getClusteringPartitionSelected();
    LOG.info("Scheduling clustering partitionSelected: {}", partitionSelected);
    List<String> partitionPaths = new ArrayList<>();
    List<String> missingPartitions = new ArrayList<>();
    List<HoodieClusteringGroup> clusteringGroups;
    int clusteringMaxNumGroups = getWriteConfig().getClusteringMaxNumGroups();

    if (StringUtils.isNullOrEmpty(partitionSelected)) {
      // get matched partitions if set
      partitionPaths = getRegexPatternMatchedPartitions(config, partitions.get());
      // filter the partition paths if needed to reduce list status
    } else {
      partitionPaths = Arrays.asList(partitionSelected.split(","));
      // Users may temporarily set specific partitions for clustering.
      // Ensure the coherence of the missing partitions.
      missingPartitions = (List<String>)executor.fetchMissingPartitions(TableServiceType.CLUSTER).getRight();
    }

    Pair<List<String>, List<String>> partitionsPair = filterPartitionPaths(getWriteConfig(), partitionPaths);
    partitionPaths = partitionsPair.getLeft();
    missingPartitions.addAll(partitionsPair.getRight());
    LOG.info("Scheduling clustering partitionPaths: {}", partitionPaths);
    LOG.info("Missing Scheduled clustering partitionPaths: {}", missingPartitions);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }

    List<Pair<List<HoodieClusteringGroup>, String>> res = getEngineContext().map(partitionPaths, partitionPath -> {
      List<FileSlice> fileSlicesEligible = getFileSlicesEligibleForClustering(partitionPath).collect(Collectors.toList());
      Pair<Stream<HoodieClusteringGroup>, Boolean> groupPair = buildClusteringGroupsForPartition(partitionPath, fileSlicesEligible);
      List<HoodieClusteringGroup> clusteringGroupsPartition = groupPair.getLeft().collect(Collectors.toList());
      boolean partialScheduled = groupPair.getRight();
      // return missed partition path
      // because the candidate fileSlices in the current partition have not been completely processed.
      if (clusteringGroupsPartition.size() > clusteringMaxNumGroups) {
        return Pair.of(clusteringGroupsPartition.subList(0, clusteringMaxNumGroups), partitionPath);
      } else if (partialScheduled) {
        return Pair.of(clusteringGroupsPartition, partitionPath);
      } else {
        return Pair.of(clusteringGroupsPartition, "");
      }
    }, partitionPaths.size());

    if (config.isIncrementalTableServiceEnabled()) {
      Set<String> skippedPartitions = new HashSet<>();
      List<HoodieClusteringGroup> collectedGroups = res.stream().flatMap(pair -> {
        String missingPartition = pair.getRight();
        if (!StringUtils.isNullOrEmpty(missingPartition)) {
          // missingPartition value is not empty, which means it related candidate fileSlices all not all processed.
          // so that we need to mark this kind of partition as missing partition.
          skippedPartitions.add(missingPartition);
        }
        return pair.getLeft().stream();
      }).collect(Collectors.toList());

      clusteringGroups = collectedGroups.stream().limit(clusteringMaxNumGroups).collect(Collectors.toList());
      // sublist of collectedGroups are skipped, marking related partition as missing partitions.
      collectedGroups.subList(Math.min(clusteringMaxNumGroups, collectedGroups.size()), collectedGroups.size()).forEach(group -> {
        String missed = group.getSlices().get(0).getPartitionPath();
        skippedPartitions.add(missed);
      });

      skippedPartitions.addAll(missingPartitions);
      missingPartitions = new ArrayList<>(skippedPartitions);
    } else {
      clusteringGroups = res.stream().flatMap(pair -> pair.getLeft().stream()).limit(clusteringMaxNumGroups).collect(Collectors.toList());
      missingPartitions = null;
    }

    if (clusteringGroups.isEmpty()) {
      LOG.warn("No data available to cluster");
      return Option.empty();
    }

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(getWriteConfig().getClusteringExecutionStrategyClass())
        .setStrategyParams(getStrategyParams())
        .build();

    return Option.of(HoodieClusteringPlan.newBuilder()
        .setStrategy(strategy)
        .setInputGroups(clusteringGroups)
        .setExtraMetadata(getExtraMetadata())
        .setVersion(getPlanVersion())
        .setPreserveHoodieMetadata(true)
        .setMissingSchedulePartitions(missingPartitions)
        .build());
  }

  public List<String> getRegexPatternMatchedPartitions(HoodieWriteConfig config, List<String> partitionPaths) {
    String pattern = config.getClusteringPartitionFilterRegexPattern();
    if (!StringUtils.isNullOrEmpty(pattern)) {
      partitionPaths = partitionPaths.stream()
          .filter(partition -> Pattern.matches(pattern, partition))
          .collect(Collectors.toList());
    }
    return partitionPaths;
  }

  protected int getNumberOfOutputFileGroups(long groupSize, long targetFileSize) {
    return (int) Math.ceil(groupSize / (double) targetFileSize);
  }
  
  /**
   * Get the schema hash for a file slice to enable schema-based grouping.
   * @param fileSlice The file slice to get schema hash from
   * @return Hash code of the schema for efficient comparison
   */
  private Integer getFileSchemaHash(FileSlice fileSlice) {
    if (fileSlice.getBaseFile().isPresent()) {
      String filePath = fileSlice.getBaseFile().get().getPath();
      try {
        // Use centralized ParquetUtils method to read schema hash
        return ParquetUtils.readSchemaHash(
            getHoodieTable().getStorage(),
            new org.apache.hudi.storage.StoragePath(filePath));
      } catch (Exception e) {
        LOG.warn("Failed to read schema hash from file: " + filePath, e);
        // Return a default hash if we can't read the schema
        return 0;
      }
    }
    // Return default hash for files without base file
    return 0;
  }
}
