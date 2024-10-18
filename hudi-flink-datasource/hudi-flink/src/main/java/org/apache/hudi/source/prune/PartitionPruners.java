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

package org.apache.hudi.source.prune;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.source.ExpressionEvaluators.Evaluator;
import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.source.stats.ColumnStatsIndices;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tools to prune partitions.
 */
public class PartitionPruners {

  public interface PartitionPruner extends Serializable {

    /**
     * Applies partition pruning on the given partition list, return remained partitions.
     */
    Set<String> filter(Collection<String> partitions);
  }

  /**
   * Dynamic partition pruner for hoodie table source which partitions list is available in runtime phase.
   *
   * <p>Note: The data of new partitions created after the job starts could be read if they match the
   * filter conditions.
   */
  public static class DynamicPartitionPruner implements PartitionPruner {

    private static final long serialVersionUID = 1L;

    private final List<ExpressionEvaluators.Evaluator> partitionEvaluator;

    private final String[] partitionKeys;

    private final List<DataType> partitionTypes;

    private final String defaultParName;

    private final boolean hivePartition;

    private DynamicPartitionPruner(
        List<ExpressionEvaluators.Evaluator> partitionEvaluators,
        List<String> partitionKeys,
        List<DataType> partitionTypes,
        String defaultParName,
        boolean hivePartition) {
      this.partitionEvaluator = partitionEvaluators;
      this.partitionKeys = partitionKeys.toArray(new String[] {});
      this.partitionTypes = partitionTypes;
      this.defaultParName = defaultParName;
      this.hivePartition = hivePartition;
    }

    public Set<String> filter(Collection<String> partitions) {
      return partitions.stream().filter(this::evaluate).collect(Collectors.toSet());
    }

    private boolean evaluate(String partition) {
      String[] partStrArray = FilePathUtils.extractPartitionKeyValues(
          new org.apache.hadoop.fs.Path(partition),
          hivePartition,
          partitionKeys).values().toArray(new String[] {});
      Map<String, ColumnStats> partStats = new LinkedHashMap<>();
      for (int idx = 0; idx < partitionKeys.length; idx++) {
        String partKey = partitionKeys[idx];
        Object partVal = partStrArray[idx].equals(defaultParName)
            ? null : DataTypeUtils.resolvePartition(partStrArray[idx], partitionTypes.get(idx));
        ColumnStats columnStats = new ColumnStats(partVal, partVal, partVal == null ? 1 : 0);
        partStats.put(partKey, columnStats);
      }
      return partitionEvaluator.stream().allMatch(evaluator -> evaluator.eval(partStats));
    }
  }

  /**
   * Static partition pruner for hoodie table source which partitions list is available in compile phase.
   * After applied this partition pruner, hoodie source could not read the data from other partitions during runtime.
   *
   * <p>Note: the data of new partitions created after the job starts would never be read.
   */
  public static class StaticPartitionPruner implements PartitionPruner {

    private static final long serialVersionUID = 1L;

    private final Set<String> partitions;

    private StaticPartitionPruner(Collection<String> partitions) {
      this.partitions = new HashSet<>(partitions);
    }

    public Set<String> filter(Collection<String> partitions) {
      return partitions.stream()
          .filter(this.partitions::contains).collect(Collectors.toSet());
    }
  }

  /**
   * ColumnStats partition pruner for hoodie table source which enables partition stats index.
   *
   * <p>Note: The data of new partitions created after the job starts could be read if they match the
   * filter conditions.
   */
  public static class ColumnStatsPartitionPruner implements PartitionPruner {
    private final String basePath;
    private final HoodieMetadataConfig metadataConfig;
    private final DataPruner dataPruner;
    private final RowType rowType;

    public ColumnStatsPartitionPruner(
        RowType rowType,
        String basePath,
        HoodieMetadataConfig metadataConfig,
        DataPruner dataPruner) {
      this.rowType = rowType;
      this.basePath = basePath;
      this.metadataConfig = metadataConfig;
      this.dataPruner = dataPruner;
    }

    @Override
    public Set<String> filter(Collection<String> partitions) {
      Set<String> candidatePartitions = ColumnStatsIndices.candidatePartitionsInMetadataTable(
          basePath, metadataConfig, rowType, dataPruner, new ArrayList<>(partitions));
      if (candidatePartitions == null) {
        return new HashSet<>(partitions);
      }
      return partitions.stream().filter(candidatePartitions::contains).collect(Collectors.toSet());
    }
  }

  /**
   * Chained partition pruner for hoodie table source which combines multiple partition pruners.
   */
  public static class ChainedPartitionPruner implements PartitionPruner {
    private final List<PartitionPruner> pruners;

    public ChainedPartitionPruner(List<PartitionPruner> pruners) {
      this.pruners = pruners;
    }

    @Override
    public Set<String> filter(Collection<String> partitions) {
      for (PartitionPruner pruner: pruners) {
        partitions = pruner.filter(partitions);
      }
      return new HashSet<>(partitions);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private RowType rowType;
    private String basePath;
    private Configuration conf;
    private DataPruner dataPruner;
    private List<ExpressionEvaluators.Evaluator> partitionEvaluators;
    private List<String> partitionKeys;
    private List<DataType> partitionTypes;
    private String defaultParName;
    private boolean hivePartition;
    private Collection<String> candidatePartitions;

    private Builder() {
    }

    public Builder rowType(RowType rowType) {
      this.rowType = rowType;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder conf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder dataPruner(DataPruner dataPruner) {
      this.dataPruner = dataPruner;
      return this;
    }

    public Builder partitionEvaluators(List<Evaluator> partitionEvaluators) {
      this.partitionEvaluators = partitionEvaluators;
      return this;
    }

    public Builder partitionKeys(List<String> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    public Builder partitionTypes(List<DataType> partitionTypes) {
      this.partitionTypes = partitionTypes;
      return this;
    }

    public Builder defaultParName(String defaultParName) {
      this.defaultParName = defaultParName;
      return this;
    }

    public Builder hivePartition(boolean hivePartition) {
      this.hivePartition = hivePartition;
      return this;
    }

    public Builder candidatePartitions(Collection<String> candidatePartitions) {
      this.candidatePartitions = candidatePartitions;
      return this;
    }

    public PartitionPruner build() {
      PartitionPruner staticPruner = null;
      if (candidatePartitions != null && !candidatePartitions.isEmpty()) {
        staticPruner = new StaticPartitionPruner(candidatePartitions);
      }
      PartitionPruner dynamicPruner = null;
      if (partitionEvaluators != null && !partitionEvaluators.isEmpty()) {
        Preconditions.checkArgument(partitionKeys != null && partitionTypes != null && defaultParName != null);
        dynamicPruner = new DynamicPartitionPruner(partitionEvaluators, partitionKeys, partitionTypes, defaultParName, hivePartition);
      }
      PartitionPruner columnStatsPruner = null;
      if (dataPruner != null
          && conf.get(FlinkOptions.READ_PARTITION_DATA_SKIPPING_ENABLED)
          && conf.get(FlinkOptions.METADATA_ENABLED)) {
        Preconditions.checkArgument(rowType != null && basePath != null && conf != null);
        columnStatsPruner = new ColumnStatsPartitionPruner(rowType, basePath, StreamerUtil.metadataConfig(conf), dataPruner);
      }
      List<PartitionPruner> partitionPruners =
          Stream.of(staticPruner, dynamicPruner, columnStatsPruner)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      return new ChainedPartitionPruner(partitionPruners);
    }
  }

  @Deprecated
  public static PartitionPruner getInstance(
      List<ExpressionEvaluators.Evaluator> partitionEvaluators,
      List<String> partitionKeys,
      List<DataType> partitionTypes,
      String defaultParName,
      boolean hivePartition) {
    ValidationUtils.checkState(!partitionEvaluators.isEmpty());
    return new DynamicPartitionPruner(partitionEvaluators, partitionKeys, partitionTypes, defaultParName, hivePartition);
  }

  @Deprecated
  public static PartitionPruner getInstance(Collection<String> candidatePartitions) {
    return new StaticPartitionPruner(candidatePartitions);
  }

  @Deprecated
  public static PartitionPruner getInstance(String... candidatePartitions) {
    return new StaticPartitionPruner(Arrays.asList(candidatePartitions));
  }
}
