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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.util.DataTypeUtils;

import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        Object partVal = partKey.equals(defaultParName)
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

  public static PartitionPruner getInstance(
      List<ExpressionEvaluators.Evaluator> partitionEvaluators,
      List<String> partitionKeys,
      List<DataType> partitionTypes,
      String defaultParName,
      boolean hivePartition) {
    ValidationUtils.checkState(!partitionEvaluators.isEmpty());
    return new DynamicPartitionPruner(partitionEvaluators, partitionKeys, partitionTypes, defaultParName, hivePartition);
  }

  public static PartitionPruner getInstance(Collection<String> candidatePartitions) {
    return new StaticPartitionPruner(candidatePartitions);
  }

  public static PartitionPruner getInstance(String... candidatePartitions) {
    return new StaticPartitionPruner(Arrays.asList(candidatePartitions));
  }
}
