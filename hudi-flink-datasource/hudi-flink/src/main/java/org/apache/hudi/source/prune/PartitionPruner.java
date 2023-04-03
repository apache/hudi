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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.source.stats.ColumnStats;
import org.apache.hudi.util.DataTypeUtils;

import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Dynamic partition pruner for hoodie table source which partitions list is available in runtime phase. Note: the data of new partitions created after the job starts could be read if they match the
 * filter conditions.
 */
public class PartitionPruner implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Option<List<ExpressionEvaluators.Evaluator>> partitionEvaluatorOpt;

  private final List<String> partitionKeys;

  private final List<DataType> partitionTypes;

  private final String defaultParName;

  private final boolean hivePartition;

  private final Option<Set<String>> candidatePartitionOpt;

  public PartitionPruner(
      List<ExpressionEvaluators.Evaluator> partitionEvaluators,
      List<String> partitionKeys,
      List<DataType> partitionTypes,
      String defaultParName,
      boolean hivePartition) {
    this(null, partitionEvaluators, partitionKeys, partitionTypes, defaultParName, hivePartition);
  }

  public PartitionPruner(Set<String> candidatePartitions) {
    this(candidatePartitions, null, null, null, null, false);
  }

  private PartitionPruner(
      Set<String> candidatePartitions,
      List<ExpressionEvaluators.Evaluator> partitionEvaluators,
      List<String> partitionKeys,
      List<DataType> partitionTypes,
      String defaultParName,
      boolean hivePartition) {
    this.candidatePartitionOpt = Option.ofNullable(candidatePartitions);
    this.partitionEvaluatorOpt = Option.ofNullable(partitionEvaluators);
    this.partitionKeys = partitionKeys;
    this.partitionTypes = partitionTypes;
    this.defaultParName = defaultParName;
    this.hivePartition = hivePartition;
  }

  public Set<String> filter(Collection<String> partitions) {
    return partitions.stream().filter(this::evaluate).collect(Collectors.toSet());
  }

  private boolean evaluate(String partition) {
    if (candidatePartitionOpt.isPresent() && !candidatePartitionOpt.get().contains(partition)) {
      return false;
    }
    if (!partitionEvaluatorOpt.isPresent() || partitionEvaluatorOpt.get().isEmpty()) {
      return true;
    }
    String[] partStrArray = extractPartitionValues(partition, partitionKeys, hivePartition);
    Map<String, ColumnStats> partStats = new LinkedHashMap<>();
    for (int idx = 0; idx < partitionKeys.size(); idx++) {
      String partKey = partitionKeys.get(idx);
      Object partVal = partKey.equals(defaultParName)
          ? null : DataTypeUtils.resolvePartition(partStrArray[idx], partitionTypes.get(idx));
      ColumnStats columnStats = new ColumnStats(partVal, partVal, partVal == null ? 1 : 0);
      partStats.put(partKey, columnStats);
    }
    return partitionEvaluatorOpt.get().stream().allMatch(evaluator -> evaluator.eval(partStats));
  }

  private static String[] extractPartitionValues(
      String partitionPath,
      List<String> partitionKeys,
      boolean hivePartition) {
    String[] paths = partitionPath.split(Path.SEPARATOR);
    ValidationUtils.checkArgument(
        paths.length == partitionKeys.size(),
        "Illegal partition: " + partitionPath);
    if (hivePartition) {
      String[] partitionValues = new String[paths.length];
      for (int idx = 0; idx < paths.length; idx++) {
        String[] kv = paths[idx].split("=");
        ValidationUtils.checkArgument(
            kv.length == 2 && kv[0].equals(partitionKeys.get(idx)),
            "Illegal partition: " + partitionPath);
        partitionValues[idx] = kv[1];
      }
      return partitionValues;
    } else {
      return paths;
    }
  }
}
