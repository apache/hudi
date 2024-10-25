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

package org.apache.hudi.source.stats;

import org.apache.hudi.source.prune.ColumnStatsProbe;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Base support that leverages Metadata Table's indexes, such as Column Stats Index
 * and Partition Stats Index, to prune files and partitions.
 */
public interface ColumnStatsIndex extends Serializable {

  /**
   * Returns the partition name of the index.
   */
  String getIndexPartitionName();

  /**
   * Computes the filtered files with given candidates.
   *
   * @param columnStatsProbe The utility to filter the column stats metadata.
   * @param allFile          The file name list of the candidate files.
   *
   * @return The set of filtered file names
   */
  Set<String> computeCandidateFiles(ColumnStatsProbe columnStatsProbe, List<String> allFile);

  /**
   * Computes the filtered partition paths with given candidates.
   *
   * @param columnStatsProbe The utility to filter the column stats metadata.
   * @param allPartitions    The <strong>relative</strong> partition path list of the candidate partitions.
   *
   * @return The set of filtered relative partition paths
   */
  Set<String> computeCandidatePartitions(ColumnStatsProbe columnStatsProbe, List<String> allPartitions);
}
