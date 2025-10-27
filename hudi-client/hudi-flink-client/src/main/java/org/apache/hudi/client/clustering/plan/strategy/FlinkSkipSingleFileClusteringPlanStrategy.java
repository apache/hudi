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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An extension of {@link FlinkSizeBasedClusteringPlanStrategy} that skips clustering
 * for partitions containing only one small file.
 */
public class FlinkSkipSingleFileClusteringPlanStrategy<T>
    extends FlinkSizeBasedClusteringPlanStrategy<T> {

  public FlinkSkipSingleFileClusteringPlanStrategy(HoodieTable table,
                                              HoodieEngineContext engineContext,
                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected Stream<FileSlice> getFileSlicesEligibleForClustering(final String partition) {
    List<FileSlice> fileSlices = super.getFileSlicesEligibleForClustering(partition)
            // Only files that have base file size smaller than small file size are eligible.
            .filter(slice -> slice.getBaseFile().map(HoodieBaseFile::getFileSize).orElse(0L)
                    < getWriteConfig().getClusteringSmallFileLimit())
            .collect(Collectors.toList());

    //  if some special sort columns are declared, we can not skip the clustering.
    if (!StringUtils.isNullOrEmpty(getWriteConfig().getClusteringSortColumns())) {
      return fileSlices.stream();
    }

    if (fileSlices.size() > 1) {
      return fileSlices.stream();
    }
    return Stream.empty();
  }
}
