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

package org.apache.hudi.source.split;

import org.apache.flink.core.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.source.HoodieScanContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of HoodieContinuousSplitDiscover.
 */
public class DefaultHoodieSplitDiscover implements HoodieContinuousSplitDiscover {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultHoodieSplitDiscover.class);

  private final HoodieTableMetaClient metaClient;
  private final HoodieScanContext scanContext;
  private final IncrementalInputSplits incrementalInputSplits;

  public DefaultHoodieSplitDiscover(
      HoodieScanContext scanContext,
      HoodieTableMetaClient metaClient) {
    this.scanContext = scanContext;
    this.metaClient = metaClient;
    this.incrementalInputSplits = IncrementalInputSplits.builder()
        .conf(scanContext.getConf())
        .path(new Path(scanContext.getPath().toUri()))
        .rowType(scanContext.getRowType())
        .maxCompactionMemoryInBytes(scanContext.getMaxCompactionMemoryInBytes())
        .skipCompaction(scanContext.isSkipCompaction())
        .skipClustering(scanContext.isSkipClustering())
        .skipInsertOverwrite(scanContext.isSkipInsertOverwrite())
        .partitionPruner(scanContext.getPartitionPruner()).build();
  }

  @Override
  public HoodieContinuousSplitBatch discoverSplits(String lastInstant) {
    return incrementalInputSplits.inputHoodieSourceSplits(metaClient, lastInstant, scanContext.isCdcEnabled());
  }
}
