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

package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

/**
 * A schedule node in the DAG of operations for a workflow helps to schedule compact operation.
 */
public class ScheduleCompactNode extends DagNode<Option<String>> {

  public ScheduleCompactNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    log.info("Executing schedule compact node {}", this.getName());
    // Can only be done with an instantiation of a new WriteClient hence cannot be done during DeltaStreamer
    // testing for now
    // Find the last commit and extra the extra metadata to be passed to the schedule compaction. This is
    // done to ensure the CHECKPOINT is correctly passed from commit to commit
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(executionContext.getHoodieTestSuiteWriter().getConfiguration()))
        .setBasePath(executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath)
        .build();
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().getCommitsTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      HoodieCommitMetadata metadata = metaClient.getActiveTimeline().readCommitMetadata(lastInstant.get());
      Option<String> scheduledInstant = executionContext.getHoodieTestSuiteWriter().scheduleCompaction(Option.of(metadata
          .getExtraMetadata()));
      if (scheduledInstant.isPresent()) {
        log.info("Scheduling compaction instant {}", scheduledInstant.get());
      }
      this.result = scheduledInstant;
    }
  }
}
