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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.helpers.DFSTestSuitePathSelector;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import lombok.extern.slf4j.Slf4j;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A rollback node in the DAG helps to perform rollback operations.
 */
@Slf4j
public class RollbackNode extends DagNode<Option<HoodieInstant>> {

  public RollbackNode(Config config) {
    this.config = config;
  }

  /**
   * Method helps to rollback the last commit instant in the timeline, if it has one.
   *
   * @param executionContext Execution context to perform this rollback
   * @param curItrCount current iteration count.
   * @throws Exception will be thrown if any error occurred
   */
  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    int numRollbacks = config.getNumRollbacks();
    log.info("Executing rollback node {} with {} rollbacks", this.getName(), numRollbacks);
    // Can only be done with an instantiation of a new WriteClient hence cannot be done during DeltaStreamer
    // testing for now
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(executionContext.getHoodieTestSuiteWriter().getConfiguration()))
        .setBasePath(executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath)
        .build();
    for (int i = 0; i < numRollbacks; i++) {
      metaClient.reloadActiveTimeline();
      Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().getCommitsTimeline().lastInstant();
      if (lastInstant.isPresent()) {
        log.info("Rolling back last instant {}", lastInstant.get());
        log.info(
            "Cleaning up generated data for the instant being rolled back {}", lastInstant.get());
        ValidationUtils.checkArgument(
            getStringWithAltKeys(executionContext.getWriterContext().getProps(),
                DFSPathSelectorConfig.SOURCE_INPUT_SELECTOR, DFSPathSelector.class.getName())
                .equalsIgnoreCase(DFSTestSuitePathSelector.class.getName()),
            "Test Suite only supports DFSTestSuitePathSelector");
        executionContext.getHoodieTestSuiteWriter().getWriteClient(this)
            .rollback(lastInstant.get().requestedTime());
        metaClient.getStorage().deleteDirectory(new StoragePath(
            executionContext.getWriterContext().getCfg().inputBasePath,
            executionContext.getWriterContext().getHoodieTestSuiteWriter().getLastCheckpoint()
                .map(Checkpoint::getCheckpointKey)
                .orElse("")));
        this.result = lastInstant;
      }
    }
  }

}
