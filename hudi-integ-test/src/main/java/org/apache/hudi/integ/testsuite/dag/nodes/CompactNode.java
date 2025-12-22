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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import org.apache.spark.api.java.JavaRDD;

/**
 * Represents a compact node in the DAG of operations for a workflow.
 */
public class CompactNode extends DagNode<JavaRDD<WriteStatus>> {

  public CompactNode(Config config) {
    this.config = config;
  }

  /**
   * Method helps to start the compact operation. It will compact the last pending compact instant in the timeline
   * if it has one.
   *
   * @param executionContext Execution context to run this compaction
   * @param curItrCount      cur iteration count.
   * @throws Exception will be thrown if any error occurred.
   */
  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(executionContext.getHoodieTestSuiteWriter().getConfiguration()))
        .setBasePath(executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath)
        .build();
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline()
        .getWriteTimeline().filterPendingCompactionTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      log.info("Compacting instant {}", lastInstant.get());
      this.result = executionContext.getHoodieTestSuiteWriter().compact(Option.of(lastInstant.get().getTimestamp()));
      executionContext.getHoodieTestSuiteWriter().commitCompaction(result, executionContext.getJsc().emptyRDD(), Option.of(lastInstant.get().getTimestamp()));
    }
  }
}
