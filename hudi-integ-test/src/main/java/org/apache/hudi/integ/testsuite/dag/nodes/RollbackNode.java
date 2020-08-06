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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

public class RollbackNode extends DagNode<Option<HoodieInstant>> {

  public RollbackNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext) throws Exception {
    log.info("Executing rollback node {}", this.getName());
    // Can only be done with an instantiation of a new WriteClient hence cannot be done during DeltaStreamer
    // testing for now
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(executionContext.getHoodieTestSuiteWriter().getConfiguration(),
        executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath);
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().getCommitsTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      log.info("Rolling back last instant {}", lastInstant.get());
      executionContext.getHoodieTestSuiteWriter().getWriteClient(this).rollback(lastInstant.get().getTimestamp());
      this.result = lastInstant;
    }
  }

}
