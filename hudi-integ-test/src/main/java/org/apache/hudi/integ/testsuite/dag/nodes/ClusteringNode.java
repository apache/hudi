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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

/**
 * Triggers inline clustering. Works only with writeClient. Also, add this as last node and end with validation if possible. As of now, after clustering, further inserts/upserts may not work since we
 * call deltaStreamer.
 */
public class ClusteringNode extends DagNode<Option<String>> {

  public ClusteringNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    if (config.getIterationCountToExecute() == curItrCount) {
      try {
        log.warn("Executing ClusteringNode node {}", this.getName());
        executionContext.getHoodieTestSuiteWriter().inlineClustering();
      } catch (Exception e) {
        log.warn("Exception thrown in ClusteringNode Node :: " + e.getCause() + ", msg :: " + e.getMessage());
        throw e;
      }
    }
  }

}
