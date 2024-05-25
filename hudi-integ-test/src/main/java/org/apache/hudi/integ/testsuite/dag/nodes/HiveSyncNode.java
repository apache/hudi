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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.sync.common.util.SyncUtilHelpers;

import org.apache.hadoop.fs.Path;

/**
 * Represents a hive sync node in the DAG of operations for a workflow. Helps to sync hoodie data to hive table.
 */
public class HiveSyncNode extends DagNode<Boolean> {

  public HiveSyncNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    log.info("Executing hive sync node");
    SyncUtilHelpers.runHoodieMetaSync(HiveSyncTool.class.getName(), new TypedProperties(executionContext.getHoodieTestSuiteWriter().getProps()),
        executionContext.getHoodieTestSuiteWriter().getConfiguration(),
        new Path(executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath).getFileSystem(executionContext.getHoodieTestSuiteWriter().getConfiguration()),
        executionContext.getHoodieTestSuiteWriter().getCfg().targetBasePath, executionContext.getHoodieTestSuiteWriter().getCfg().baseFileFormat);
  }
}
