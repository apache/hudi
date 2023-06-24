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

package org.apache.hudi.integ.testsuite.dag.scheduler;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.integ.testsuite.dag.WorkflowDag;
import org.apache.hudi.integ.testsuite.dag.WriterContext;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaferSchemaDagScheduler extends DagScheduler {
  private static Logger LOG = LoggerFactory.getLogger(SaferSchemaDagScheduler.class);
  int processedVersion;

  public SaferSchemaDagScheduler(WorkflowDag workflowDag, WriterContext writerContext, JavaSparkContext jsc) {
    super(workflowDag, writerContext, jsc);
  }

  public SaferSchemaDagScheduler(WorkflowDag workflowDag, WriterContext writerContext, JavaSparkContext jsc, int version) {
    super(workflowDag, writerContext, jsc);
    processedVersion = version;
  }

  @Override
  protected void executeNode(DagNode node, int curRound) throws HoodieException {
    if (node.getConfig().getSchemaVersion() < processedVersion) {
      LOG.info(String.format("----------------- Processed SaferSchema version %d is available.  "
              + "Skipping redundant Insert Operation. (Processed = %d) -----------------",
          node.getConfig().getSchemaVersion(), processedVersion));
      return;
    }
    super.executeNode(node, curRound);
  }
}
