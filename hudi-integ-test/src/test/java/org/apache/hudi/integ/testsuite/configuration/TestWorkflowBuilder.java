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

package org.apache.hudi.integ.testsuite.configuration;

import org.apache.hudi.integ.testsuite.dag.WorkflowDag;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.InsertNode;
import org.apache.hudi.integ.testsuite.dag.nodes.UpsertNode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for the build process of {@link DagNode} and {@link WorkflowDag}.
 */
public class TestWorkflowBuilder {

  @Test
  public void testWorkloadOperationSequenceBuilder() {

    DagNode root = new InsertNode(DeltaConfig.Config.newBuilder()
        .withNumRecordsToInsert(10000)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    DagNode child1 = new UpsertNode(DeltaConfig.Config.newBuilder()
        .withNumRecordsToUpdate(10000)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    root.addChildNode(child1);
    child1.addParentNode(root);
    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);
    WorkflowDag workflowDag = new WorkflowDag(rootNodes);

    assertEquals(workflowDag.getNodeList().size(), 1);
    assertEquals(((DagNode) workflowDag.getNodeList().get(0)).getChildNodes().size(), 1);
    DagNode dagNode = (DagNode) workflowDag.getNodeList().get(0);
    assertTrue(dagNode instanceof InsertNode);
    DeltaConfig.Config config = dagNode.getConfig();
    assertEquals(config.getNumInsertPartitions(), 1);
    assertEquals(config.getRecordSize(), 1000);
    assertEquals(config.getRepeatCount(), 2);
    assertEquals(config.getNumRecordsInsert(), 10000);
    assertEquals(config.getNumRecordsUpsert(), 0);
    dagNode = (DagNode) ((DagNode) workflowDag.getNodeList().get(0)).getChildNodes().get(0);
    assertTrue(dagNode instanceof UpsertNode);
    config = dagNode.getConfig();
    assertEquals(config.getNumInsertPartitions(), 1);
    assertEquals(config.getRecordSize(), 1000);
    assertEquals(config.getRepeatCount(), 2);
    assertEquals(config.getNumRecordsInsert(), 0);
    assertEquals(config.getNumRecordsUpsert(), 10000);
  }

}
