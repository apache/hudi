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

package org.apache.hudi.integ.testsuite.dag;

import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.InsertNode;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A utility class for DAG test.
 */
public class TestDagUtils {

  private static final String COW_DAG_DOCKER_DEMO_RELATIVE_PATH = "/hudi-integ-test/src/test/resources/unit-test-cow-dag.yaml";

  @Test
  public void testConvertDagToYaml() throws Exception {
    ComplexDagGenerator dag = new ComplexDagGenerator();
    String yaml = DagUtils.convertDagToYaml(dag.build());
    System.out.println(yaml);
  }

  @Test
  @Disabled
  // TODO(HUDI-3668): Fix this test
  public void testConvertDagToYamlHiveQuery() throws Exception {
    WorkflowDag dag = new HiveSyncDagGenerator().build();
    DagNode insert1 = (DagNode) dag.getNodeList().get(0);
    DagNode hiveSync1 = (DagNode)insert1.getChildNodes().get(0);
    DagNode hiveQuery1 = (DagNode)hiveSync1.getChildNodes().get(0);

    String yaml = DagUtils.convertDagToYaml(dag);

    WorkflowDag dag2 = DagUtils.convertYamlToDag(yaml);
    DagNode insert2 = (DagNode) dag2.getNodeList().get(0);
    DagNode hiveSync2 = (DagNode)insert2.getChildNodes().get(0);
    DagNode hiveQuery2 = (DagNode)hiveSync2.getChildNodes().get(0);
    assertEquals(hiveQuery1.getConfig().getHiveQueries().get(0),
        hiveQuery2.getConfig().getHiveQueries().get(0));
    assertEquals(hiveQuery1.getConfig().getHiveProperties().get(0),
        hiveQuery2.getConfig().getHiveProperties().get(0));
  }

  @Test
  public void testConvertDagToYamlAndBack() throws Exception {
    final ComplexDagGenerator dag = new ComplexDagGenerator();
    final WorkflowDag originalWorkflowDag = dag.build();
    final String yaml = DagUtils.convertDagToYaml(dag.build());
    final WorkflowDag regeneratedWorkflowDag = DagUtils.convertYamlToDag(yaml);

    final List<DagNode> originalWorkflowDagNodes = originalWorkflowDag.getNodeList();
    final List<DagNode> regeneratedWorkflowDagNodes = regeneratedWorkflowDag.getNodeList();

    assertEquals(originalWorkflowDagNodes.size(), regeneratedWorkflowDagNodes.size());
    assertEquals(originalWorkflowDagNodes.get(0).getChildNodes().size(),
            regeneratedWorkflowDagNodes.get(0).getChildNodes().size());
  }

  @Test
  public void testConvertYamlToDag() throws Exception {
    WorkflowDag dag = DagUtils.convertYamlToDag(UtilitiesTestBase.Helpers
        .readFileFromAbsolutePath((System.getProperty("user.dir") + "/.." + COW_DAG_DOCKER_DEMO_RELATIVE_PATH)));
    assertEquals(dag.getDagName(), "unit-test-cow-dag");
    assertEquals(dag.getRounds(), 1);
    assertEquals(dag.getIntermittentDelayMins(), 10);
    assertEquals(dag.getNodeList().size(), 1);
    Assertions.assertEquals(((DagNode) dag.getNodeList().get(0)).getParentNodes().size(), 0);
    assertEquals(((DagNode) dag.getNodeList().get(0)).getChildNodes().size(), 1);
    DagNode firstChild = (DagNode) ((DagNode) dag.getNodeList().get(0)).getChildNodes().get(0);
    assertEquals(firstChild.getParentNodes().size(), 1);
    assertEquals(firstChild.getChildNodes().size(), 1);
    assertEquals(((DagNode) firstChild.getChildNodes().get(0)).getChildNodes().size(), 1);
  }

  public static class ComplexDagGenerator implements WorkflowDagGenerator {

    @Override
    public WorkflowDag build() {
      DagNode root = new InsertNode(Config.newBuilder()
          .withNumRecordsToInsert(1000000)
          .withNumInsertPartitions(1)
          .withNumTimesToRepeat(2)
          .withRecordSize(1000).build());

      DagNode child1 = new InsertNode(Config.newBuilder()
          .withNumRecordsToInsert(1000000)
          .withNumInsertPartitions(1)
          .withNumTimesToRepeat(2)
          .withRecordSize(1000).build());

      DagNode child2 = new InsertNode(Config.newBuilder()
          .withNumRecordsToInsert(1000000)
          .withNumInsertPartitions(1)
          .withNumTimesToRepeat(2)
          .withRecordSize(1000).build());

      root.addChildNode(child1);
      root.addChildNode(child2);

      DagNode child3 = new InsertNode(Config.newBuilder()
          .withNumRecordsToInsert(1000000)
          .withNumInsertPartitions(1)
          .withNumTimesToRepeat(2)
          .withRecordSize(1000).build());

      child2.addChildNode(child3);
      List<DagNode> rootNodes = new ArrayList<>();
      rootNodes.add(root);

      return new WorkflowDag(rootNodes);
    }
  }

}
