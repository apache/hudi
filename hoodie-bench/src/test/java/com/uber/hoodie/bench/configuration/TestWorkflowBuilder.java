package com.uber.hoodie.bench.configuration;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.WorkflowDag;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.bench.dag.nodes.UpsertNode;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TestWorkflowBuilder {

  @Test
  public void testWorkloadOperationSequenceBuilder() {

    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(10000)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    DagNode child1 = new UpsertNode(Config.newBuilder()
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
    Config config = dagNode.getConfig();
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
    System.out.println(workflowDag.dagView());
  }

}
