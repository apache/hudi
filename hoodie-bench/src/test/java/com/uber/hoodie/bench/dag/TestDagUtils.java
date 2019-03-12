package com.uber.hoodie.bench.dag;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestDagUtils {

  @Test
  public void testConvertYamlFromDag() throws Exception {
    ComplexDag dag = new ComplexDag();
    String yaml = DagUtils.convertDagToYaml(dag.build());
    System.out.println(yaml);
  }

  @Test
  public void testConvertYamlToDag() throws Exception {
    WorkflowDag dag = DagUtils.convertYamlToDag(
        new String(IOUtils.toByteArray(getClass().getResourceAsStream(
            "/hoodie-bench-config/complex-workflow-dag-cow.yaml")
        )));
    Assert.assertEquals(dag.getNodeList().size(), 1);
    Assert.assertEquals(((DagNode) dag.getNodeList().get(0)).getParentNodes().size(), 0);
    Assert.assertEquals(((DagNode) dag.getNodeList().get(0)).getChildNodes().size(), 1);
    DagNode firstChild = (DagNode) ((DagNode) dag.getNodeList().get(0)).getChildNodes().get(0);
    Assert.assertEquals(firstChild.getParentNodes().size(), 1);
    Assert.assertEquals(firstChild.getChildNodes().size(), 1);
    Assert.assertEquals(((DagNode) firstChild.getChildNodes().get(0)).getChildNodes().size(), 1);
  }

  public static class ComplexDag extends WorkflowDagGenerator {

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
