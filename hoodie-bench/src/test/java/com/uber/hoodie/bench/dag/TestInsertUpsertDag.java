package com.uber.hoodie.bench.dag;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.bench.dag.nodes.UpsertNode;
import java.util.ArrayList;
import java.util.List;

public class TestInsertUpsertDag extends WorkflowDagGenerator {

  @Override
  public WorkflowDag build() {
    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(1000000)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    DagNode child1 = new UpsertNode(Config.newBuilder()
        .withNumRecordsToUpdate(1000000)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    root.addChildNode(child1);
    child1.addParentNode(root);
    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);
    return new WorkflowDag(rootNodes);
  }

}
