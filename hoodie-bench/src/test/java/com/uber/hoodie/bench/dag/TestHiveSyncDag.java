package com.uber.hoodie.bench.dag;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.HiveQueryNode;
import com.uber.hoodie.bench.dag.nodes.HiveSyncNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.common.util.collection.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestHiveSyncDag extends WorkflowDagGenerator {

  @Override
  public WorkflowDag build() {
    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(100)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(1)
        .withRecordSize(1000).build());

    DagNode child1 = new HiveSyncNode(Config.newBuilder().withHiveLocal(true).build());

    root.addChildNode(child1);

    DagNode child2 = new HiveQueryNode(Config.newBuilder().withHiveLocal(true).withHiveQueryAndResults(Arrays.asList(Pair.of
        ("select " + "count(*) from testdb1.hive_trips group " + "by rider having count(*) < 1", 0))).build());
    child1.addChildNode(child2);

    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);
    return new WorkflowDag(rootNodes);
  }

}
