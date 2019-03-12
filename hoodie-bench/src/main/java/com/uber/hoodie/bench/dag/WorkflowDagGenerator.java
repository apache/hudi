package com.uber.hoodie.bench.dag;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.HiveQueryNode;
import com.uber.hoodie.bench.dag.nodes.HiveSyncNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.bench.dag.nodes.UpsertNode;
import com.uber.hoodie.common.util.collection.Pair;
import java.util.ArrayList;
import java.util.List;

public class WorkflowDagGenerator {

  public WorkflowDag build() {

    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(1000)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

//    DagNode child1 = new InsertNode(Config.newBuilder()
//        .withNumRecordsToInsert(1000)
//        .withNumInsertPartitions(1)
//        .withNumTimesToRepeat(2)
//        .withRecordSize(1000).build());
//
//    DagNode child2 = new InsertNode(Config.newBuilder()
//        .withNumRecordsToInsert(1000)
//        .withNumInsertPartitions(1)
//        .withNumTimesToRepeat(2)
//        .withRecordSize(1000).build());

//    root.addChildNode(child1);
//    root.addChildNode(child2);

//    DagNode newNode2 = new UpsertNode(Config.newBuilder()
//        .withNumRecordsToUpdate(1000)
//        .withNumUpsertPartitions(2)
//        .withNumTimesToRepeat(1)
//        .withRecordSize(1000).build());

    // child1.addChildNode(newNode2);
    // child2.addChildNode(newNode2);

//    DagNode syncNode1 = new HiveSyncNode(Config.newBuilder().withHiveLocal(true).build());

//    child1.addChildNode(syncNode1);

    List<Pair<String, Integer>> queryAndResult = new ArrayList<>();
    queryAndResult.add(Pair.of("select " + "count(*) from testdb1.hive_trips group "
        + "by rider having count(*) < 1", 0));
    DagNode queryNode1 = new HiveQueryNode(Config.newBuilder().withHiveQueryAndResults(queryAndResult).withHiveLocal
        (true).build());
    root.addChildNode(queryNode1);

    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);

    return new WorkflowDag(rootNodes);
  }

}
