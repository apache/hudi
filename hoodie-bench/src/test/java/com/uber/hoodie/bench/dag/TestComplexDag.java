package com.uber.hoodie.bench.dag;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.bench.dag.nodes.UpsertNode;
import com.uber.hoodie.bench.dag.nodes.ValidateNode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.api.java.JavaRDD;

public class TestComplexDag extends WorkflowDagGenerator {

  @Override
  public WorkflowDag build() {
    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(1000)
        .withNumInsertPartitions(3)
        .withRecordSize(1000).build());

    DagNode child1 = new UpsertNode(Config.newBuilder()
        .withNumRecordsToUpdate(999)
        .withNumRecordsToInsert(1000)
        .withNumUpsertFiles(3)
        .withNumUpsertPartitions(3)
        .withNumInsertPartitions(1)
        .withRecordSize(10000).build());

    Function<List<DagNode<JavaRDD<WriteStatus>>>, Boolean> function = (dagNodes) -> {
      DagNode<JavaRDD<WriteStatus>> parent1 = dagNodes.get(0);
      List<WriteStatus> statuses = parent1.getResult().collect();
      long totalRecordsTouched = statuses.stream().map(st -> st.getStat().getNumUpdateWrites() + st.getStat()
          .getNumInserts()).reduce((a, b) -> a + b).get();
      boolean b1 = totalRecordsTouched == parent1.getConfig().getNumRecordsInsert()
          + parent1.getConfig().getNumRecordsUpsert();
      boolean b2 = statuses.size() > parent1.getConfig().getNumUpsertFiles();

      DagNode<JavaRDD<WriteStatus>> parent2 = parent1.getParentNodes().get(0);
      statuses = parent2.getResult().collect();
      totalRecordsTouched = statuses.stream().map(st -> st.getStat().getNumUpdateWrites() + st.getStat()
          .getNumInserts()).reduce((a, b) -> a + b).get();
      boolean b3 = totalRecordsTouched == parent2.getConfig().getNumRecordsInsert()
          * parent2.getConfig().getNumInsertPartitions() + parent2.getConfig().getNumRecordsUpsert();
      assert b1 && b2 && b3 == true;
      return b1 && b2 && b3;
    };
    DagNode child2 = new ValidateNode(Config.newBuilder().build(), function);

    root.addChildNode(child1);
    // child1.addParentNode(root);
    child1.addChildNode(child2);
    // child2.addParentNode(child1);
    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);
    return new WorkflowDag(rootNodes);
  }

}
