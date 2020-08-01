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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.InsertNode;
import org.apache.hudi.integ.testsuite.dag.nodes.UpsertNode;
import org.apache.hudi.integ.testsuite.dag.nodes.ValidateNode;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.dag.nodes.InsertNode;
import org.apache.hudi.integ.testsuite.dag.nodes.UpsertNode;
import org.apache.hudi.integ.testsuite.dag.nodes.ValidateNode;
import org.apache.spark.api.java.JavaRDD;

public class ComplexDagGenerator implements WorkflowDagGenerator {

  @Override
  public WorkflowDag build() {
    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(1000)
        .withNumInsertPartitions(3)
        .withRecordSize(1000).build());

    DagNode child1 = new UpsertNode(Config.newBuilder()
        .withNumRecordsToUpdate(999)
        .withNumRecordsToInsert(1000)
        .withNumUpsertFiles(1)
        .withNumUpsertPartitions(1)
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
      return b1 & b2 & b3;
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
