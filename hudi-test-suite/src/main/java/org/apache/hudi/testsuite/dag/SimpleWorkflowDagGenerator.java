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

package org.apache.hudi.testsuite.dag;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.testsuite.dag.nodes.DagNode;
import org.apache.hudi.testsuite.dag.nodes.HiveQueryNode;
import org.apache.hudi.testsuite.dag.nodes.InsertNode;
import org.apache.hudi.testsuite.dag.nodes.UpsertNode;

import java.util.ArrayList;
import java.util.List;

/**
 * An example of how to generate a workflow dag programmatically. This is also used as the default workflow dag if
 * none is provided.
 */
public class SimpleWorkflowDagGenerator implements WorkflowDagGenerator {

  @Override
  public WorkflowDag build() {

    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(100)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    DagNode child1 = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(100)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(2)
        .withRecordSize(1000).build());

    root.addChildNode(child1);

    DagNode child1OfChild1 = new UpsertNode(Config.newBuilder()
        .withNumRecordsToUpdate(100)
        .withNumUpsertPartitions(2)
        .withNumTimesToRepeat(1)
        .withRecordSize(1000).build());

    // Tests running 2 nodes in parallel
    child1.addChildNode(child1OfChild1);

    List<Pair<String, Integer>> queryAndResult = new ArrayList<>();
    queryAndResult.add(Pair.of("select " + "count(*) from testdb1.hive_trips group "
        + "by rider having count(*) < 1", 0));
    DagNode child2OfChild1 = new HiveQueryNode(Config.newBuilder()
        .withHiveQueryAndResults(queryAndResult).withHiveLocal(true).build());
    child1.addChildNode(child2OfChild1);

    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);

    return new WorkflowDag(rootNodes);
  }

}
