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
import org.apache.hudi.testsuite.dag.nodes.HiveSyncNode;
import org.apache.hudi.testsuite.dag.nodes.InsertNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HiveSyncDagGenerator implements WorkflowDagGenerator {

  @Override
  public WorkflowDag build() {
    DagNode root = new InsertNode(Config.newBuilder()
        .withNumRecordsToInsert(100)
        .withNumInsertPartitions(1)
        .withNumTimesToRepeat(1)
        .withRecordSize(1000).build());

    DagNode child1 = new HiveSyncNode(Config.newBuilder().withHiveLocal(true).build());

    root.addChildNode(child1);

    DagNode child2 = new HiveQueryNode(Config.newBuilder().withHiveLocal(true).withHiveQueryAndResults(Arrays
        .asList(Pair.of("select " + "count(*) from testdb1.hive_trips group " + "by rider having count(*) < 1", 0)))
        .build());
    child1.addChildNode(child2);

    List<DagNode> rootNodes = new ArrayList<>();
    rootNodes.add(root);
    return new WorkflowDag(rootNodes);
  }

}
