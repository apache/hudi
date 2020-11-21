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

import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;

import java.util.List;

import static org.apache.hudi.integ.testsuite.dag.DagUtils.DEFAULT_DAG_NAME;
import static org.apache.hudi.integ.testsuite.dag.DagUtils.DEFAULT_DAG_ROUNDS;
import static org.apache.hudi.integ.testsuite.dag.DagUtils.DEFAULT_INTERMITTENT_DELAY_MINS;

/**
 * Workflow dag that encapsulates all execute nodes.
 */
public class WorkflowDag<O> {

  private String dagName;
  private int rounds;
  private int intermittentDelayMins;
  private List<DagNode<O>> nodeList;

  public WorkflowDag(List<DagNode<O>> nodeList) {
    this(DEFAULT_DAG_NAME, DEFAULT_DAG_ROUNDS, DEFAULT_INTERMITTENT_DELAY_MINS, nodeList);
  }

  public WorkflowDag(String dagName, int rounds, int intermittentDelayMins, List<DagNode<O>> nodeList) {
    this.dagName = dagName;
    this.rounds = rounds;
    this.intermittentDelayMins = intermittentDelayMins;
    this.nodeList = nodeList;
  }

  public String getDagName() {
    return dagName;
  }

  public int getRounds() {
    return rounds;
  }

  public int getIntermittentDelayMins() {
    return intermittentDelayMins;
  }

  public List<DagNode<O>> getNodeList() {
    return nodeList;
  }

}
