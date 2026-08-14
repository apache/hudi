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

package org.apache.hudi.integ.testsuite.dag.nodes;

import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Base abstraction of an compute node in the DAG of operations for a workflow.
 */
@Getter
public abstract class DagNode<O> implements Comparable<DagNode<O>> {

  protected List<DagNode<O>> childNodes;
  @Setter
  protected List<DagNode<O>> parentNodes;
  protected O result;
  protected Config config;
  @Setter
  private boolean completed;

  public DagNode clone() {
    List<DagNode<O>> tempChildNodes = new ArrayList<>();
    for (DagNode dagNode: childNodes) {
      tempChildNodes.add(dagNode.clone());
    }
    this.childNodes = tempChildNodes;
    this.result = null;
    this.completed = false;
    return this;
  }

  public DagNode<O> addChildNode(DagNode childNode) {
    childNode.getParentNodes().add(this);
    getChildNodes().add(childNode);
    return this;
  }

  public DagNode<O> addParentNode(DagNode parentNode) {
    if (!this.getParentNodes().contains(parentNode)) {
      this.getParentNodes().add(parentNode);
    }
    return this;
  }

  public List<DagNode<O>> getChildNodes() {
    if (childNodes == null) {
      childNodes = new LinkedList<>();
    }
    return childNodes;
  }

  public List<DagNode<O>> getParentNodes() {
    if (parentNodes == null) {
      this.parentNodes = new ArrayList<>();
    }
    return this.parentNodes;
  }

  /**
   * Execute the {@link DagNode}.
   *
   * @param context The context needed for an execution of a node.
   * @param curItrCount iteration count for executing the node.
   * @throws Exception Thrown if the execution failed.
   */
  public abstract void execute(ExecutionContext context, int curItrCount) throws Exception;

  public String getName() {
    Object name = this.config.getOtherConfigs().get(Config.NODE_NAME);
    if (name == null) {
      String randomName = UUID.randomUUID().toString();
      this.config.getOtherConfigs().put(Config.NODE_NAME, randomName);
      return randomName;
    }
    return name.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DagNode<?> dagNode = (DagNode<?>) o;
    return getName() == dagNode.getName();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName());
  }

  @Override
  public int compareTo(DagNode<O> thatNode) {
    return this.hashCode() - thatNode.hashCode();
  }
}
