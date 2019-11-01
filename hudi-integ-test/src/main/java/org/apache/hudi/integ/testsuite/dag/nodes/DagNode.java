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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a Node in the DAG of operations for a workflow.
 */
public abstract class DagNode<O> implements Comparable<DagNode<O>> {

  protected static Logger log = LoggerFactory.getLogger(DagNode.class);

  protected List<DagNode<O>> childNodes;
  protected List<DagNode<O>> parentNodes;
  protected O result;
  protected Config config;
  private boolean isCompleted;

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

  public O getResult() {
    return result;
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

  public void setParentNodes(List<DagNode<O>> parentNodes) {
    this.parentNodes = parentNodes;
  }

  public abstract void execute(ExecutionContext context) throws Exception;

  public boolean isCompleted() {
    return isCompleted;
  }

  public void setCompleted(boolean completed) {
    isCompleted = completed;
  }

  public Config getConfig() {
    return config;
  }

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
