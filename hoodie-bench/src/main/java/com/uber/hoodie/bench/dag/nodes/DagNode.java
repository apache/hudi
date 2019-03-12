package com.uber.hoodie.bench.dag.nodes;

import static com.uber.hoodie.bench.configuration.DeltaConfig.Config.NODE_NAME;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Represents a Node in the DAG of operations for a workflow.
 */
public abstract class DagNode<O> implements Comparable<DagNode<O>> {

  protected static Logger log = LogManager.getLogger(DagNode.class);

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
    Object name = this.config.getOtherConfigs().get(NODE_NAME);
    if (name == null) {
      String randomName = UUID.randomUUID().toString();
      this.config.getOtherConfigs().put(NODE_NAME, randomName);
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
