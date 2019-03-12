package com.uber.hoodie.bench.dag;

import com.uber.hoodie.bench.dag.nodes.DagNode;
import java.util.List;

public class WorkflowDag<O> {

  private List<DagNode<O>> nodeList;

  public WorkflowDag(List<DagNode<O>> nodeList) {
    this.nodeList = nodeList;
  }

  public List<DagNode<O>> getNodeList() {
    return nodeList;
  }

  public String dagView() {
    return "";
  }

}
