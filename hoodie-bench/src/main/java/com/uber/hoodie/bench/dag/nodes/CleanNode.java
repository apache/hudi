package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.bench.writer.DeltaWriter;

public class CleanNode extends DagNode<Boolean> {

  public CleanNode() {
  }

  public void execute(DeltaWriter deltaWriter) throws Exception {
    log.info("Executing clean node " + this.getName());
    deltaWriter.getWriteClient().clean();
  }

}
