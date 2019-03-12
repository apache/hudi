package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import java.util.List;
import java.util.function.Function;

public class ValidateNode<R> extends DagNode {

  protected Function<List<DagNode>, R> function;

  public ValidateNode(Config config, Function<List<DagNode>, R> function) {
    this.function = function;
    this.config = config;
  }

  public R execute() {
    if (this.getParentNodes().size() > 0 && (Boolean) this.config.getOtherConfigs().getOrDefault("WAIT_FOR_PARENTS",
        true)) {
      for (DagNode node : (List<DagNode>) this.getParentNodes()) {
        if (!node.isCompleted()) {
          throw new RuntimeException("cannot validate before parent nodes are complete");
        }
      }
    }
    return this.function.apply((List<DagNode>) this.getParentNodes());
  }

}
