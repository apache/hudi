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

import java.util.List;
import java.util.function.Function;

/**
 * A validate node helps to validate its parent nodes with given function.
 */
public class ValidateNode<R> extends DagNode {

  protected Function<List<DagNode>, R> function;

  public ValidateNode(Config config, Function<List<DagNode>, R> function) {
    this.function = function;
    this.config = config;
  }

  /**
   * Method to start the validate operation. Exceptions will be thrown if its parent nodes exist and WAIT_FOR_PARENTS
   * was set to true or default, but the parent nodes have not completed yet.
   *
   * @param executionContext Context to execute this node
   * @param curItrCount current iteration count.
   */
  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) {
    if (this.getParentNodes().size() > 0 && (Boolean) this.config.getOtherConfigs().getOrDefault("WAIT_FOR_PARENTS",
        true)) {
      for (DagNode node : (List<DagNode>) this.getParentNodes()) {
        if (!node.isCompleted()) {
          throw new RuntimeException("cannot validate before parent nodes are complete");
        }
      }
    }
    this.result = this.function.apply((List<DagNode>) this.getParentNodes());
  }

}
