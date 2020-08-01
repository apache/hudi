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

import java.util.List;
import java.util.function.Function;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

public class ValidateNode<R> extends DagNode {

  protected Function<List<DagNode>, R> function;

  public ValidateNode(Config config, Function<List<DagNode>, R> function) {
    this.function = function;
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext) {
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
