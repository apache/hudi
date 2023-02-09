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

import org.apache.hudi.integ.testsuite.dag.ExecutionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delay Node to add delays between each group of test runs.
 */
public class DelayNode extends DagNode<Boolean> {

  private static Logger log = LoggerFactory.getLogger(ValidateDatasetNode.class);
  private int delayMins;

  public DelayNode(int delayMins) {
    this.delayMins = delayMins;
  }

  @Override
  public void execute(ExecutionContext context, int curItrCount) throws Exception {
    log.warn("Waiting for " + delayMins + " mins before going for next test run");
    Thread.sleep(delayMins * 60 * 1000);
  }
}
