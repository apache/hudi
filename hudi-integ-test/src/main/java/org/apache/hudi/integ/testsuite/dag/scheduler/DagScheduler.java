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

package org.apache.hudi.integ.testsuite.dag.scheduler;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.dag.WorkflowDag;
import org.apache.hudi.integ.testsuite.dag.WriterContext;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.metrics.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.CONFIG_NAME;

/**
 * The Dag scheduler schedules the workflow DAGs. It will convert DAG to node set and execute the nodes according to the relations between nodes.
 */
public class DagScheduler {

  private static Logger log = LoggerFactory.getLogger(DagScheduler.class);
  private WorkflowDag workflowDag;
  private ExecutionContext executionContext;

  public DagScheduler(WorkflowDag workflowDag, WriterContext writerContext) {
    this.workflowDag = workflowDag;
    this.executionContext = new ExecutionContext(null, writerContext);
  }

  /**
   * Method to start executing workflow DAGs.
   *
   * @throws Exception Thrown if schedule failed.
   */
  public void schedule() throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(2);
    try {
      execute(service, workflowDag.getNodeList());
      service.shutdown();
    } finally {
      if (!service.isShutdown()) {
        log.info("Forcing shutdown of executor service, this might kill running tasks");
        service.shutdownNow();
      }
    }
  }

  /**
   * Method to start executing the nodes in workflow DAGs.
   *
   * @param service ExecutorService
   * @param nodes Nodes to be executed
   * @throws Exception will be thrown if ant error occurred
   */
  private void execute(ExecutorService service, List<DagNode> nodes) throws Exception {
    // Nodes at the same level are executed in parallel
    Queue<DagNode> queue = new PriorityQueue<>(nodes);
    log.info("Running workloads");
    do {
      List<Future> futures = new ArrayList<>();
      Set<DagNode> childNodes = new HashSet<>();
      while (queue.size() > 0) {
        DagNode nodeToExecute = queue.poll();
        log.info("Node to execute in dag scheduler " + nodeToExecute.getConfig().toString());
        futures.add(service.submit(() -> executeNode(nodeToExecute)));
        if (nodeToExecute.getChildNodes().size() > 0) {
          childNodes.addAll(nodeToExecute.getChildNodes());
        }
      }
      queue.addAll(childNodes);
      childNodes.clear();
      for (Future future : futures) {
        future.get(1, TimeUnit.HOURS);
      }

      // After each level, report and flush the metrics
      Metrics.flush();
    } while (queue.size() > 0);
    log.info("Finished workloads");
  }

  /**
   * Execute the given node.
   *
   * @param node The node to be executed
   */
  private void executeNode(DagNode node) {
    if (node.isCompleted()) {
      throw new RuntimeException("DagNode already completed! Cannot re-execute");
    }
    try {
      int repeatCount = node.getConfig().getRepeatCount();
      while (repeatCount > 0) {
        log.warn("executing node: \"" + node.getConfig().getOtherConfigs().get(CONFIG_NAME) + "\" of type: " + node.getClass() + " :: " + node.getConfig().toString());
        node.execute(executionContext);
        log.info("Finished executing {}", node.getName());
        repeatCount--;
      }
      node.setCompleted(true);
    } catch (Exception e) {
      log.error("Exception executing node", e);
      throw new HoodieException(e);
    }
  }
}
