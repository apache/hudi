package com.uber.hoodie.bench.dag.scheduler;

import com.uber.hoodie.bench.dag.WorkflowDag;
import com.uber.hoodie.bench.dag.nodes.BulkInsertNode;
import com.uber.hoodie.bench.dag.nodes.CleanNode;
import com.uber.hoodie.bench.dag.nodes.CompactNode;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.HiveQueryNode;
import com.uber.hoodie.bench.dag.nodes.HiveSyncNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.bench.dag.nodes.RollbackNode;
import com.uber.hoodie.bench.dag.nodes.ScheduleCompactNode;
import com.uber.hoodie.bench.dag.nodes.UpsertNode;
import com.uber.hoodie.bench.dag.nodes.ValidateNode;
import com.uber.hoodie.bench.generator.DeltaGenerator;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.exception.HoodieException;
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DagScheduler {

  private static Logger log = LogManager.getLogger(DagScheduler.class);
  private WorkflowDag workflowDag;
  private DeltaGenerator deltaGenerator;
  private DeltaWriter deltaWriter;

  public DagScheduler(WorkflowDag workflowDag, DeltaWriter deltaWriter, DeltaGenerator deltaGenerator) {
    this.workflowDag = workflowDag;
    this.deltaWriter = deltaWriter;
    this.deltaGenerator = deltaGenerator;
  }

  public void schedule() throws Exception {
    ExecutorService service = Executors.newFixedThreadPool(1);
    execute(service, workflowDag.getNodeList());
  }

  private void execute(ExecutorService service, List<DagNode> nodes) throws Exception {
    // Nodes at the same level are executed in parallel
      Queue<DagNode> queue = new PriorityQueue<>(nodes);
    log.info("----------- Running workloads ----------");
    do {
      List<Future> futures = new ArrayList<>();
      Set<DagNode> childNodes = new HashSet<>();
      while (queue.size() > 0) {
        DagNode nodeToExecute = queue.poll();
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
    } while (queue.size() > 0);
    log.info("----------- Finished workloads ----------");
  }

  private void executeNode(DagNode node) {
    if (node.isCompleted()) {
      throw new RuntimeException("DagNode already completed! Cannot re-execute");
    }
    try {
      if (node instanceof InsertNode) {
        ((InsertNode) node).execute(deltaWriter, deltaGenerator);
      } else if (node instanceof BulkInsertNode) {
        ((InsertNode) node).execute(deltaWriter, deltaGenerator);
      } else if (node instanceof UpsertNode) {
        ((InsertNode) node).execute(deltaWriter, deltaGenerator);
      } else if (node instanceof CompactNode) {
        ((CompactNode) node).execute(deltaWriter);
      } else if (node instanceof ScheduleCompactNode) {
        ((ScheduleCompactNode) node).execute(deltaWriter);
      } else if (node instanceof CleanNode) {
        ((CleanNode) node).execute(deltaWriter);
      } else if (node instanceof RollbackNode) {
        ((RollbackNode) node).execute(deltaWriter);
      } else if (node instanceof ValidateNode) {
        ((ValidateNode) node).execute();
      } else if (node instanceof HiveSyncNode) {
        ((HiveSyncNode) node).execute(deltaWriter);
      } else if (node instanceof HiveQueryNode) {
        ((HiveQueryNode) node).execute(deltaWriter);
      }
      node.setCompleted(true);
      log.info("Finished executing => " + node.getName());
    } catch (Exception e) {
      log.error("Exception executing node");
      throw new HoodieException(e);
    }
  }
}
