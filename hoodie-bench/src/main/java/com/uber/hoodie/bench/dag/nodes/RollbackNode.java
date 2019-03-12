package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.util.Optional;

public class RollbackNode extends DagNode<Optional<HoodieInstant>> {

  public RollbackNode(Config config) {
    this.config = config;
  }

  public Optional<HoodieInstant> execute(DeltaWriter deltaWriter) throws Exception {
    log.info("Executing rollback node " + this.getName());
    // Can only be done with an instantiation of a new WriteClient hence cannot be done during DeltaStreamer
    // testing for now
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(deltaWriter.getConfiguration(),
        deltaWriter.getCfg().targetBasePath);
    Optional<HoodieInstant> lastInstant = metaClient.getActiveTimeline().getCommitsTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      log.info("Rolling back last instant => " + lastInstant.get());
      deltaWriter.getWriteClient().rollback(lastInstant.get().getTimestamp());
      return lastInstant;
    }
    return Optional.empty();
  }

}
