package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.util.Optional;

public class ScheduleCompactNode extends DagNode<Optional<String>> {

  public ScheduleCompactNode() {
  }

  public Optional<String> execute(DeltaWriter deltaWriter) throws Exception {
    log.info("Executing schedule compact node " + this.getName());
    // Can only be done with an instantiation of a new WriteClient hence cannot be done during DeltaStreamer
    // testing for now
    // Find the last commit and extra the extra metadata to be passed to the schedule compaction. This is
    // done to ensure the CHECKPOINT is correctly passed from commit to commit
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(deltaWriter.getConfiguration(),
        deltaWriter.getCfg().targetBasePath);
    Optional<HoodieInstant> lastInstant = metaClient.getActiveTimeline().getCommitsTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      HoodieCommitMetadata metadata = com.uber.hoodie.common.model.HoodieCommitMetadata.fromBytes(metaClient
          .getActiveTimeline().getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
      Optional<String> scheduledInstant = deltaWriter.getWriteClient().scheduleCompaction(Optional.of(metadata
          .getExtraMetadata()));
      log.info("Scheduling compaction instant => " + scheduledInstant.get());
      return scheduledInstant;
    }
    return Optional.empty();
  }

}
