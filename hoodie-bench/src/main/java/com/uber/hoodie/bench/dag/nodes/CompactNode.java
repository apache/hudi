package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.bench.writer.DeltaWriter;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import java.util.Optional;
import org.apache.spark.api.java.JavaRDD;

public class CompactNode extends DagNode<JavaRDD<WriteStatus>> {

  public CompactNode() {
  }

  public JavaRDD<WriteStatus> execute(DeltaWriter deltaWriter) throws Exception {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(deltaWriter.getConfiguration(),
        deltaWriter.getCfg().targetBasePath);
    Optional<HoodieInstant> lastInstant = metaClient.getActiveTimeline()
        .getCommitsAndCompactionTimeline().filterPendingCompactionTimeline().lastInstant();
    if (lastInstant.isPresent()) {
      log.info("Compacting instant => " + lastInstant.get());
      this.result = deltaWriter.compact(Optional.of(lastInstant.get().getTimestamp()));
    }
    return this.result;
  }

}
