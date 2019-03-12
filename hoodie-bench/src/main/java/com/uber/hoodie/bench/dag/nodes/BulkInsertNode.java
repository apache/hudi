package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.writer.DeltaWriter;
import java.util.Optional;
import org.apache.spark.api.java.JavaRDD;

public class BulkInsertNode extends InsertNode {

  public BulkInsertNode(Config config) {
    super(config);
  }

  @Override
  protected JavaRDD<WriteStatus> ingest(DeltaWriter deltaWriter, Optional<String> commitTime)
      throws Exception {
    log.info("Execute bulk ingest node " + this.getName());
    return deltaWriter.bulkInsert(commitTime);
  }

}
