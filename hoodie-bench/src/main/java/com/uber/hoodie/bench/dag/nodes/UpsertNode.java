package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.generator.DeltaGenerator;
import com.uber.hoodie.bench.writer.DeltaWriter;
import java.util.Optional;
import org.apache.spark.api.java.JavaRDD;

public class UpsertNode extends InsertNode {

  public UpsertNode(Config config) {
    super(config);
  }

  @Override
  protected void generate(DeltaGenerator deltaGenerator) throws Exception {
    if (!config.isDisableGenerate()) {
      log.info(String.format("----------------- generating input data %s ------------------", this.getName()));
      deltaGenerator.writeRecords(deltaGenerator.generateUpdates(config)).count();
    }
  }

  @Override
  protected JavaRDD<WriteStatus> ingest(DeltaWriter deltaWriter, Optional<String> commitTime)
      throws Exception {
    if (!config.isDisableIngest()) {
      log.info(String.format("----------------- upserting input data %s ------------------", this.getName()));
      this.result = deltaWriter.upsert(commitTime);
    }
    return this.result;
  }

  @Override
  protected boolean validate() {
    return this.result.count() == this.config.getNumRecordsInsert() + this.config.getNumRecordsUpsert();
  }
}
