package com.uber.hoodie.bench.dag.nodes;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.generator.DeltaGenerator;
import com.uber.hoodie.bench.writer.DeltaWriter;
import java.util.Optional;
import org.apache.spark.api.java.JavaRDD;

public class InsertNode extends DagNode<JavaRDD<WriteStatus>> {

  public InsertNode(Config config) {
    this.config = config;
  }

  public JavaRDD<WriteStatus> execute(DeltaWriter deltaWriter, DeltaGenerator deltaGenerator) throws Exception {
    generate(deltaGenerator);
    log.info("Configs => " + this.config);
    if (!config.isDisableIngest()) {
      log.info(String.format("----------------- inserting input data %s ------------------", this.getName()));
      Optional<String> commitTime = deltaWriter.startCommit();
      JavaRDD<WriteStatus> writeStatus = ingest(deltaWriter, commitTime);
      deltaWriter.commit(writeStatus, commitTime);
      this.result = writeStatus;
    }
    validate();
    return this.result;
  }

  protected void generate(DeltaGenerator deltaGenerator) throws Exception {
    if (!config.isDisableGenerate()) {
      log.info(String.format("----------------- generating input data for node %s ------------------", this.getName()));
      deltaGenerator.writeRecords(deltaGenerator.generateInserts(config)).count();
    }
  }

  protected JavaRDD<WriteStatus> ingest(DeltaWriter deltaWriter,
      Optional<String> commitTime) throws Exception {
    return deltaWriter.insert(commitTime);
  }

  protected boolean validate() {
    return this.result.count() == config.getNumRecordsInsert();
  }

}
