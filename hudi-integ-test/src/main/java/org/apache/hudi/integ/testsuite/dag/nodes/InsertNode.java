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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteWriter;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.dag.ExecutionContext;
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

/**
 * An insert node in the DAG of operations for a workflow.
 */
@Slf4j
public class InsertNode extends DagNode<JavaRDD<WriteStatus>> {

  protected JavaRDD<DeltaWriteStats> deltaWriteStatsRDD;

  public InsertNode(Config config) {
    this.config = config;
  }

  @Override
  public void execute(ExecutionContext executionContext, int curItrCount) throws Exception {
    // if the insert node has schema override set, reinitialize the table with new schema.
    if (this.config.getReinitContext()) {
      log.info(String.format("Reinitializing table with %s", this.config.getOtherConfigs().toString()));
      executionContext.getWriterContext().reinitContext(this.config.getOtherConfigs());
    }
    generate(executionContext.getDeltaGenerator());
    log.info("Configs : {}", this.config);
    if (!config.isDisableIngest()) {
      log.info("Inserting input data {}", this.getName());
      Option<String> commitTime = executionContext.getHoodieTestSuiteWriter().startCommit();
      JavaRDD<WriteStatus> writeStatus = ingest(executionContext.getHoodieTestSuiteWriter(), commitTime);
      executionContext.getHoodieTestSuiteWriter().commit(writeStatus, this.deltaWriteStatsRDD, commitTime);
      this.result = writeStatus;
    }
  }

  protected void generate(DeltaGenerator deltaGenerator) throws Exception {
    if (!config.isDisableGenerate()) {
      log.info("Generating input data for node {}", this.getName());
      this.deltaWriteStatsRDD = deltaGenerator.writeRecords(deltaGenerator.generateInserts(config)).getValue();
      this.deltaWriteStatsRDD.cache();
      this.deltaWriteStatsRDD.count();
    }
  }

  protected JavaRDD<WriteStatus> ingest(HoodieTestSuiteWriter hoodieTestSuiteWriter,
      Option<String> commitTime) throws Exception {
    return hoodieTestSuiteWriter.insert(commitTime);
  }

}
