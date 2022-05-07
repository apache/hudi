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
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;
import org.apache.spark.api.java.JavaRDD;

/**
 * Represents an upsert node in the DAG of operations for a workflow.
 */
public class UpsertNode extends InsertNode {

  public UpsertNode(Config config) {
    super(config);
  }

  @Override
  protected void generate(DeltaGenerator deltaGenerator) throws Exception {
    if (!config.isDisableGenerate()) {
      log.info("Generating input data {}", this.getName());
      deltaGenerator.writeRecords(deltaGenerator.generateUpdates(config)).getValue().count();
    }
  }

  @Override
  protected JavaRDD<WriteStatus> ingest(HoodieTestSuiteWriter hoodieTestSuiteWriter, Option<String> commitTime)
      throws Exception {
    if (!config.isDisableIngest()) {
      log.info("Upserting input data {}", this.getName());
      this.result = hoodieTestSuiteWriter.upsert(commitTime);
    }
    return this.result;
  }

}
