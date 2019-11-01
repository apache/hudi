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

package org.apache.hudi.bench.dag.nodes;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.bench.configuration.DeltaConfig.Config;
import org.apache.hudi.bench.generator.DeltaGenerator;
import org.apache.hudi.bench.writer.DeltaWriter;
import org.apache.hudi.common.util.Option;
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
  protected JavaRDD<WriteStatus> ingest(DeltaWriter deltaWriter, Option<String> commitTime)
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
