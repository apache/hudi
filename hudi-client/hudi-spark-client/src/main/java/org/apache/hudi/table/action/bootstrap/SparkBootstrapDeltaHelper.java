/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.bootstrap;

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;
import org.apache.hudi.table.action.deltacommit.SparkDeltaCommitHelper;

import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

public class SparkBootstrapDeltaHelper<T extends HoodieRecordPayload<T>>
    extends SparkBootstrapHelper<T> {

  private SparkBootstrapDeltaHelper() {
  }

  private static class BootstrapDeltaHelperHolder {
    private static final SparkBootstrapDeltaHelper SPARK_BOOTSTRAP_DELTA_HELPER = new SparkBootstrapDeltaHelper();
  }

  public static SparkBootstrapDeltaHelper newInstance() {
    return SparkBootstrapDeltaHelper.BootstrapDeltaHelperHolder.SPARK_BOOTSTRAP_DELTA_HELPER;
  }
  
  @Override
  protected BulkInsertCommitActionExecutor<T> getBulkInsertActionExecutor(
      HoodieEngineContext context, HoodieTable table, HoodieWriteConfig config,
      Option<Map<String, String>> extraMetadata, String bootstrapSchema,
      JavaRDD<HoodieRecord<T>> inputRecordsRDD) {
    HoodieWriteConfig bulkInsertConfig = new HoodieWriteConfig.Builder()
        .withProps(config.getProps()).withSchema(bootstrapSchema).build();
    return new BulkInsertCommitActionExecutor<T>(
        context, bulkInsertConfig,
        table, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
        SparkHoodieRDDData.of(inputRecordsRDD), Option.empty(), extraMetadata,
        new SparkDeltaCommitHelper<>(context, bulkInsertConfig, table,
            HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, WriteOperationType.BULK_INSERT),
        SparkBulkInsertHelper.newInstance());
  }
}
