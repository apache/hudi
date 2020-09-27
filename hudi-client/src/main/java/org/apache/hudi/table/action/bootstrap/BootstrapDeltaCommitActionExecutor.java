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

package org.apache.hudi.table.action.bootstrap;

import java.util.Map;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.CommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.BulkInsertDeltaCommitActionExecutor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BootstrapDeltaCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BootstrapCommitActionExecutor<T> {

  public BootstrapDeltaCommitActionExecutor(JavaSparkContext jsc,
      HoodieWriteConfig config, HoodieTable<?> table,
      Option<Map<String, String>> extraMetadata) {
    super(jsc, config, table, extraMetadata);
  }

  protected CommitActionExecutor<T> getBulkInsertActionExecutor(JavaRDD<HoodieRecord> inputRecordsRDD) {
    return new BulkInsertDeltaCommitActionExecutor(jsc, new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withSchema(bootstrapSchema).build(), table, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
        inputRecordsRDD, extraMetadata);
  }
}
