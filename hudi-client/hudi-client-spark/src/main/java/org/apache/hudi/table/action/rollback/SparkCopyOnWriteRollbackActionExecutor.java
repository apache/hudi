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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseHoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkCopyOnWriteRollbackActionExecutor<T extends HoodieRecordPayload> extends BaseCopyOnWriteRollbackActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  private static final Logger LOG = LogManager.getLogger(SparkCopyOnWriteRollbackActionExecutor.class);

  public SparkCopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                                HoodieWriteConfig config,
                                                BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> table,
                                                String instantTime,
                                                HoodieInstant commitInstant,
                                                boolean deleteInstants) {
    super(context, config, table, instantTime, commitInstant, deleteInstants);
  }

  public SparkCopyOnWriteRollbackActionExecutor(HoodieEngineContext context,
                                                HoodieWriteConfig config,
                                                BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> table,
                                                String instantTime,
                                                HoodieInstant commitInstant,
                                                boolean deleteInstants,
                                                boolean skipTimelinePublish) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish);
  }

  @Override
  protected List<HoodieRollbackStat> executeRollback() throws IOException {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    long startTime = System.currentTimeMillis();
    List<HoodieRollbackStat> stats = new ArrayList<>();
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieInstant resolvedInstant = instantToRollback;

    if (instantToRollback.isCompleted()) {
      LOG.info("Unpublishing instant " + instantToRollback);
      resolvedInstant = activeTimeline.revertToInflight(instantToRollback);
      // reload meta-client to reflect latest timeline status
      table.getMetaClient().reloadActiveTimeline();
    }

    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!resolvedInstant.isRequested()) {
      // delete all the data files for this commit
      LOG.info("Clean out all parquet files generated for commit: " + resolvedInstant);
      List<RollbackRequest> rollbackRequests = generateRollbackRequests(resolvedInstant);

      //TODO: We need to persist this as rollback workload and use it in case of partial failures
      stats = new SparkRollbackHelper(table.getMetaClient(), config).performRollback(context, resolvedInstant, rollbackRequests);
    }
    // Delete Inflight instant if enabled
    deleteInflightAndRequestedInstant(deleteInstants, activeTimeline, resolvedInstant);
    LOG.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));
    return stats;
  }

}
