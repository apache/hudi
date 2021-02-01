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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("checkstyle:LineLength")
public class SparkMergeOnReadRollbackActionExecutor<T extends HoodieRecordPayload> extends
    BaseMergeOnReadRollbackActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  public SparkMergeOnReadRollbackActionExecutor(HoodieEngineContext context,
                                                HoodieWriteConfig config,
                                                HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                                String instantTime,
                                                HoodieInstant commitInstant,
                                                boolean deleteInstants) {
    super(context, config, table, instantTime, commitInstant, deleteInstants);
  }

  public SparkMergeOnReadRollbackActionExecutor(HoodieEngineContext context,
                                                HoodieWriteConfig config,
                                                HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                                String instantTime,
                                                HoodieInstant commitInstant,
                                                boolean deleteInstants,
                                                boolean skipTimelinePublish,
                                                boolean useMarkerBasedStrategy) {
    super(context, config, table, instantTime, commitInstant, deleteInstants, skipTimelinePublish, useMarkerBasedStrategy);
  }

  @Override
  protected BaseRollbackActionExecutor.RollbackStrategy getRollbackStrategy() {
    if (useMarkerBasedStrategy) {
      return new SparkMarkerBasedRollbackStrategy(table, context, config, instantTime);
    } else {
      return this::executeRollbackUsingFileListing;
    }
  }

  @Override
  protected List<HoodieRollbackStat> executeRollbackUsingFileListing(HoodieInstant resolvedInstant) {
    List<ListingBasedRollbackRequest> rollbackRequests;
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    try {
      rollbackRequests = RollbackUtils.generateRollbackRequestsUsingFileListingMOR(resolvedInstant, table, context);
    } catch (IOException e) {
      throw new HoodieIOException("Error generating rollback requests by file listing.", e);
    }
    return new ListingBasedRollbackHelper(table.getMetaClient(), config).performRollback(context, resolvedInstant, rollbackRequests);
  }
}
