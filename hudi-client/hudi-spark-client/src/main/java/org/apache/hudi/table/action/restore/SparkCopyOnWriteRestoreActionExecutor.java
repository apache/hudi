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

package org.apache.hudi.table.action.restore;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.SparkCopyOnWriteRollbackActionExecutor;

import org.apache.spark.api.java.JavaRDD;

@SuppressWarnings("checkstyle:LineLength")
public class SparkCopyOnWriteRestoreActionExecutor<T extends HoodieRecordPayload> extends
    BaseRestoreActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public SparkCopyOnWriteRestoreActionExecutor(HoodieSparkEngineContext context,
                                               HoodieWriteConfig config,
                                               HoodieTable table,
                                               String instantTime,
                                               String restoreInstantTime) {
    super(context, config, table, instantTime, restoreInstantTime);
  }

  @Override
  protected HoodieRollbackMetadata rollbackInstant(HoodieInstant instantToRollback) {
    table.getMetaClient().reloadActiveTimeline();
    SparkCopyOnWriteRollbackActionExecutor rollbackActionExecutor = new SparkCopyOnWriteRollbackActionExecutor(
        (HoodieSparkEngineContext) context,
        config,
        table,
        HoodieActiveTimeline.createNewInstantTime(),
        instantToRollback,
        true,
        true,
        false);
    if (!instantToRollback.getAction().equals(HoodieTimeline.COMMIT_ACTION)
        && !instantToRollback.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      throw new HoodieRollbackException("Unsupported action in rollback instant:" + instantToRollback);
    }
    return rollbackActionExecutor.execute();
  }
}
