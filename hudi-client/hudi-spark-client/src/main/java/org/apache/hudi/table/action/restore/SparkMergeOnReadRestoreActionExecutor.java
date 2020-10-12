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
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.SparkMergeOnReadRollbackActionExecutor;

import org.apache.spark.api.java.JavaRDD;

@SuppressWarnings("checkstyle:LineLength")
public class SparkMergeOnReadRestoreActionExecutor<T extends HoodieRecordPayload> extends
    BaseRestoreActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  public SparkMergeOnReadRestoreActionExecutor(HoodieSparkEngineContext context,
                                               HoodieWriteConfig config,
                                               HoodieTable table,
                                               String instantTime,
                                               String restoreInstantTime) {
    super(context, config, table, instantTime, restoreInstantTime);
  }

  @Override
  protected HoodieRollbackMetadata rollbackInstant(HoodieInstant instantToRollback) {
    table.getMetaClient().reloadActiveTimeline();
    SparkMergeOnReadRollbackActionExecutor rollbackActionExecutor = new SparkMergeOnReadRollbackActionExecutor(
        context,
        config,
        table,
        HoodieActiveTimeline.createNewInstantTime(),
        instantToRollback,
        true,
        true,
        false);

    switch (instantToRollback.getAction()) {
      case HoodieTimeline.COMMIT_ACTION:
      case HoodieTimeline.DELTA_COMMIT_ACTION:
      case HoodieTimeline.COMPACTION_ACTION:
      case HoodieTimeline.REPLACE_COMMIT_ACTION:
        // TODO : Get file status and create a rollback stat and file
        // TODO : Delete the .aux files along with the instant file, okay for now since the archival process will
        // delete these files when it does not see a corresponding instant file under .hoodie
        return rollbackActionExecutor.execute();
      default:
        throw new IllegalArgumentException("invalid action name " + instantToRollback.getAction());
    }
  }
}
