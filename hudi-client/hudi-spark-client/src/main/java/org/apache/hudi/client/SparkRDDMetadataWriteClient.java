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

package org.apache.hudi.client;

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieSparkMergeOnReadMetadataTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Write client to assist with writing to metadata table.
 *
 * @param <T>
 */
public class SparkRDDMetadataWriteClient<T> extends SparkRDDWriteClient<T> {

  // tracks the instants for which upsertPrepped is invoked.
  private Option<String> firstInstantOpt = Option.empty();
  private int invocationCounts = 0;

  public SparkRDDMetadataWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }

  public SparkRDDMetadataWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig,
                                     Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, timelineService);
  }

  @Override
  public String createNewInstantTime() {
    return TimelineUtils.generateInstantTime(false, timeGenerator);
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param instantTime    Instant time of the commit
   * @return Collection of WriteStatus to inspect errors and counts
   */
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<List<HoodieFileGroupId>> hoodieFileGroupIdList) {
    HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table =
        initTable(WriteOperationType.UPSERT_PREPPED, Option.ofNullable(instantTime));
    table.validateUpsertSchema();
    boolean initialCall = firstInstantOpt.isEmpty();
    invocationCounts++;
    if (initialCall) {
      // we do not want to call prewrite more than once for the same instant, since we could be writing to metadata table more than once w/ streaming writes.
      preWrite(instantTime, WriteOperationType.UPSERT_PREPPED, table.getMetaClient());
      firstInstantOpt = Option.of(instantTime);
    } else {
      ValidationUtils.checkArgument(firstInstantOpt.get().equals(instantTime), "Upsert Prepped invoked for metadata table using same write client instance "
          + " for two different instant times " + firstInstantOpt.get() + " and " + instantTime);
    }
    ValidationUtils.checkArgument(invocationCounts <= 2, "Upsert Prepped invoked more then twice for the same instant time with metadata write client "
        + firstInstantOpt.get());
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = ((HoodieSparkMergeOnReadMetadataTable) table).upsertPrepped(context, instantTime, HoodieJavaRDD.of(preppedRecords),
        hoodieFileGroupIdList, initialCall);
    HoodieWriteMetadata<JavaRDD<WriteStatus>> resultRDD = result.clone(HoodieJavaRDD.getJavaRDD(result.getWriteStatuses()));
    return postWrite(resultRDD, instantTime, table);
  }
}
