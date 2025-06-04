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

import org.apache.hudi.callback.common.WriteStatusValidator;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieSparkMergeOnReadMetadataTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Write client to assist with writing to metadata table.
 *
 * @param <T>
 */
public class SparkRDDMetadataWriteClient<T> extends SparkRDDWriteClient<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRDDMetadataWriteClient.class);

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

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                        Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc,
                        Option<WriteStatusValidator> writeStatusHandlerCallbackOpt) {
    context.setJobStatus(this.getClass().getSimpleName(), "Committing stats: " + config.getTableName());
    // Triggering the dag for writes to metadata table.
    // When streaming writes are enabled, writes to metadata may not call this method as the caller tightly controls the dag de-referencing.
    // Even then, to initialize a new partition in Metadata table and for non incremental operations like insert_overwrite, etc, writes to metadata table
    // will invoke this commit method.
    List<WriteStatus> writeStatusesList = writeStatuses.map(writeStatus -> writeStatus.removeMetadataIndexStatsAndErrorRecordsTracking()).collect();
    // Compute stats for the writes and invoke callback
    AtomicLong totalRecords = new AtomicLong(0);
    AtomicLong totalErrorRecords = new AtomicLong(0);
    writeStatusesList.forEach(entry -> {
      totalRecords.getAndAdd(entry.getTotalRecords());
      totalErrorRecords.getAndAdd(entry.getTotalErrorRecords());
    });

    // reason why we are passing RDD<WriteStatus> to the writeStatusHandler callback: We can't afford to collect all write status to dirver if there are errors, since write status will hold
    // every error record. So, just incase if there are errors, caller might be interested to fetch error records. And so, we are passing the RDD<WriteStatus> as last argument to the write status
    // handler callback.
    boolean canProceed = writeStatusHandlerCallbackOpt.isEmpty() || writeStatusHandlerCallbackOpt.get().validate(totalRecords.get(), totalErrorRecords.get(),
        totalErrorRecords.get() > 0 ? Option.of(HoodieJavaRDD.of(writeStatuses.filter(status -> !status.isMetadataTable()).map(WriteStatus::removeMetadataStats))) : Option.empty());
    // only if callback returns true, lets proceed. If not, bail out.
    if (canProceed) {
      List<HoodieWriteStat> hoodieWriteStats = writeStatusesList.stream().map(writeStatus -> writeStatus.getStat()).collect(Collectors.toList());
      return commitStats(instantTime, hoodieWriteStats, extraMetadata, commitActionType,
          partitionToReplacedFileIds, extraPreCommitFunc, Option.of(createTable(config)));
    } else {
      LOG.error("Exiting early due to errors with write operation ");
      return false;
    }
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
