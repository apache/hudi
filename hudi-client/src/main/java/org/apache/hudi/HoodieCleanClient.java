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

package org.apache.hudi;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieTable;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;

public class HoodieCleanClient<T extends HoodieRecordPayload> extends AbstractHoodieClient {

  private static Logger logger = LogManager.getLogger(HoodieCleanClient.class);
  private final transient HoodieMetrics metrics;

  public HoodieCleanClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig, HoodieMetrics metrics) {
    this(jsc, clientConfig, metrics, Option.empty());
  }

  public HoodieCleanClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig, HoodieMetrics metrics,
      Option<EmbeddedTimelineService> timelineService) {
    super(jsc, clientConfig, timelineService);
    this.metrics = metrics;
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public void clean() throws HoodieIOException {
    String startCleanTime = HoodieActiveTimeline.createNewCommitTime();
    clean(startCleanTime);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   *
   * @param startCleanTime Cleaner Instant Timestamp
   * @throws HoodieIOException in case of any IOException
   */
  protected HoodieCleanMetadata clean(String startCleanTime) throws HoodieIOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    final HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);

    // If there are inflight(failed) or previously requested clean operation, first perform them
    table.getCleanTimeline().filterInflightsAndRequested().getInstants().forEach(hoodieInstant -> {
      logger.info("There were previously unfinished cleaner operations. Finishing Instant=" + hoodieInstant);
      runClean(table, hoodieInstant.getTimestamp());
    });

    Option<HoodieCleanerPlan> cleanerPlanOpt = scheduleClean(startCleanTime);

    if (cleanerPlanOpt.isPresent()) {
      HoodieCleanerPlan cleanerPlan = cleanerPlanOpt.get();
      if ((cleanerPlan.getFilesToBeDeletedPerPartition() != null)
          && !cleanerPlan.getFilesToBeDeletedPerPartition().isEmpty()) {
        final HoodieTable<T> hoodieTable = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);
        return runClean(hoodieTable, startCleanTime);
      }
    }
    return null;
  }

  /**
   * Creates a Cleaner plan if there are files to be cleaned and stores them in instant file.
   *
   * @param startCleanTime Cleaner Instant Time
   * @return Cleaner Plan if generated
   */
  @VisibleForTesting
  protected Option<HoodieCleanerPlan> scheduleClean(String startCleanTime) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.getHoodieTable(createMetaClient(true), config, jsc);

    HoodieCleanerPlan cleanerPlan = table.scheduleClean(jsc);

    if ((cleanerPlan.getFilesToBeDeletedPerPartition() != null)
        && !cleanerPlan.getFilesToBeDeletedPerPartition().isEmpty()) {

      HoodieInstant cleanInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.CLEAN_ACTION, startCleanTime);
      // Save to both aux and timeline folder
      try {
        table.getActiveTimeline().saveToCleanRequested(cleanInstant, AvroUtils.serializeCleanerPlan(cleanerPlan));
        logger.info("Requesting Cleaning with instant time " + cleanInstant);
      } catch (IOException e) {
        logger.error("Got exception when saving cleaner requested file", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return Option.of(cleanerPlan);
    }
    return Option.empty();
  }

  /**
   * Executes the Cleaner plan stored in the instant metadata.
   *
   * @param table Hoodie Table
   * @param cleanInstantTs Cleaner Instant Timestamp
   */
  @VisibleForTesting
  protected HoodieCleanMetadata runClean(HoodieTable<T> table, String cleanInstantTs) {
    HoodieInstant cleanInstant =
        table.getCleanTimeline().getInstants().filter(x -> x.getTimestamp().equals(cleanInstantTs)).findFirst().get();

    Preconditions.checkArgument(
        cleanInstant.getState().equals(State.REQUESTED) || cleanInstant.getState().equals(State.INFLIGHT));

    try {
      logger.info("Cleaner started");
      final Timer.Context context = metrics.getCleanCtx();

      if (!cleanInstant.isInflight()) {
        // Mark as inflight first
        cleanInstant = table.getActiveTimeline().transitionCleanRequestedToInflight(cleanInstant);
      }

      List<HoodieCleanStat> cleanStats = table.clean(jsc, cleanInstant);

      if (cleanStats.isEmpty()) {
        return HoodieCleanMetadata.newBuilder().build();
      }

      // Emit metrics (duration, numFilesDeleted) if needed
      Option<Long> durationInMs = Option.empty();
      if (context != null) {
        durationInMs = Option.of(metrics.getDurationInMs(context.stop()));
        logger.info("cleanerElaspsedTime (Minutes): " + durationInMs.get() / (1000 * 60));
      }

      HoodieTableMetaClient metaClient = createMetaClient(true);
      // Create the metadata and save it
      HoodieCleanMetadata metadata =
          CleanerUtils.convertCleanMetadata(metaClient, cleanInstant.getTimestamp(), durationInMs, cleanStats);
      logger.info("Cleaned " + metadata.getTotalFilesDeleted() + " files. Earliest Retained :" + metadata.getEarliestCommitToRetain());
      metrics.updateCleanMetrics(durationInMs.orElseGet(() -> -1L), metadata.getTotalFilesDeleted());

      table.getActiveTimeline().transitionCleanInflightToComplete(
          new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, cleanInstant.getTimestamp()),
          AvroUtils.serializeCleanMetadata(metadata));
      logger.info("Marked clean started on " + cleanInstant.getTimestamp() + " as complete");
      return metadata;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    }
  }
}
