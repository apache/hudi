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

import com.codahale.metrics.Timer;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.embedded.AbstractEmbeddedTimelineService;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.BaseHoodieTable;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * Base Hoodie write client helps to build tables on HDFS [insert()] and then perform efficient mutations on an HDFS
 * table [upsert()]
 * <p>
 * Note that, at any given time, there can only be one job performing these operations on a Hoodie table.
 *
 * @param <T> Type of payload in {@link org.apache.hudi.common.model.HoodieRecord}
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 * @param <P> Type of record position [Key, Option[partitionPath, fileID]] in hoodie table
 */
public abstract class BaseHoodieWriteClient<T extends HoodieRecordPayload, I, K, O, P> extends BaseHoodieClient {

  protected final transient HoodieMetrics metrics;
  protected final transient HoodieIndex<T, I, K, O, P> index;
  protected final transient BaseHoodieTable<T, I, K, O, P> table;
  protected transient Timer.Context writeContext = null;
  protected transient WriteOperationType operationType;

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  protected BaseHoodieWriteClient(HoodieEngineContext context,
                                  BaseHoodieTable table,
                                  HoodieWriteConfig clientConfig,
                                  Option<AbstractEmbeddedTimelineService> timelineServer) {
    super(context, clientConfig, timelineServer);
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.table = table;
    this.index = table.getIndex();
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, O writeStatuses) {
    return commit(instantTime, writeStatuses, Option.empty());
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, O writeStatuses,
                        Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    return commit(instantTime, writeStatuses, extraMetadata, metaClient.getCommitActionType());
  }

  protected abstract boolean commit(String instantTime, O writeStatuses,
                                    Option<Map<String, String>> extraMetadata, String actionType);

  void emitCommitMetrics(String instantTime, HoodieCommitMetadata metadata, String actionType) {
    try {

      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime).getTime(), durationInMs,
            metadata, actionType);
        writeContext = null;
      }
    } catch (ParseException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime
          + "Instant time is not of valid format", e);
    }
  }

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   *
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  protected abstract void postCommit(HoodieCommitMetadata metadata, String instantTime, Option<Map<String, String>> extraMetadata);

  /**
   * Finalize Write operation.
   *
   * @param table       HoodieTable
   * @param instantTime Instant Time
   * @param stats       Hoodie Write Stat
   */
  protected abstract void finalizeWrite(BaseHoodieTable<T, I, K, O, P> table, String instantTime, List<HoodieWriteStat> stats);

  public HoodieMetrics getMetrics() {
    return metrics;
  }

  public HoodieIndex<T, I, K, O, P> getIndex() {
    return index;
  }

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  protected void setWriteSchemaForDeletes(HoodieTableMetaClient metaClient) {
    try {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      Option<HoodieInstant> lastInstant =
          activeTimeline.filterCompletedInstants().filter(s -> s.getAction().equals(metaClient.getCommitActionType()))
              .lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            activeTimeline.getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getExtraMetadata().containsKey(HoodieCommitMetadata.SCHEMA_KEY)) {
          config.setSchema(commitMetadata.getExtraMetadata().get(HoodieCommitMetadata.SCHEMA_KEY));
        } else {
          throw new HoodieIOException("Latest commit does not have any schema in commit metadata");
        }
      } else {
        throw new HoodieIOException("Deletes issued without any prior commits");
      }
    } catch (IOException e) {
      throw new HoodieIOException("IOException thrown while reading last commit metadata", e);
    }
  }

  @Override
  public void close() {
    // Stop timeline-server if running
    super.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public abstract HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException;
}
