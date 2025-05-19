/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.hudi.adapter.OperatorCoordinatorAdapter;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sink.common.AbstractStreamWriteFunction;
import org.apache.hudi.sink.event.Correspondent;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.CoordinationResponseSeDe;
import org.apache.hudi.sink.utils.EventBuffers;
import org.apache.hudi.sink.utils.ExplicitClassloaderThreadFactory;
import org.apache.hudi.sink.utils.HiveSyncContext;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.util.ClientIds;
import org.apache.hudi.util.ClusteringUtil;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.FlinkWriteClients;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.util.StreamerUtil.initTableIfNotExists;

/**
 * {@link OperatorCoordinator} for {@link StreamWriteFunction}.
 *
 * <p>This coordinator starts a new instant when a new checkpoint starts. It commits the instant when all the
 * operator tasks write the buffer successfully for a round of checkpoint.
 *
 * <p>If there is no data for a round of checkpointing,
 * the coordinator will either commit the instant or rolls back it in LAZY cleaning
 * based on the user configuration.
 *
 * <p><h2>Insurance of Exactly Once Semantics</h2></p>
 *
 * <p>Task failover workflow:
 * <pre>
 *             task failover
 *                  |
 *           initially started ?
 *            /(yes)     \(no)
 *           /            \
 *       has pending      send an empty
 *     instant event ?    bootstrap event
 *        /(yes)  \(no)
 *       /         \
 *    resend      no-op
 * </pre>
 *
 * @see StreamWriteFunction         for the data inputs checkpointing and semantics
 * @see AbstractStreamWriteFunction for the bootstrap event sending workflow
 */
public class StreamWriteOperatorCoordinator
    implements OperatorCoordinatorAdapter, CoordinationRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteOperatorCoordinator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Hive config options.
   */
  private final StorageConfiguration<org.apache.hadoop.conf.Configuration> storageConf;

  /**
   * Coordinator context.
   */
  private final Context context;

  /**
   * Gateways for sending events to sub-tasks.
   */
  private transient SubtaskGateway[] gateways;

  /**
   * Write client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  /**
   * Meta client.
   */
  private transient HoodieTableMetaClient metaClient;

  /**
   * Current REQUESTED instant, for validation.
   */
  private volatile String instant = WriteMetadataEvent.BOOTSTRAP_INSTANT;

  /**
   * Event buffers for checkpointing.
   *
   * <p>It's a map of {checkpointId -> (instant, events)}.
   *
   * <p>A checkpoint ack event of one instant implies
   * that the instant write succeed then we can commit it.
   */
  private transient EventBuffers eventBuffers;

  /**
   * Task number of the operator.
   */
  private final int parallelism;

  /**
   * A single-thread executor to handle all the write metadata events.
   */
  private NonThrownExecutor executor;

  /**
   * A single-thread executor to handle the instant time request.
   */
  private NonThrownExecutor instantRequestExecutor;

  /**
   * A single-thread executor to handle asynchronous hive sync.
   */
  private NonThrownExecutor hiveSyncExecutor;

  /**
   * Context that holds variables for asynchronous hive sync.
   */
  private HiveSyncContext hiveSyncContext;

  /**
   * The table state.
   */
  private transient TableState tableState;

  /**
   * The client id heartbeats.
   */
  private ClientIds clientIds;

  /**
   * Constructs a StreamingSinkOperatorCoordinator.
   *
   * @param conf    The config options
   * @param context The coordinator context
   */
  public StreamWriteOperatorCoordinator(
      Configuration conf,
      Context context) {
    this.conf = conf;
    this.context = context;
    this.parallelism = context.currentParallelism();
    this.storageConf = HadoopFSUtils.getStorageConfWithCopy(HadoopConfigurations.getHiveConf(conf));
  }

  @Override
  public void start() throws Exception {
    // setup classloader for APIs that use reflection without taking ClassLoader param
    // reference: https://stackoverflow.com/questions/1771679/difference-between-threads-context-class-loader-and-normal-classloader
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    // initialize event buffer
    this.eventBuffers = EventBuffers.getInstance();
    this.gateways = new SubtaskGateway[this.parallelism];
    try {
      // init table, create if not exists.
      this.metaClient = initTableIfNotExists(this.conf);
      // the write client must create after the table creation
      this.writeClient = FlinkWriteClients.createWriteClient(conf);
      this.writeClient.tryUpgrade(instant, this.metaClient);
      initMetadataTable(this.writeClient);
      this.tableState = TableState.create(conf);
      // start the executor
      this.executor = NonThrownExecutor.builder(LOG)
          .threadFactory(getThreadFactory("meta-event-handle"))
          .exceptionHook((errMsg, t) -> this.context.failJob(new HoodieException(errMsg, t)))
          .waitForTasksFinish(true).build();
      this.instantRequestExecutor = NonThrownExecutor.builder(LOG)
          .threadFactory(getThreadFactory("instant-response"))
          .exceptionHook((errMsg, t) -> this.context.failJob(new HoodieException(errMsg, t)))
          .build();
      // start the executor if required
      if (tableState.syncHive) {
        initHiveSync();
      }
      // start client id heartbeats for optimistic concurrency control
      if (OptionsResolver.isMultiWriter(conf)) {
        initClientIds(conf);
      }
    } catch (Throwable throwable) {
      LOG.error("Failed to start operator coordinator.", throwable);
      context.failJob(throwable);
    }
  }

  @Override
  public void close() throws Exception {
    // teardown the resource
    if (executor != null) {
      executor.close();
    }
    if (instantRequestExecutor != null) {
      instantRequestExecutor.close();
    }
    if (hiveSyncExecutor != null) {
      hiveSyncExecutor.close();
    }
    // the write client must close after the executor service
    // because the task in the service may send requests to the embedded timeline service.
    if (writeClient != null) {
      writeClient.close();
    }
    this.eventBuffers = null;
    if (this.clientIds != null) {
      this.clientIds.close();
    }
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
    executor.execute(
        () -> {
          try {
            result.complete(new byte[0]);
          } catch (Throwable throwable) {
            // when a checkpoint fails, throws directly.
            result.completeExceptionally(
                new CompletionException(
                    String.format("Failed to checkpoint Instant %s for source %s",
                        this.instant, this.getClass().getSimpleName()), throwable));
          }
        }, "taking checkpoint %d", checkpointId
    );
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    executor.execute(
        () -> {
          // for streaming mode, commits the ever received events anyway,
          // the stream write task snapshot and flush the data buffer synchronously in sequence,
          // so a successful checkpoint subsumes the old one(follows the checkpoint subsuming contract)
          final boolean committed = commitInstants(checkpointId);
          // schedules the compaction or clustering if it is enabled in stream execution mode
          scheduleTableServices(committed);

          if (committed) {
            // sync Hive if is enabled
            syncHiveAsync();
          }
        }, "commits the instant %s", this.instant
    );
  }

  @Override
  public void resetToCheckpoint(long checkpointID, byte[] checkpointData) {
    // no operation
  }

  @Override
  public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
    ValidationUtils.checkState(operatorEvent instanceof WriteMetadataEvent,
        "The coordinator can only handle WriteMetaEvent");
    WriteMetadataEvent event = (WriteMetadataEvent) operatorEvent;

    if (event.isEndInput()) {
      // handle end input event synchronously
      // wrap handleEndInputEvent in executeSync to preserve the order of events
      executor.executeSync(() -> handleEndInputEvent(event), "handle end input event for instant %s", this.instant);
    } else {
      executor.execute(
          () -> {
            if (event.isBootstrap()) {
              handleBootstrapEvent(event);
            } else {
              handleWriteMetaEvent(event);
            }
          }, "handle write metadata event for instant %s", this.instant
      );
    }
  }

  @Override
  public void subtaskFailed(int i, @Nullable Throwable throwable) {
    // no operation
  }

  @Override
  public void subtaskReset(int i, long l) {
    // no operation
  }

  @Override
  public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
    this.gateways[i] = subtaskGateway;
  }

  @Override
  public CompletableFuture<CoordinationResponse> handleCoordinationRequest(CoordinationRequest request) {
    CompletableFuture<CoordinationResponse> response = new CompletableFuture<>();
    instantRequestExecutor.execute(() -> {
      Correspondent.InstantTimeRequest instantTimeRequest = (Correspondent.InstantTimeRequest) request;
      long checkpointId = instantTimeRequest.getCheckpointId();
      Pair<String, WriteMetadataEvent[]> instantTimeAndEventBuffer = this.eventBuffers.getInstantAndEventBuffer(checkpointId);
      final String instantTime;
      if (instantTimeAndEventBuffer == null) {
        instantTime = startInstant();
        this.eventBuffers.initNewEventBuffer(checkpointId, instantTime, this.parallelism);
      } else {
        instantTime = instantTimeAndEventBuffer.getLeft();
      }
      response.complete(CoordinationResponseSeDe.wrap(Correspondent.InstantTimeResponse.getInstance(instantTime)));
    }, "request instant time");
    return response;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private ThreadFactory getThreadFactory(String threadName) {
    return new ExplicitClassloaderThreadFactory(threadName, context.getUserCodeClassloader());
  }

  private void initHiveSync() {
    this.hiveSyncExecutor = NonThrownExecutor.builder(LOG)
        .threadFactory(getThreadFactory("hive-sync"))
        .waitForTasksFinish(true).build();
    this.hiveSyncContext = HiveSyncContext.create(conf, this.storageConf);
  }

  private void syncHiveAsync() {
    if (tableState.syncHive) {
      this.hiveSyncExecutor.execute(this::doSyncHive, "sync hive metadata for instant %s", this.instant);
    }
  }

  private void syncHive() {
    if (tableState.syncHive) {
      doSyncHive();
      LOG.info("Sync hive metadata for instant {} success!", this.instant);
    }
  }

  /**
   * Sync hoodie table metadata to Hive metastore.
   */
  public void doSyncHive() {
    try (HiveSyncTool syncTool = hiveSyncContext.hiveSyncTool()) {
      syncTool.syncHoodieTable();
    }
  }

  private void scheduleTableServices(Boolean committed) {
    // if compaction is on, schedule the compaction
    if (tableState.scheduleCompaction) {
      CompactionUtil.scheduleCompaction(writeClient, tableState.isDeltaTimeCompaction, committed);
    }
    // if clustering is on, schedule the clustering
    if (tableState.scheduleClustering) {
      ClusteringUtil.scheduleClustering(conf, writeClient, committed);
    }
  }

  private static void initMetadataTable(HoodieFlinkWriteClient<?> writeClient) {
    writeClient.initMetadataTable();
  }

  private void initClientIds(Configuration conf) {
    this.clientIds = ClientIds.builder().conf(conf).build();
    this.clientIds.start();
  }

  private String startInstant() {
    // refresh the meta client which is reused
    metaClient.reloadActiveTimeline();
    // refresh the last txn metadata
    this.writeClient.preTxn(tableState.operationType, this.metaClient);
    // put the assignment in front of metadata generation,
    // because the instant request from write task is asynchronous.
    this.instant = this.writeClient.startCommit(tableState.commitAction, this.metaClient);
    this.metaClient.getActiveTimeline().transitionRequestedToInflight(tableState.commitAction, this.instant);
    this.writeClient.setWriteTimer(tableState.commitAction);
    LOG.info("Create instant [{}] for table [{}] with type [{}]", this.instant,
        this.conf.getString(FlinkOptions.TABLE_NAME), conf.getString(FlinkOptions.TABLE_TYPE));
    return this.instant;
  }

  /**
   * Recommits the last inflight instant if the write metadata checkpoint successfully
   * but was not committed due to some rare cases.
   */
  private void recommitInstant(long checkpointId, String instant, WriteMetadataEvent[] bootstrapBuffer) {
    HoodieTimeline completedTimeline = this.metaClient.getActiveTimeline().filterCompletedInstants();
    if (!completedTimeline.containsInstant(instant)) {
      LOG.info("Recommit instant {}", instant);
      // Recommit should start heartbeat for lazy failed writes clean policy to avoid aborting for heartbeat expired;
      // The following up checkpoints would recommit the instant.
      if (writeClient.getConfig().getFailedWritesCleanPolicy().isLazy()) {
        writeClient.getHeartbeatClient().start(instant);
      }
      commitInstant(checkpointId, instant, bootstrapBuffer);
    }
  }

  private void handleBootstrapEvent(WriteMetadataEvent event) {
    if (event.getInstantTime().equals(WriteMetadataEvent.BOOTSTRAP_INSTANT)) {
      this.eventBuffers.cleanLegacyEvents(event);
      return;
    }
    WriteMetadataEvent[] eventBuffer = this.eventBuffers.getOrCreateBootstrapBuffer(event, this.parallelism);
    eventBuffer[event.getTaskID()] = event;
    if (Arrays.stream(eventBuffer).allMatch(evt -> evt != null && evt.isBootstrap())) {
      // start to recommit the instant.
      recommitInstant(event.getCheckpointId(), event.getInstantTime(), eventBuffer);
    }
  }

  private void handleEndInputEvent(WriteMetadataEvent event) {
    WriteMetadataEvent[] eventBuffer = this.eventBuffers.addEventToBuffer(event);
    if (EventBuffers.allEventsReceived(eventBuffer)) {
      // start to commit the instant.
      boolean committed = commitInstant(event.getCheckpointId(), event.getInstantTime(), eventBuffer);
      if (committed) {
        // sync Hive synchronously if it is enabled in batch mode.
        syncHive();
        // schedules the compaction or clustering if it is enabled in batch execution mode
        scheduleTableServices(true);
      }
    }
  }

  private void handleWriteMetaEvent(WriteMetadataEvent event) {
    // the write task does not block after checkpointing(and before it receives a checkpoint success event),
    // if it checkpoints succeed then flushes the data buffer again before this coordinator receives a checkpoint
    // success event, the data buffer would flush with an older instant time.
    ValidationUtils.checkState(
        compareTimestamps(this.instant, GREATER_THAN_OR_EQUALS, event.getInstantTime()),
        String.format("Receive an unexpected event for instant %s from task %d",
            event.getInstantTime(), event.getTaskID()));

    this.eventBuffers.addEventToBuffer(event);
  }

  /**
   * Commits the instant.
   */
  private boolean commitInstants(long checkpointId) {
    // use < instead of <= because the write metadata event sends the last known checkpoint id which is smaller than the current one.
    List<Boolean> result = this.eventBuffers.getEventBufferStream().filter(entry -> entry.getKey() < checkpointId)
        .map(entry -> commitInstant(entry.getKey(), entry.getValue().getLeft(), entry.getValue().getRight())).collect(Collectors.toList());
    return result.stream().anyMatch(i -> i);
  }

  /**
   * Commits the instant.
   *
   * @return true if the write statuses are committed successfully.
   */
  private boolean commitInstant(long checkpointId, String instant, WriteMetadataEvent[] eventBuffer) {
    if (Arrays.stream(eventBuffer).allMatch(Objects::isNull)) {
      // all the tasks are reset by failover, reset the while buffer and returns early.
      this.eventBuffers.reset(checkpointId);
      // stop the heart beat for lazy cleaning
      writeClient.getHeartbeatClient().stop(instant);
      return false;
    }

    List<WriteStatus> writeResults = Arrays.stream(eventBuffer)
        .filter(Objects::nonNull)
        .map(WriteMetadataEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (writeResults.isEmpty() && !OptionsResolver.allowCommitOnEmptyBatch(conf)) {
      // No data has written, reset the buffer and returns early
      this.eventBuffers.reset(checkpointId);
      // stop the heart beat for lazy cleaning
      writeClient.getHeartbeatClient().stop(instant);
      return false;
    }
    doCommit(checkpointId, instant, writeResults);
    return true;
  }

  /**
   * Performs the actual commit action.
   */
  @SuppressWarnings("unchecked")
  private void doCommit(long checkpointId, String instant, List<WriteStatus> writeResults) {
    // commit or rollback
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
    boolean hasErrors = totalErrorRecords > 0;

    if (!hasErrors || this.conf.get(FlinkOptions.IGNORE_FAILED)) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (hasErrors) {
        LOG.warn("Some records failed to merge but forcing commit since commitOnErrors set to true. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      final Map<String, List<String>> partitionToReplacedFileIds = tableState.isOverwrite
          ? writeClient.getPartitionToReplacedFileIds(tableState.operationType, writeResults)
          : Collections.emptyMap();
      boolean success = writeClient.commit(instant, writeResults, Option.of(checkpointCommitMetadata),
          tableState.commitAction, partitionToReplacedFileIds);
      if (success) {
        this.eventBuffers.reset(checkpointId);
        LOG.info("Commit instant [{}] success!", instant);
      } else {
        throw new HoodieException(String.format("Commit instant [%s] failed!", instant));
      }
    } else {
      LOG.error("Error when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("The first 10 files with write errors:");
      writeResults.stream().filter(WriteStatus::hasErrors).limit(10).forEach(ws -> {
        if (ws.getGlobalError() != null) {
          LOG.error("Global error for partition path {} and fileID {}: {}",
              ws.getPartitionPath(), ws.getFileId(), ws.getGlobalError());
        }
        if (!ws.getErrors().isEmpty()) {
          LOG.error("The first 100 records-level errors for partition path {} and fileID {}:",
              ws.getPartitionPath(), ws.getFileId());
          ws.getErrors().entrySet().stream().limit(100).forEach(entry -> LOG.error("Error for key: "
              + entry.getKey() + " and Exception: " + entry.getValue().getMessage()));
        }
      });
      // Rolls back instant
      writeClient.rollback(instant);
      throw new HoodieException(String.format("Commit instant [%s] failed and rolled back !", instant));
    }
  }

  @VisibleForTesting
  public WriteMetadataEvent[] getEventBuffer() {
    return this.eventBuffers.getLatestEventBuffer(this.instant);
  }

  @VisibleForTesting
  public WriteMetadataEvent[] getEventBuffer(long checkpointId) {
    return this.eventBuffers.getEventBuffer(checkpointId);
  }

  @VisibleForTesting
  public String getInstant() {
    return instant;
  }

  @VisibleForTesting
  public Context getContext() {
    return context;
  }

  @VisibleForTesting
  public HoodieFlinkWriteClient getWriteClient() {
    return writeClient;
  }

  @VisibleForTesting
  public void setExecutor(NonThrownExecutor executor) throws Exception {
    if (this.executor != null) {
      this.executor.close();
    }
    this.executor = executor;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Provider for {@link StreamWriteOperatorCoordinator}.
   */
  public static class Provider implements OperatorCoordinator.Provider {
    private final OperatorID operatorId;
    private final Configuration conf;

    public Provider(OperatorID operatorId, Configuration conf) {
      this.operatorId = operatorId;
      this.conf = conf;
    }

    @Override
    public OperatorID getOperatorId() {
      return this.operatorId;
    }

    @Override
    public OperatorCoordinator create(Context context) {
      return new StreamWriteOperatorCoordinator(this.conf, context);
    }
  }

  /**
   * Remember some table state variables.
   */
  private static class TableState implements Serializable {
    private static final long serialVersionUID = 1L;

    final WriteOperationType operationType;
    final String commitAction;
    final boolean isOverwrite;
    final boolean scheduleCompaction;
    final boolean scheduleClustering;
    final boolean syncHive;
    final boolean syncMetadata;
    final boolean isDeltaTimeCompaction;

    private TableState(Configuration conf) {
      this.operationType = WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION));
      this.commitAction = CommitUtils.getCommitActionType(this.operationType,
          HoodieTableType.valueOf(conf.getString(FlinkOptions.TABLE_TYPE).toUpperCase(Locale.ROOT)));
      this.isOverwrite = WriteOperationType.isOverwrite(this.operationType);
      this.scheduleCompaction = OptionsResolver.needsScheduleCompaction(conf);
      this.scheduleClustering = OptionsResolver.needsScheduleClustering(conf);
      this.syncHive = conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED);
      this.syncMetadata = conf.getBoolean(FlinkOptions.METADATA_ENABLED);
      this.isDeltaTimeCompaction = OptionsResolver.isDeltaTimeCompaction(conf);
    }

    public static TableState create(Configuration conf) {
      return new TableState(conf);
    }
  }
}
