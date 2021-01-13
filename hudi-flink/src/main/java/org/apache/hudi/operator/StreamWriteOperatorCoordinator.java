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

package org.apache.hudi.operator;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.operator.event.BatchWriteSuccessEvent;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@link OperatorCoordinator} for {@link StreamWriteFunction}.
 *
 * <p>This coordinator starts a new instant when a new checkpoint starts. It commits the instant when all the
 * operator tasks write the buffer successfully for a round of checkpoint.
 *
 * <p>If there is no data for a round of checkpointing, it rolls back the metadata.
 *
 * @see StreamWriteFunction for the work flow and semantics
 */
public class StreamWriteOperatorCoordinator
    implements OperatorCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteOperatorCoordinator.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Write client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  private long inFlightCheckpoint = -1;

  /**
   * Current REQUESTED instant, for validation.
   */
  private String inFlightInstant = "";

  /**
   * Event buffer for one round of checkpointing. When all the elements are non-null and have the same
   * write instant, then the instant succeed and we can commit it.
   */
  private transient BatchWriteSuccessEvent[] eventBuffer;

  /**
   * Task number of the operator.
   */
  private final int parallelism;

  /**
   * Constructs a StreamingSinkOperatorCoordinator.
   *
   * @param conf        The config options
   * @param parallelism The operator task number
   */
  public StreamWriteOperatorCoordinator(
      Configuration conf,
      int parallelism) {
    this.conf = conf;
    this.parallelism = parallelism;
  }

  @Override
  public void start() throws Exception {
    // initialize event buffer
    reset();
    // writeClient
    initWriteClient();
    // init table, create it if not exists.
    initTable();
  }

  @Override
  public void close() {
    if (writeClient != null) {
      writeClient.close();
    }
    this.eventBuffer = null;
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
    try {
      final String errMsg = "A new checkpoint starts while the last checkpoint buffer"
          + " data has not finish writing, roll back the last write and throw";
      checkAndForceCommit(errMsg);
      this.inFlightInstant = this.writeClient.startCommit();
      this.inFlightCheckpoint = checkpointId;
      LOG.info("Create instant [{}], at checkpoint [{}]", this.inFlightInstant, checkpointId);
      result.complete(writeCheckpointBytes());
    } catch (Throwable throwable) {
      // when a checkpoint fails, throws directly.
      result.completeExceptionally(
          new CompletionException(
              String.format("Failed to checkpoint Instant %s for source %s",
                  this.inFlightInstant, this.getClass().getSimpleName()), throwable));
    }
  }

  @Override
  public void checkpointComplete(long checkpointId) {
    // start to commit the instant.
    checkAndCommitWithRetry();
  }

  public void notifyCheckpointAborted(long checkpointId) {
    Preconditions.checkState(inFlightCheckpoint == checkpointId,
        "The aborted checkpoint should always be the last checkpoint");
    checkAndForceCommit("The last checkpoint was aborted, roll back the last write and throw");
  }

  @Override
  public void resetToCheckpoint(@Nullable byte[] checkpointData) throws Exception {
    if (checkpointData != null) {
      // restore when any checkpoint completed
      deserializeCheckpointAndRestore(checkpointData);
    }
  }

  @Override
  public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
    // no event to handle
    Preconditions.checkState(operatorEvent instanceof BatchWriteSuccessEvent,
        "The coordinator can only handle BatchWriteSuccessEvent");
    BatchWriteSuccessEvent event = (BatchWriteSuccessEvent) operatorEvent;
    Preconditions.checkState(event.getInstantTime().equals(this.inFlightInstant),
        String.format("Receive an unexpected event for instant %s from task %d",
            event.getInstantTime(), event.getTaskID()));
    this.eventBuffer[event.getTaskID()] = event;
  }

  @Override
  public void subtaskFailed(int i, @Nullable Throwable throwable) {
    // no operation
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  @SuppressWarnings("rawtypes")
  private void initWriteClient() {
    writeClient = new HoodieFlinkWriteClient(
        new HoodieFlinkEngineContext(new FlinkTaskContextSupplier(null)),
        StreamerUtil.getHoodieClientConfig(this.conf),
        true);
  }

  private void initTable() throws IOException {
    final String basePath = this.conf.getString(FlinkOptions.PATH);
    final org.apache.hadoop.conf.Configuration hadoopConf = StreamerUtil.getHadoopConf();
    // Hadoop FileSystem
    try (FileSystem fs = FSUtils.getFs(basePath, hadoopConf)) {
      if (!fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))) {
        HoodieTableMetaClient.initTableType(
            hadoopConf,
            basePath,
            HoodieTableType.valueOf(this.conf.getString(FlinkOptions.TABLE_TYPE)),
            this.conf.getString(FlinkOptions.TABLE_NAME),
            "archived",
            this.conf.getString(FlinkOptions.PAYLOAD_CLASS),
            1);
        LOG.info("Table initialized");
      } else {
        LOG.info("Table [{}/{}] already exists, no need to initialize the table",
            basePath, this.conf.getString(FlinkOptions.TABLE_NAME));
      }
    }
  }

  static byte[] readBytes(DataInputStream in, int size) throws IOException {
    byte[] bytes = new byte[size];
    in.readFully(bytes);
    return bytes;
  }

  /**
   * Serialize the coordinator state. The current implementation may not be super efficient,
   * but it should not matter that much because most of the state should be rather small.
   * Large states themselves may already be a problem regardless of how the serialization
   * is implemented.
   *
   * @return A byte array containing the serialized state of the source coordinator.
   * @throws IOException When something goes wrong in serialization.
   */
  private byte[] writeCheckpointBytes() throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {

      out.writeLong(this.inFlightCheckpoint);
      byte[] serializedInstant = this.inFlightInstant.getBytes();
      out.writeInt(serializedInstant.length);
      out.write(serializedInstant);
      out.flush();
      return baos.toByteArray();
    }
  }

  /**
   * Restore the state of this source coordinator from the state bytes.
   *
   * @param bytes The checkpoint bytes that was returned from {@link #writeCheckpointBytes()}
   * @throws Exception When the deserialization failed.
   */
  private void deserializeCheckpointAndRestore(byte[] bytes) throws Exception {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         DataInputStream in = new DataInputViewStreamWrapper(bais)) {
      long checkpointID = in.readLong();
      int serializedInstantSize = in.readInt();
      byte[] serializedInstant = readBytes(in, serializedInstantSize);
      this.inFlightCheckpoint = checkpointID;
      this.inFlightInstant = new String(serializedInstant);
    }
  }

  private void reset() {
    this.inFlightInstant = "";
    this.eventBuffer = new BatchWriteSuccessEvent[this.parallelism];
  }

  private void checkAndForceCommit(String errMsg) {
    if (!checkReady()) {
      // forced but still has inflight instant
      String inflightInstant = writeClient.getInflightAndRequestedInstant(this.conf.getString(FlinkOptions.TABLE_TYPE));
      if (inflightInstant != null) {
        assert inflightInstant.equals(this.inFlightInstant);
        writeClient.rollback(this.inFlightInstant);
        throw new HoodieException(errMsg);
      }
      if (Arrays.stream(eventBuffer).allMatch(Objects::isNull)) {
        // The last checkpoint finished successfully.
        return;
      }
    }
    doCommit();
  }

  private void checkAndCommitWithRetry() {
    int retryTimes = this.conf.getInteger(FlinkOptions.RETRY_TIMES);
    long retryIntervalMillis = this.conf.getLong(FlinkOptions.RETRY_INTERVAL_MS);
    int tryTimes = 0;
    while (tryTimes++ < retryTimes) {
      try {
        if (!checkReady()) {
          // Do not throw if the try times expires but the event buffer are still not ready,
          // because we have a force check when next checkpoint starts.
          waitFor(retryIntervalMillis);
          continue;
        }
        doCommit();
        return;
      } catch (Throwable throwable) {
        String cause = throwable.getCause() == null ? "" : throwable.getCause().toString();
        LOG.warn("Try to commit the instant {} failed, with times {} and cause {}", this.inFlightInstant, tryTimes, cause);
        if (tryTimes == retryTimes) {
          throw new HoodieException(throwable);
        }
        waitFor(retryIntervalMillis);
      }
    }
  }

  private void waitFor(long intervalMillis) {
    try {
      TimeUnit.MILLISECONDS.sleep(intervalMillis);
    } catch (InterruptedException e) {
      LOG.error("Thread interrupted while waiting to retry the instant commits");
      throw new HoodieException(e);
    }
  }

  /** Checks the buffer is ready to commit. */
  private boolean checkReady() {
    return Arrays.stream(eventBuffer).allMatch(event ->
        event != null && event.getInstantTime().equals(this.inFlightInstant));
  }

  /** Performs the actual commit action. */
  private void doCommit() {
    List<WriteStatus> writeResults = Arrays.stream(eventBuffer)
        .map(BatchWriteSuccessEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (writeResults.size() == 0) {
      // No data has written, clear the metadata file
      this.writeClient.deletePendingInstant(this.conf.getString(FlinkOptions.TABLE_TYPE), this.inFlightInstant);
      reset();
      return;
    }

    // commit or rollback
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
    boolean hasErrors = totalErrorRecords > 0;

    if (!hasErrors || this.conf.getBoolean(FlinkOptions.IGNORE_FAILED_BATCH)) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (hasErrors) {
        LOG.warn("Some records failed to merge but forcing commit since commitOnErrors set to true. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = writeClient.commit(this.inFlightInstant, writeResults, Option.of(checkpointCommitMetadata));
      if (success) {
        reset();
        LOG.info("Commit instant [{}] success!", this.inFlightInstant);
      } else {
        throw new HoodieException(String.format("Commit instant [%s] failed!", this.inFlightInstant));
      }
    } else {
      LOG.error("Error when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("The first 100 error messages");
      writeResults.stream().filter(WriteStatus::hasErrors).limit(100).forEach(ws -> {
        LOG.error("Global error for partition path {} and fileID {}: {}",
            ws.getGlobalError(), ws.getPartitionPath(), ws.getFileId());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " and value " + value));
        }
      });
      // Rolls back instant
      writeClient.rollback(this.inFlightInstant);
      throw new HoodieException(String.format("Commit instant [%s] failed and rolled back !", this.inFlightInstant));
    }
  }

  @VisibleForTesting
  public BatchWriteSuccessEvent[] getEventBuffer() {
    return eventBuffer;
  }

  @VisibleForTesting
  public String getInFlightInstant() {
    return inFlightInstant;
  }

  /**
   * Provider for {@link StreamWriteOperatorCoordinator}.
   */
  public static class Provider implements OperatorCoordinator.Provider {
    private final OperatorID operatorId;
    private final Configuration conf;
    private final int numTasks;

    public Provider(OperatorID operatorId, Configuration conf, int numTasks) {
      this.operatorId = operatorId;
      this.conf = conf;
      this.numTasks = numTasks;
    }

    public OperatorID getOperatorId() {
      return this.operatorId;
    }

    public OperatorCoordinator create(Context context) {
      return new StreamWriteOperatorCoordinator(this.conf, this.numTasks);
    }
  }
}
