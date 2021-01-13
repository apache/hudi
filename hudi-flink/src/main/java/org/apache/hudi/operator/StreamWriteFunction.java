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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.operator.event.BatchWriteSuccessEvent;
import org.apache.hudi.util.RowDataToAvroConverters;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * Sink function to write the data to the underneath filesystem.
 *
 * <p><h2>Work Flow</h2>
 *
 * <p>The function firstly buffers the data as a batch of {@link HoodieRecord}s,
 * It flushes(write) the records batch when a Flink checkpoint starts. After a batch has been written successfully,
 * the function notifies its operator coordinator {@link StreamWriteOperatorCoordinator} to mark a successful write.
 *
 * <p><h2>Exactly-once Semantics</h2>
 *
 * <p>The task implements exactly-once semantics by buffering the data between checkpoints. The operator coordinator
 * starts a new instant on the time line when a checkpoint triggers, the coordinator checkpoints always
 * start before its operator, so when this function starts a checkpoint, a REQUESTED instant already exists.
 * The function process thread then block data buffering and the checkpoint thread starts flushing the existing data buffer.
 * When the existing data buffer write successfully, the process thread unblock and start buffering again for the next round checkpoint.
 * Because any checkpoint failures would trigger the write rollback, it implements the exactly-once semantics.
 *
 * <p><h2>Fault Tolerance</h2>
 *
 * <p>The operator coordinator checks the validity for the last instant when it starts a new one. The operator rolls back
 * the written data and throws when any error occurs. This means any checkpoint or task failure would trigger a failover.
 * The operator coordinator would try several times when committing the writestatus.
 *
 * <p>Note: The function task requires the input stream be partitioned by the partition fields to avoid different write tasks
 * write to the same file group that conflict. The general case for partition path is a datetime field,
 * so the sink task is very possible to have IO bottleneck, the more flexible solution is to shuffle the
 * data by the file group IDs.
 *
 * @param <I> Type of the input record
 * @see StreamWriteOperatorCoordinator
 */
public class StreamWriteFunction<K, I, O> extends KeyedProcessFunction<K, I, O> implements CheckpointedFunction {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamWriteFunction.class);

  /**
   * Write buffer for a checkpoint.
   */
  private transient List<HoodieRecord> buffer;

  /**
   * The buffer lock to control data buffering/flushing.
   */
  private transient ReentrantLock bufferLock;

  /**
   * The condition to decide whether to add new records into the buffer.
   */
  private transient Condition addToBufferCondition;

  /**
   * Flag saying whether there is an on-going checkpoint.
   */
  private volatile boolean onCheckpointing = false;

  /**
   * Config options.
   */
  private final Configuration config;

  /**
   * Id of current subtask.
   */
  private int taskID;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  private transient BiFunction<List<HoodieRecord>, String, List<WriteStatus>> writeFunction;

  /**
   * HoodieKey generator.
   */
  private transient KeyGenerator keyGenerator;

  /**
   * Row type of the input.
   */
  private final RowType rowType;

  /**
   * Avro schema of the input.
   */
  private final Schema avroSchema;

  private transient RowDataToAvroConverters.RowDataToAvroConverter converter;

  /**
   * The REQUESTED instant we write the data.
   */
  private volatile String currentInstant;

  /**
   * Gateway to send operator events to the operator coordinator.
   */
  private transient OperatorEventGateway eventGateway;

  /**
   * Constructs a StreamingSinkFunction.
   *
   * @param rowType The input row type
   * @param config  The config options
   */
  public StreamWriteFunction(RowType rowType, Configuration config) {
    this.rowType = rowType;
    this.avroSchema = StreamerUtil.getSourceSchema(config);
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws IOException {
    this.taskID = getRuntimeContext().getIndexOfThisSubtask();
    this.keyGenerator = StreamerUtil.createKeyGenerator(FlinkOptions.flatOptions(this.config));
    this.converter = RowDataToAvroConverters.createConverter(this.rowType);
    initBuffer();
    initWriteClient();
    initWriteFunction();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    // no operation
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
    bufferLock.lock();
    try {
      // Based on the fact that the coordinator starts the checkpoint first,
      // it would check the validity.
      this.onCheckpointing = true;
      this.currentInstant = this.writeClient.getInflightAndRequestedInstant(this.config.get(FlinkOptions.TABLE_TYPE));
      Preconditions.checkNotNull(this.currentInstant,
          "No inflight instant when flushing data");
      // wait for the buffer data flush out and request a new instant
      flushBuffer();
      // signal the task thread to start buffering
      addToBufferCondition.signal();
    } finally {
      this.onCheckpointing = false;
      bufferLock.unlock();
    }
  }

  @Override
  public void processElement(I value, KeyedProcessFunction<K, I, O>.Context ctx, Collector<O> out) throws Exception {
    bufferLock.lock();
    try {
      if (onCheckpointing) {
        addToBufferCondition.await();
      }
      this.buffer.add(toHoodieRecord(value));
    } finally {
      bufferLock.unlock();
    }
  }

  @Override
  public void close() {
    if (this.writeClient != null) {
      this.writeClient.close();
    }
  }

  // -------------------------------------------------------------------------
  //  Getter/Setter
  // -------------------------------------------------------------------------

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public List<HoodieRecord> getBuffer() {
    return buffer;
  }

  @VisibleForTesting
  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return writeClient;
  }

  public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    this.eventGateway = operatorEventGateway;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private void initBuffer() {
    this.buffer = new ArrayList<>();
    this.bufferLock = new ReentrantLock();
    this.addToBufferCondition = this.bufferLock.newCondition();
  }

  private void initWriteClient() {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(StreamerUtil.getHadoopConf()),
            new FlinkTaskContextSupplier(getRuntimeContext()));

    writeClient = new HoodieFlinkWriteClient<>(context, StreamerUtil.getHoodieClientConfig(this.config));
  }

  private void initWriteFunction() {
    final String writeOperation = this.config.get(FlinkOptions.OPERATION);
    switch (WriteOperationType.fromValue(writeOperation)) {
      case INSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.insert(records, instantTime);
        break;
      case UPSERT:
        this.writeFunction = (records, instantTime) -> this.writeClient.upsert(records, instantTime);
        break;
      default:
        throw new RuntimeException("Unsupported write operation : " + writeOperation);
    }
  }

  /**
   * Converts the give record to a {@link HoodieRecord}.
   *
   * @param record The input record
   * @return HoodieRecord based on the configuration
   * @throws IOException if error occurs
   */
  @SuppressWarnings("rawtypes")
  private HoodieRecord toHoodieRecord(I record) throws IOException {
    boolean shouldCombine = this.config.getBoolean(FlinkOptions.INSERT_DROP_DUPS)
        || WriteOperationType.fromValue(this.config.getString(FlinkOptions.OPERATION)) == WriteOperationType.UPSERT;
    GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
    HoodieRecordPayload payload = shouldCombine
        ? StreamerUtil.createPayload(this.config.getString(FlinkOptions.PAYLOAD_CLASS), gr,
        (Comparable) HoodieAvroUtils.getNestedFieldVal(gr, this.config.getString(FlinkOptions.PRECOMBINE_FIELD), false))
        : StreamerUtil.createPayload(this.config.getString(FlinkOptions.PAYLOAD_CLASS), gr);
    return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
  }

  private void flushBuffer() {
    final List<WriteStatus> writeStatus;
    if (buffer.size() > 0) {
      writeStatus = writeFunction.apply(buffer, currentInstant);
      buffer.clear();
    } else {
      LOG.info("No data to write in subtask [{}] for instant [{}]", taskID, currentInstant);
      writeStatus = Collections.emptyList();
    }
    this.eventGateway.sendEventToCoordinator(new BatchWriteSuccessEvent(this.taskID, currentInstant, writeStatus));
    this.currentInstant = "";
  }
}
