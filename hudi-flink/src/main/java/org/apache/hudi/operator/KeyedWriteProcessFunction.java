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

package org.apache.hudi.operator;

import org.apache.hudi.HoodieFlinkStreamer;
import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieFlinkStreamerException;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * A {@link KeyedProcessFunction} where the write operations really happens.
 */
public class KeyedWriteProcessFunction extends KeyedProcessFunction<String, HoodieRecord, Tuple3<String, List<WriteStatus>, Integer>> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(KeyedWriteProcessFunction.class);
  /**
   * Records buffer, will be processed in snapshotState function.
   */
  private List<HoodieRecord> bufferedRecords = new LinkedList<>();

  /**
   * Flink collector help s to send data downstream.
   */
  private Collector<Tuple3<String, List<WriteStatus>, Integer>> output;

  /**
   * Id of current subtask.
   */
  private int indexOfThisSubtask;

  /**
   * Instant time this batch belongs to.
   */
  private String latestInstant;

  /**
   * Flag indicate whether this subtask has records in.
   */
  private boolean hasRecordsIn;

  /**
   * Job conf.
   */
  private HoodieFlinkStreamer.Config cfg;

  /**
   * Write Client.
   */
  private transient HoodieFlinkWriteClient writeClient;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

    cfg = (HoodieFlinkStreamer.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()), new FlinkTaskContextSupplier(getRuntimeContext()));

    writeClient = new HoodieFlinkWriteClient<>(context, StreamerUtil.getHoodieClientConfig(cfg));
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {

    // get latest requested instant
    String commitType = cfg.tableType.equals(HoodieTableType.COPY_ON_WRITE.name()) ? HoodieTimeline.COMMIT_ACTION : HoodieTimeline.DELTA_COMMIT_ACTION;
    List<String> latestInstants = writeClient.getInflightsAndRequestedInstants(commitType);
    latestInstant = latestInstants.isEmpty() ? null : latestInstants.get(0);

    if (bufferedRecords.size() > 0) {
      hasRecordsIn = true;
      if (output != null && latestInstant != null) {
        String instantTimestamp = latestInstant;
        LOG.info("Write records, subtask id = [{}]  checkpoint_id = [{}}] instant = [{}], record size = [{}]", indexOfThisSubtask, context.getCheckpointId(), instantTimestamp, bufferedRecords.size());

        List<WriteStatus> writeStatus;
        switch (cfg.operation) {
          case INSERT:
            writeStatus = writeClient.insert(bufferedRecords, instantTimestamp);
            break;
          case UPSERT:
            writeStatus = writeClient.upsert(bufferedRecords, instantTimestamp);
            break;
          default:
            throw new HoodieFlinkStreamerException("Unknown operation : " + cfg.operation);
        }
        output.collect(new Tuple3<>(instantTimestamp, writeStatus, indexOfThisSubtask));
        bufferedRecords.clear();
      }
    } else {
      LOG.info("No data in subtask [{}]", indexOfThisSubtask);
      hasRecordsIn = false;
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) {
    // no operation
  }

  @Override
  public void processElement(HoodieRecord hoodieRecord, Context context, Collector<Tuple3<String, List<WriteStatus>, Integer>> collector) {
    if (output == null) {
      output = collector;
    }

    // buffer the records
    bufferedRecords.add(hoodieRecord);
  }

  public boolean hasRecordsIn() {
    return hasRecordsIn;
  }

  public String getLatestInstant() {
    return latestInstant;
  }

  @Override
  public void close() {
    if (writeClient != null) {
      writeClient.close();
    }
  }
}
