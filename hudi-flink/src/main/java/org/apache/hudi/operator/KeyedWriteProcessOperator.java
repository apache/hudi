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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Operator helps to mock empty write results and deliver downstream when no data flow in some subtask.
 */
public class KeyedWriteProcessOperator extends KeyedProcessOperator<String, HoodieRecord, Tuple3<String, List<WriteStatus>, Integer>> {

  public static final String NAME = "WriteProcessOperator";
  private static final Logger LOG = LoggerFactory.getLogger(KeyedWriteProcessOperator.class);
  private KeyedWriteProcessFunction writeProcessFunction;

  private List<String> latestInstantList = new ArrayList<>(1);
  private transient ListState<String> latestInstantState;

  private List<Tuple3<String, List<WriteStatus>, Integer>> latestOutputResultList = new ArrayList<>(1);
  private transient ListState<Tuple3<String, List<WriteStatus>, Integer>> latestOutputResultState;

  public KeyedWriteProcessOperator(KeyedProcessFunction<String, HoodieRecord, Tuple3<String, List<WriteStatus>, Integer>> function) {
    super(function);
    this.writeProcessFunction = (KeyedWriteProcessFunction) function;
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // This super.snapshotState(context) triggers `writeProcessFunction.snapshotState()` method. which means the logic
    // below will be executed after `writeProcessFunction.snapshotState()` method.

    // If there is no data flows in `writeProcessFunction`, it will never send anything downstream. so, in order to make
    // sure each subtask will send a write status downstream, we implement this operator`s snapshotState() to mock empty
    // write status and send it downstream when there is no data flows in some subtask.
    super.snapshotState(context);
    Tuple3<String, List<WriteStatus>, Integer> outputResult;
    // make up an empty result and send downstream
    String instantTime = writeProcessFunction.getLatestInstant();
    if (!writeProcessFunction.hasRecordsIn() && instantTime != null) {
      outputResult = new Tuple3(instantTime, new ArrayList<WriteStatus>(), getRuntimeContext().getIndexOfThisSubtask());
      LOG.info("Mock empty writeStatus, subtaskId = [{}], instant = [{}]", getRuntimeContext().getIndexOfThisSubtask(), instantTime);
    } else {
      outputResult = writeProcessFunction.getLatestOutputResult();
    }

    // update state
    if (instantTime != null) {
      if (latestInstantList.isEmpty()) {
        latestInstantList.add(instantTime);
      } else {
        latestInstantList.set(0, instantTime);
      }
      latestInstantState.update(latestInstantList);
      LOG.info("Update latest instant [{}]", latestInstantList);

      if (latestOutputResultList.isEmpty()) {
        latestOutputResultList.add(outputResult);
      } else {
        latestOutputResultList.set(0, outputResult);
      }
      latestOutputResultState.update(latestOutputResultList);
      LOG.info("Update latest output result [{}]", outputResult);
    }

    output.collect(new StreamRecord<>(outputResult));
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // instantState
    ListStateDescriptor<String> latestInstantStateDescriptor = new ListStateDescriptor<String>("latestInstant", String.class);
    latestInstantState = context.getOperatorStateStore().getListState(latestInstantStateDescriptor);
    // recordState
    ListStateDescriptor<Tuple3<String, List<WriteStatus>, Integer>> latestOutputResultDescriptor =
            new ListStateDescriptor<>("latestOutputResult", new TypeHint<Tuple3<String, List<WriteStatus>, Integer>>() {}.getTypeInfo());
    latestOutputResultState = context.getOperatorStateStore().getListState(latestOutputResultDescriptor);

    if (context.isRestored()) {
      Iterator<String> latestInstantIterator = latestInstantState.get().iterator();
      if (latestInstantIterator.hasNext()) {
        String latestInstant = latestInstantIterator.next();
        if (StringUtils.isNullOrEmpty(latestInstant)) {
          LOG.info("KeyedWriteProcessOperator initializeState get latestInstant [{}] is empty !", latestInstant);
          return;
        }

        // get hudi instant status
        // HoodieFlinkEngineContext hoodieFlinkEngineContext =
        // new HoodieFlinkEngineContext(new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()), new FlinkTaskContextSupplier(getRuntimeContext()));
        // HoodieFlinkStreamer.Config cfg = (HoodieFlinkStreamer.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(hoodieFlinkEngineContext, StreamerUtil.getHoodieClientConfig(cfg));

        HoodieFlinkWriteClient writeClient = writeProcessFunction.getWriteClient();
        Option<HoodieInstant.State> instantTimeState = writeClient.getInstantTimeState(latestInstant);
        LOG.info("KeyedWriteProcessOperator initializeState get latestInstant [{}] state [{}]", latestInstant,instantTimeState);

        // maybe instant was archived and removed from hdfs
        if (instantTimeState.isPresent()) {
          switch (instantTimeState.get()) {
            // upsert success but commit failed
            case INFLIGHT:
              LOG.info("KeyedWriteProcessOperator initializeState get latestInstant [{}] state [{}] need to output data again.", latestInstant,instantTimeState);
              handleInflightState();
              break;
            // restored from a success checkpoint or a success savepoint
            case COMPLETED: break;
            default:break;
          }
        } else {
          Option<String> latestCompletedInstanTimeOpt = writeClient.getLatestCompletedInstanTime();
          String latestCompletedInstanTime = latestCompletedInstanTimeOpt.orElseGet(null);
          if (StringUtils.isNullOrEmpty(latestCompletedInstanTime) || Long.parseLong(latestCompletedInstanTime) < Long.parseLong(latestInstant)) {
            throw new RuntimeException("KeyedWriteProcessOperator initializeState get latestInstant is null ! latestCompletedInstanTime [" + latestCompletedInstanTime + "]");
          }
        }
      }
    }
    
  }

  private void handleInflightState() throws Exception {
    Iterator<Tuple3<String, List<WriteStatus>, Integer>> iterator = latestOutputResultState.get().iterator();
    if (iterator.hasNext()) {
      Tuple3<String, List<WriteStatus>, Integer> next = iterator.next();
      List<WriteStatus> writeStatuses = next.f1;
      writeStatuses.forEach(x -> {
        LOG.info("KeyedWriteProcessOperator initializeState output writeStatus [{}]",x);
      });
      output.collect(new StreamRecord<>(next));
    }

  }
}
