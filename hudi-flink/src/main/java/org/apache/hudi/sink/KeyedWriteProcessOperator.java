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

package org.apache.hudi.sink;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Operator helps to mock empty write results and deliver downstream when no data flow in some subtask.
 */
public class KeyedWriteProcessOperator extends KeyedProcessOperator<String, HoodieRecord, Tuple3<String, List<WriteStatus>, Integer>> {

  public static final String NAME = "WriteProcessOperator";
  private static final Logger LOG = LoggerFactory.getLogger(KeyedWriteProcessOperator.class);
  private KeyedWriteProcessFunction writeProcessFunction;

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
    // write status and send it downstream when there is no data flows in some subtasks.
    super.snapshotState(context);

    // make up an empty result and send downstream
    if (!writeProcessFunction.hasRecordsIn() && writeProcessFunction.getLatestInstant() != null) {
      String instantTime = writeProcessFunction.getLatestInstant();
      LOG.info("Mock empty writeStatus, subtaskId = [{}], instant = [{}]", getRuntimeContext().getIndexOfThisSubtask(), instantTime);
      output.collect(new StreamRecord<>(new Tuple3(instantTime, new ArrayList<WriteStatus>(), getRuntimeContext().getIndexOfThisSubtask())));
    }
  }
}
