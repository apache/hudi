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

package org.apache.hudi.sink.bucket.disruptor;

import org.apache.hudi.common.util.Functions;
import org.apache.hudi.disruptor.DisruptorData;
import org.apache.hudi.disruptor.DisruptorEvent;
import org.apache.hudi.disruptor.DisruptorEventHandler;

import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRowDataEventHandler implements DisruptorEventHandler<DisruptorData<RowData>> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkRowDataEventHandler.class);
  private final Functions.VoidFunction3<RowData, String, String> bufferRecordFunc;
  private boolean suicide = false;

  public FlinkRowDataEventHandler(Functions.VoidFunction3<RowData, String, String> bufferRecordFunc) {
    this.bufferRecordFunc = bufferRecordFunc;
  }

  @Override
  public void onEvent(DisruptorEvent<DisruptorData<RowData>> event, long sequence, boolean endOfBatch) throws Exception {
    if (event.getData() instanceof FlinkDisruptorWritePoison) {
      LOG.info("Eat Poison, suicide now!");
      this.suicide = true;
      return;
    }

    if (suicide && event.getData() instanceof FlinkDisruptorWriteAntidote) {
      LOG.info("Find antidote!");
      this.suicide = false;
      return;
    }

    FlinkDisruptorWriteData data = (FlinkDisruptorWriteData) event.getData();
    String fileID = data.getFileID();
    RowData record = data.getData();
    String partitionPath = data.getPartitionPath();
    bufferRecordFunc.apply(record, partitionPath, fileID);
    event.clear();
  }

  @Override
  public boolean isStoped() {
    return suicide;
  }
}
