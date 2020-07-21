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

package org.apache.hudi.execution;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.LazyInsertIterable.HoodieInsertValueGenResult;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Consumes stream of hoodie records from in-memory queue and writes to one or more create-handles.
 */
public class CopyOnWriteInsertHandler<T extends HoodieRecordPayload>
    extends BoundedInMemoryQueueConsumer<HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> {

  protected HoodieWriteConfig config;
  protected String instantTime;
  protected HoodieTable<T> hoodieTable;
  protected String idPrefix;
  protected int numFilesWritten;
  protected SparkTaskContextSupplier sparkTaskContextSupplier;
  protected WriteHandleFactory<T> writeHandleFactory;

  protected final List<WriteStatus> statuses = new ArrayList<>();
  protected Map<String, HoodieWriteHandle> handles = new HashMap<>();

  public CopyOnWriteInsertHandler(
      HoodieWriteConfig config, String instantTime, HoodieTable<T> hoodieTable, String idPrefix,
      SparkTaskContextSupplier sparkTaskContextSupplier, WriteHandleFactory<T> writeHandleFactory) {
    this.config = config;
    this.instantTime = instantTime;
    this.hoodieTable = hoodieTable;
    this.idPrefix = idPrefix;
    this.numFilesWritten = 0;
    this.sparkTaskContextSupplier = sparkTaskContextSupplier;
    this.writeHandleFactory = writeHandleFactory;
  }

  @Override
  public void consumeOneRecord(HoodieInsertValueGenResult<HoodieRecord> payload) {
    final HoodieRecord insertPayload = payload.record;
    String partitionPath = insertPayload.getPartitionPath();
    HoodieWriteHandle handle = handles.get(partitionPath);
    // lazily initialize the handle, for the first time
    if (handle == null) {
      handle = writeHandleFactory.create(
          config, instantTime, hoodieTable, insertPayload.getPartitionPath(),
          idPrefix, sparkTaskContextSupplier);
      handles.put(partitionPath, handle);
    }

    if (!handle.canWrite(payload.record)) {
      // handle is full.
      statuses.add(handle.close());
      // Need to handle the rejected payload & open new handle
      handle = writeHandleFactory.create(
          config, instantTime, hoodieTable, insertPayload.getPartitionPath(),
          idPrefix, sparkTaskContextSupplier);
      handles.put(partitionPath, handle);
    }
    handle.write(insertPayload, payload.insertValue, payload.exception);
  }

  @Override
  public void finish() {
    for (HoodieWriteHandle handle : handles.values()) {
      statuses.add(handle.close());
    }
    handles.clear();
    assert statuses.size() > 0;
  }

  @Override
  public List<WriteStatus> getResult() {
    return statuses;
  }
}
