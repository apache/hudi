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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.HoodieLazyInsertIterable.HoodieInsertValueGenResult;
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
public class CopyOnWriteInsertHandler<T>
    extends BoundedInMemoryQueueConsumer<HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> {

  private HoodieWriteConfig config;
  private String instantTime;
  private boolean areRecordsSorted;
  private HoodieTable hoodieTable;
  private String idPrefix;
  private TaskContextSupplier taskContextSupplier;
  private WriteHandleFactory writeHandleFactory;

  private final List<WriteStatus> statuses = new ArrayList<>();
  // Stores the open HoodieWriteHandle for each table partition path
  // If the records are consumed in order, there should be only one open handle in this mapping.
  // Otherwise, there may be multiple handles.
  private Map<String, HoodieWriteHandle> handles = new HashMap<>();

  public CopyOnWriteInsertHandler(HoodieWriteConfig config, String instantTime,
                                  boolean areRecordsSorted, HoodieTable hoodieTable, String idPrefix,
                                  TaskContextSupplier taskContextSupplier,
                                  WriteHandleFactory writeHandleFactory) {
    this.config = config;
    this.instantTime = instantTime;
    this.areRecordsSorted = areRecordsSorted;
    this.hoodieTable = hoodieTable;
    this.idPrefix = idPrefix;
    this.taskContextSupplier = taskContextSupplier;
    this.writeHandleFactory = writeHandleFactory;
  }

  @Override
  public void consumeOneRecord(HoodieInsertValueGenResult<HoodieRecord> genResult) {
    final HoodieRecord record = genResult.getResult();
    String partitionPath = record.getPartitionPath();
    HoodieWriteHandle<?,?,?,?> handle = handles.get(partitionPath);
    if (handle == null) {
      // If the records are sorted, this means that we encounter a new partition path
      // and the records for the previous partition path are all written,
      // so we can safely closely existing open handle to reduce memory footprint.
      if (areRecordsSorted) {
        closeOpenHandles();
      }
      // Lazily initialize the handle, for the first time
      handle = writeHandleFactory.create(config, instantTime, hoodieTable,
          record.getPartitionPath(), idPrefix, taskContextSupplier);
      handles.put(partitionPath, handle);
    }

    if (!handle.canWrite(genResult.getResult())) {
      // Handle is full. Close the handle and add the WriteStatus
      statuses.addAll(handle.close());
      // Open new handle
      handle = writeHandleFactory.create(config, instantTime, hoodieTable,
          record.getPartitionPath(), idPrefix, taskContextSupplier);
      handles.put(partitionPath, handle);
    }
    handle.write(record, genResult.schema, new TypedProperties(genResult.props));
  }

  @Override
  public void finish() {
    closeOpenHandles();
    assert statuses.size() > 0;
  }

  @Override
  public List<WriteStatus> getResult() {
    return statuses;
  }

  private void closeOpenHandles() {
    for (HoodieWriteHandle<?,?,?,?> handle : handles.values()) {
      statuses.addAll(handle.close());
    }
    handles.clear();
  }
}
