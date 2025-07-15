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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.io.HoodieWriteMergeHandle;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;

/**
 * Helper to read records from previous version of base file and run Merge.
 */
public abstract class BaseMergeHelper {

  /**
   * Read records from previous version of base file and merge.
   * @param table Hoodie Table
   * @param mergeHandle Merge Handle
   * @throws IOException in case of error
   */
  public abstract void runMerge(HoodieTable<?, ?, ?, ?> table, HoodieWriteMergeHandle<?, ?, ?, ?> mergeHandle) throws IOException;

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  protected static class UpdateHandler implements HoodieConsumer<HoodieRecord, Void> {

    private final HoodieWriteMergeHandle mergeHandle;

    protected UpdateHandler(HoodieWriteMergeHandle mergeHandle) {
      this.mergeHandle = mergeHandle;
    }

    @Override
    public void consume(HoodieRecord record) {
      mergeHandle.write(record);
    }

    @Override
    public Void finish() {
      mergeHandle.close();
      return null;
    }
  }
}
