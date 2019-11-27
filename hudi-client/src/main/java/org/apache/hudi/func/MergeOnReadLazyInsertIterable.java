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

package org.apache.hudi.func;

import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new log files.
 */
public class MergeOnReadLazyInsertIterable<T extends HoodieRecordPayload> extends CopyOnWriteLazyInsertIterable<T> {

  public MergeOnReadLazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
      String commitTime, HoodieTable<T> hoodieTable, String idPfx) {
    super(sortedRecordItr, config, commitTime, hoodieTable, idPfx);
  }

  @Override
  protected CopyOnWriteInsertHandler getInsertHandler() {
    return new MergeOnReadInsertHandler();
  }

  protected class MergeOnReadInsertHandler extends CopyOnWriteInsertHandler {

    @Override
    protected void consumeOneRecord(HoodieInsertValueGenResult<HoodieRecord> payload) {
      final HoodieRecord insertPayload = payload.record;
      List<WriteStatus> statuses = new ArrayList<>();
      // lazily initialize the handle, for the first time
      if (handle == null) {
        handle = new HoodieAppendHandle(hoodieConfig, commitTime, hoodieTable, getNextFileId(idPrefix));
      }
      if (handle.canWrite(insertPayload)) {
        // write the payload, if the handle has capacity
        handle.write(insertPayload, payload.insertValue, payload.exception);
      } else {
        // handle is full.
        handle.close();
        statuses.add(handle.getWriteStatus());
        // Need to handle the rejected payload & open new handle
        handle = new HoodieAppendHandle(hoodieConfig, commitTime, hoodieTable, getNextFileId(idPrefix));
        handle.write(insertPayload, payload.insertValue, payload.exception); // we should be able to write 1 payload.
      }
    }
  }

}
