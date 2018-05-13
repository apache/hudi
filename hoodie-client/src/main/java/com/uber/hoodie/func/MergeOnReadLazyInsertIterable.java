/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.func;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.HoodieAppendHandle;
import com.uber.hoodie.table.HoodieTable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.IndexedRecord;
import scala.Tuple2;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new
 * log files.
 */
public class MergeOnReadLazyInsertIterable<T extends HoodieRecordPayload> extends
    CopyOnWriteLazyInsertIterable<T> {

  public MergeOnReadLazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
      String commitTime, HoodieTable<T> hoodieTable) {
    super(sortedRecordItr, config, commitTime, hoodieTable);
  }

  @Override
  protected CopyOnWriteInsertHandler getInsertHandler() {
    return new MergeOnReadInsertHandler();
  }

  protected class MergeOnReadInsertHandler extends CopyOnWriteInsertHandler {

    @Override
    protected void consumeOneRecord(Tuple2<HoodieRecord<T>, Optional<IndexedRecord>> payload) {
      final HoodieRecord insertPayload = payload._1();
      List<WriteStatus> statuses = new ArrayList<>();
      // lazily initialize the handle, for the first time
      if (handle == null) {
        handle = new HoodieAppendHandle(hoodieConfig, commitTime, hoodieTable);
      }
      if (handle.canWrite(insertPayload)) {
        // write the payload, if the handle has capacity
        handle.write(insertPayload, payload._2);
      } else {
        // handle is full.
        handle.close();
        statuses.add(handle.getWriteStatus());
        // Need to handle the rejected payload & open new handle
        handle = new HoodieAppendHandle(hoodieConfig, commitTime, hoodieTable);
        handle.write(insertPayload, payload._2); // we should be able to write 1 payload.
      }
    }
  }

}
