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
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.func.payload.AbstractBufferedIteratorPayload;
import com.uber.hoodie.func.payload.HoodieRecordBufferedIteratorPayload;
import com.uber.hoodie.io.HoodieCreateHandle;
import com.uber.hoodie.io.HoodieIOHandle;
import com.uber.hoodie.table.HoodieTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new
 * files.
 */
public class LazyInsertIterable<T extends HoodieRecordPayload> extends
    LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {

  private final HoodieWriteConfig hoodieConfig;
  private final String commitTime;
  private final HoodieTable<T> hoodieTable;
  private Set<String> partitionsCleaned;
  private HoodieCreateHandle handle;

  public LazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
      String commitTime, HoodieTable<T> hoodieTable) {
    super(sortedRecordItr);
    this.partitionsCleaned = new HashSet<>();
    this.hoodieConfig = config;
    this.commitTime = commitTime;
    this.hoodieTable = hoodieTable;
  }

  @Override
  protected void start() {
  }

  /**
   * Transformer function to help transform a HoodieRecord. This transformer is used by BufferedIterator to offload some
   * expensive operations of transformation to the reader thread.
   * @param schema
   * @param <T>
   * @return
   */
  public static <T extends HoodieRecordPayload> Function<HoodieRecord<T>, AbstractBufferedIteratorPayload>
      bufferedItrPayloadTransform(Schema schema) {
    return (hoodieRecord) -> new HoodieRecordBufferedIteratorPayload(hoodieRecord, schema);
  }

  @Override
  protected List<WriteStatus> computeNext() {
    // Executor service used for launching writer thread.
    final ExecutorService writerService = Executors.newFixedThreadPool(1);
    try {
      Function<BufferedIterator, List<WriteStatus>> function = (bufferedIterator) -> {
        List<WriteStatus> statuses = new LinkedList<>();
        statuses.addAll(handleWrite(bufferedIterator));
        return statuses;
      };
      BufferedIteratorExecutor<HoodieRecord<T>, AbstractBufferedIteratorPayload, List<WriteStatus>>
          bufferedIteratorExecutor = new BufferedIteratorExecutor(hoodieConfig, inputItr,
          bufferedItrPayloadTransform(HoodieIOHandle.createHoodieWriteSchema(hoodieConfig)),
              writerService);
      Future<List<WriteStatus>> writerResult = bufferedIteratorExecutor.start(function);
      final List<WriteStatus> result = writerResult.get();
      assert result != null && !result.isEmpty() && !bufferedIteratorExecutor.isRemaining();
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      writerService.shutdownNow();
    }
  }

  private List<WriteStatus> handleWrite(
      final BufferedIterator<HoodieRecord<T>, AbstractBufferedIteratorPayload> bufferedIterator) {
    List<WriteStatus> statuses = new ArrayList<>();
    while (bufferedIterator.hasNext()) {
      final HoodieRecordBufferedIteratorPayload payload = (HoodieRecordBufferedIteratorPayload) bufferedIterator
          .next();
      final HoodieRecord insertPayload = (HoodieRecord) payload.getInputPayload();
      // clean up any partial failures
      if (!partitionsCleaned
          .contains(insertPayload.getPartitionPath())) {
        // This insert task could fail multiple times, but Spark will faithfully retry with
        // the same data again. Thus, before we open any files under a given partition, we
        // first delete any files in the same partitionPath written by same Spark partition
        HoodieIOHandle.cleanupTmpFilesFromCurrentCommit(hoodieConfig, commitTime, insertPayload.getPartitionPath(),
            TaskContext.getPartitionId(), hoodieTable);
        partitionsCleaned.add(insertPayload.getPartitionPath());
      }

      // lazily initialize the handle, for the first time
      if (handle == null) {
        handle = new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable, insertPayload.getPartitionPath());
      }

      if (handle.canWrite(((HoodieRecord) payload.getInputPayload()))) {
        // write the payload, if the handle has capacity
        handle.write(insertPayload, (Optional<IndexedRecord>) payload.getOutputPayload(), payload.exception);
      } else {
        // handle is full.
        statuses.add(handle.close());
        // Need to handle the rejected payload & open new handle
        handle = new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable, insertPayload.getPartitionPath());
        handle.write(insertPayload,
            (Optional<IndexedRecord>) payload.getOutputPayload(),
            payload.exception); // we should be able to write 1 payload.
      }
    }

    // If we exited out, because we ran out of records, just close the pending handle.
    if (!bufferedIterator.hasNext()) {
      if (handle != null) {
        statuses.add(handle.close());
      }
    }

    assert statuses.size() > 0 && !bufferedIterator.hasNext(); // should never return empty statuses
    return statuses;
  }

  @Override
  protected void end() {

  }
}
