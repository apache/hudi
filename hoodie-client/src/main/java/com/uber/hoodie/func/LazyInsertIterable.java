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
import com.uber.hoodie.io.HoodieCreateHandle;
import com.uber.hoodie.io.HoodieIOHandle;
import com.uber.hoodie.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath, into new
 * files.
 */
public class LazyInsertIterable<T extends HoodieRecordPayload> extends
    LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {

  private static Logger logger = LogManager.getLogger(LazyInsertIterable.class);
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

  @Override
  protected List<WriteStatus> computeNext() {
    // Need to set current spark thread's TaskContext into newly launched thread so that new thread can access
    // TaskContext properties.
    final TaskContext sparkThreadTaskContext = TaskContext.get();
    // Executor service used for launching writer thread.
    final ExecutorService writerService = Executors.newFixedThreadPool(1);
    try {
      // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
      final BufferedIterator<T, HoodieRecord<T>> bufferedIterator =
          new BufferedIterator<>(inputItr, hoodieConfig.getWriteBufferLimitBytes(),
              HoodieIOHandle.createHoodieWriteSchema(hoodieConfig));
      Future<List<WriteStatus>> writerResult =
          writerService.submit(
              () -> {
                logger.info("starting hoodie writer thread");
                // Passing parent thread's TaskContext to newly launched thread for it to access original TaskContext
                // properties.
                TaskContext$.MODULE$.setTaskContext(sparkThreadTaskContext);
                List<WriteStatus> statuses = new LinkedList<>();
                try {
                  statuses.addAll(handleWrite(bufferedIterator));
                  logger.info("hoodie write is done; notifying reader thread");
                  return statuses;
                } catch (Exception e) {
                  logger.error("error writing hoodie records", e);
                  bufferedIterator.markAsFailed(e);
                  throw e;
                }
              });
      // Buffering records into internal buffer. This can throw exception either if reading records from spark fails or
      // if writing buffered records into parquet file fails.
      bufferedIterator.startBuffering();
      logger.info("waiting for hoodie write to finish");
      final List<WriteStatus> result = writerResult.get();
      assert result != null && !result.isEmpty() && !bufferedIterator.hasNext();
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      writerService.shutdownNow();
    }
  }

  private List<WriteStatus> handleWrite(final BufferedIterator<T, HoodieRecord<T>> bufferedIterator) {
    List<WriteStatus> statuses = new ArrayList<>();
    while (bufferedIterator.hasNext()) {
      final BufferedIterator.BufferedIteratorPayload<HoodieRecord<T>> payload = bufferedIterator.next();

      // clean up any partial failures
      if (!partitionsCleaned.contains(payload.record.getPartitionPath())) {
        // This insert task could fail multiple times, but Spark will faithfully retry with
        // the same data again. Thus, before we open any files under a given partition, we
        // first delete any files in the same partitionPath written by same Spark partition
        HoodieIOHandle.cleanupTmpFilesFromCurrentCommit(hoodieConfig,
            commitTime,
            payload.record.getPartitionPath(),
            TaskContext.getPartitionId(),
            hoodieTable);
        partitionsCleaned.add(payload.record.getPartitionPath());
      }

      // lazily initialize the handle, for the first time
      if (handle == null) {
        handle =
            new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable, payload.record.getPartitionPath());
      }

      if (handle.canWrite(payload.record)) {
        // write the payload, if the handle has capacity
        handle.write(payload.record, payload.insertValue, payload.exception);
      } else {
        // handle is full.
        statuses.add(handle.close());
        // Need to handle the rejected payload & open new handle
        handle =
            new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable, payload.record.getPartitionPath());
        handle.write(payload.record, payload.insertValue, payload.exception); // we should be able to write 1 payload.
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
