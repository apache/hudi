/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.ThrowingRunnable;
import org.apache.hudi.adapter.MailboxExecutorAdapter;
import org.apache.hudi.adapter.RateLimiterAdapter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;

/**
 *  A separate thread to process splits. Can control read rate by FlinkOptions.READ_RATE_LIMIT
 */
public class SplitProcessThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(SplitProcessThread.class);
  private volatile boolean running = true;
  private transient Queue<MergeOnReadInputSplit> splits;

  private final MailboxExecutorAdapter executor;

  private MergeOnReadInputFormat format;

  private transient SourceFunction.SourceContext<RowData> sourceContext;

  private transient ThrowingRunnable<? extends Exception> exceptionHandler;
  private transient Option<RateLimiterAdapter> rateLimiter;

  public SplitProcessThread(Queue<MergeOnReadInputSplit> splits, MailboxExecutorAdapter executor, MergeOnReadInputFormat format,
                            SourceFunction.SourceContext<RowData> sourceContext, ThrowingRunnable<? extends Exception> exceptionHandler,
                            int indexOfThisSubtask, int numberOfParallelSubtasks) {
    setName(String.format("split-process-thread (%s/%s)", indexOfThisSubtask + 1, numberOfParallelSubtasks));
    this.splits = splits;
    this.executor = executor;
    this.format = format;
    this.sourceContext = sourceContext;
    this.exceptionHandler = exceptionHandler;
    long rate = format.getConf().getLong(FlinkOptions.READ_RATE_LIMIT);
    this.rateLimiter = rate > 0 ? Option.of(RateLimiterAdapter.create(rate)) : Option.empty();
  }

  @Override
  public void run() {
    while (running) {
      try {
        processSplits();
      } catch (IOException e) {
        running = false;
        LOG.error("Process splits wrong", e);
        executor.execute(exceptionHandler, "process splits wrong");
        throw new RuntimeException(e);
      }
    }
  }

  private void processSplits() throws IOException {
    MergeOnReadInputSplit split = splits.peek();
    if (split == null) {
      return;
    }
    // 1. open a fresh new input split and start reading as mini-batch
    // 2. if the input split has remaining records to read, switches to another runnable to handle
    // 3. if the input split reads to the end, close the format and remove the split from the queue #splits
    // 4. for each runnable, reads at most #MINI_BATCH_SIZE number of records
    if (format.isClosed()) {
      // This log is important to indicate the consuming process,
      // there is only one log message for one data bucket.
      LOG.info("Processing input split : {}", split);
      format.open(split);
    }
    consume(split);
  }

  /**
   * Consumes one record for the given input split {@code split}.
   *
   * <p>Note: close the input format and remove the input split for the queue {@link #splits}
   * if the split reads to the end.
   *
   * @param split The input split
   */
  private void consume(MergeOnReadInputSplit split) throws IOException {
    if (!format.reachedEnd()) {
      sourceContext.collect(format.nextRecord(null));
      split.consume();
      if (rateLimiter.isPresent()) {
        rateLimiter.get().acquire();
      }
    } else {
      // close the input format
      format.close();
      // remove the split
      splits.poll();
    }
  }

  public void shutdown() {
    this.running = false;
  }

}
