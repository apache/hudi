/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

/**
 * Executor for a BufferedIterator operation. This class takes as input the input iterator which
 * needs to be buffered, the runnable function that needs to be executed in the reader thread and
 * return the transformed output based on the writer function
 */
public class BufferedIteratorExecutor<I, O, E> {

  private static Logger logger = LogManager.getLogger(BufferedIteratorExecutor.class);

  // Executor service used for launching writer thread.
  final ExecutorService writerService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  final BufferedIterator<I, O> bufferedIterator;
  // Need to set current spark thread's TaskContext into newly launched thread so that new
  // thread can access
  // TaskContext properties.
  final TaskContext sparkThreadTaskContext;

  public BufferedIteratorExecutor(final HoodieWriteConfig hoodieConfig, final Iterator<I> inputItr,
      final Function<I, O> bufferedIteratorTransform,
      final ExecutorService writerService) {
    this.sparkThreadTaskContext = TaskContext.get();
    this.writerService = writerService;
    this.bufferedIterator = new BufferedIterator<>(inputItr, hoodieConfig.getWriteBufferLimitBytes(),
        bufferedIteratorTransform);
  }

  /**
   * Starts buffering and executing the writer function
   */
  public Future<E> start(Function<BufferedIterator, E> writerFunction) {
    try {
      Future<E> future = writerService.submit(
          () -> {
            logger.info("starting hoodie writer thread");
            // Passing parent thread's TaskContext to newly launched thread for it to access original TaskContext
            // properties.
            TaskContext$.MODULE$.setTaskContext(sparkThreadTaskContext);
            try {
              E result = writerFunction.apply(bufferedIterator);
              logger.info("hoodie write is done; notifying reader thread");
              return result;
            } catch (Exception e) {
              logger.error("error writing hoodie records", e);
              bufferedIterator.markAsFailed(e);
              throw e;
            }
          });
      bufferedIterator.startBuffering();
      return future;
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  public boolean isRemaining() {
    return bufferedIterator.hasNext();
  }
}
