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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Wrapper of input records iterator
 */
public class SimpleHoodieMessageQueue<I, O> implements HoodieMessageQueue<I, O>, Iterable<O> {

  private static final Logger LOG = LogManager.getLogger(SimpleHoodieMessageQueue.class);
  private final Iterator<I> inputItr;
  private final InnerIterator innerIterator;
  private final Function<I, O> transformFunction;
  private final AtomicBoolean isWriteDone = new AtomicBoolean(false);
  private final AtomicLong recordNumber = new AtomicLong(0);

  public SimpleHoodieMessageQueue(Iterator<I> inputItr, Function<I, O> transformFunction) {
    this.inputItr = inputItr;
    this.transformFunction = transformFunction;
    this.innerIterator = new InnerIterator();
  }

  @Override
  public Iterator<O> iterator() {
    return this.innerIterator;
  }

  @Override
  public long size() {
    return this.recordNumber.get();
  }

  @Override
  public void insertRecord(I t) throws Exception {
    // no-op
  }

  @Override
  public Option<O> readNextRecord() {
    // no-op
    return null;
  }

  @Override
  public void markAsFailed(Throwable e) {
    // no-op
  }

  @Override
  public boolean isEmpty() {
    return !innerIterator.hasNext();
  }

  @Override
  public void seal() {
    isWriteDone.set(true);
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Iterator for the memory bounded queue.
   */
  private final class InnerIterator implements Iterator<O> {

    @Override
    public boolean hasNext() {
      return inputItr.hasNext();
    }

    @Override
    public O next() {

      if (isWriteDone.get()) {
        throw new IllegalStateException("Queue closed for getting new entries");
      }

      recordNumber.incrementAndGet();
      return transformFunction.apply(inputItr.next());
    }
  }
}
