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

package org.apache.hudi.data;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskCompletionListener;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Helper class for adding a spark task completion listener that will ensure the iterator is closed if it is an instance of {@link AutoCloseable}.
 * This is commonly used with {@link org.apache.hudi.common.util.collection.ClosableIterator} to ensure the resources are closed after the task completes.
 */
@Slf4j
public class CloseableIteratorListener implements TaskCompletionListener {
  /** The iterator to close; null once it was released, so the task's listener list no longer keeps it reachable. */
  private Object iterator;

  private CloseableIteratorListener(Object iterator) {
    this.iterator = iterator;
  }

  public static <T> Iterator<T> addListener(Iterator<T> iterator) {
    TaskContext.get().addTaskCompletionListener(new CloseableIteratorListener(iterator));
    return iterator;
  }

  public static <T> scala.collection.Iterator<T> addListener(scala.collection.Iterator<T> iterator) {
    TaskContext.get().addTaskCompletionListener(new CloseableIteratorListener(iterator));
    return iterator;
  }

  /**
   * Like {@link #addListener(scala.collection.Iterator)}, for one of the many iterators a task may open one after another (one
   * per file of a scan): the returned iterator releases {@code iterator} once it is exhausted. When its {@code hasNext()} first
   * returns false it closes {@code iterator} (if it is {@link AutoCloseable}) and drops its own and the listener's reference to
   * it, so the iterator, and the row buffers it holds, do not stay reachable from the task's listeners until the task ends. An
   * iterator that is not read to its end is closed at task completion, as with {@link #addListener(scala.collection.Iterator)}.
   */
  public static <T> scala.collection.Iterator<T> addListenerReleasedOnExhaustion(scala.collection.Iterator<T> iterator) {
    CloseableIteratorListener listener = new CloseableIteratorListener(iterator);
    TaskContext.get().addTaskCompletionListener(listener);
    return new ReleasingIterator<>(iterator, listener);
  }

  /**
   * Closes the iterator if it also implements {@link AutoCloseable}, otherwise it is a no-op.
   *
   * @param context the spark context
   */
  @Override
  public void onTaskCompletion(TaskContext context) {
    release();
  }

  /** Closes the iterator if it is {@link AutoCloseable} and drops the reference to it; later calls are no-ops. */
  synchronized void release() {
    Object released = iterator;
    iterator = null;
    if (released instanceof AutoCloseable) {
      try {
        ((AutoCloseable) released).close();
      } catch (Exception ex) {
        log.warn("Failed to properly close iterator", ex);
      }
    }
  }

  /** The iterator {@link #addListenerReleasedOnExhaustion} returns: releases its delegate once exhausted or closed. */
  private static final class ReleasingIterator<T> extends scala.collection.AbstractIterator<T> implements Closeable {
    private scala.collection.Iterator<T> delegate;
    private CloseableIteratorListener listener;

    private ReleasingIterator(scala.collection.Iterator<T> delegate, CloseableIteratorListener listener) {
      this.delegate = delegate;
      this.listener = listener;
    }

    @Override
    public boolean hasNext() {
      if (delegate == null) {
        return false;
      }
      if (delegate.hasNext()) {
        return true;
      }
      close();
      return false;
    }

    @Override
    public T next() {
      if (delegate == null) {
        throw new NoSuchElementException("the iterator was exhausted and released");
      }
      return delegate.next();
    }

    @Override
    public void close() {
      if (listener != null) {
        listener.release();
      }
      delegate = null;
      listener = null;
    }
  }
}
