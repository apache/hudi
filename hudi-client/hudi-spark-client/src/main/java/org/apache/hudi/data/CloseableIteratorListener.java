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

import java.util.Iterator;

/**
 * Helper class for adding a spark task completion listener that will ensure the iterator is closed if it is an instance of {@link AutoCloseable}.
 * This is commonly used with {@link org.apache.hudi.common.util.collection.ClosableIterator} to ensure the resources are closed after the task completes.
 */
@Slf4j
public class CloseableIteratorListener implements TaskCompletionListener {
  private final Object iterator;

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
   * Closes the iterator if it also implements {@link AutoCloseable}, otherwise it is a no-op.
   *
   * @param context the spark context
   */
  @Override
  public void onTaskCompletion(TaskContext context) {
    if (iterator instanceof AutoCloseable) {
      try {
        ((AutoCloseable) iterator).close();
      } catch (Exception ex) {
        log.warn("Failed to properly close iterator", ex);
      }
    }
  }
}
