/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Coordinator executor that executes the tasks asynchronously, it fails the job
 * for any task exceptions.
 *
 * <p>We need this because the coordinator methods are called by
 * the Job Manager's main thread (mailbox thread), executes the methods asynchronously
 * to avoid blocking the main thread.
 */
public class CoordinatorExecutor extends NonThrownExecutor {
  private final OperatorCoordinator.Context context;

  public CoordinatorExecutor(OperatorCoordinator.Context context, Logger logger) {
    super(logger);
    this.context = context;
  }

  @Override
  protected void exceptionHook(String actionString, Throwable t) {
    this.context.failJob(new HoodieException(actionString, t));
  }

  @Override
  public void close() throws Exception {
    // wait for the remaining tasks to finish.
    executor.shutdown();
    // We do not expect this to actually block for long. At this point, there should
    // be very few task running in the executor, if any.
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
  }
}
