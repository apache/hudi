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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * A mock {@link NonThrownExecutor} that executes the actions synchronously.
 */
@Slf4j
public class MockCoordinatorExecutor extends NonThrownExecutor {

  public MockCoordinatorExecutor(OperatorCoordinator.Context context) {
    super(log, null, (errMsg, t) -> context.failJob(new HoodieException(errMsg, t)), true);
  }

  @Override
  public void execute(
      ThrowingRunnable<Throwable> action,
      ExceptionHook hook,
      String actionName,
      Object... actionParams) {
    final String actionString = String.format(actionName, actionParams);
    try {
      action.run();
      log.info("Executor executes action [{}] success!", actionString);
    } catch (Throwable t) {
      // if we have a JVM critical error, promote it immediately, there is a good
      // chance the
      // logging or job failing will not succeed anymore
      log.error("Executor executes action [{}] failed.", actionString, t);
      ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
      final String errMsg = String.format("Executor executes action [%s] error", actionString);
      if (hook != null) {
        hook.apply(errMsg, t);
      }
    }
  }
}
