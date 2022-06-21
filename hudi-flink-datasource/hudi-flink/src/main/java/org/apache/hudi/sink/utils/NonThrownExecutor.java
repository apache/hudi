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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * An executor service that catches all the throwable with logging.
 *
 * <p>A post-exception hook {@link ExceptionHook} can be defined on construction
 * or on each execution.
 */
public class NonThrownExecutor implements AutoCloseable {
  private final Logger logger;

  /**
   * A single-thread executor to handle all the asynchronous jobs.
   */
  private final ExecutorService executor;

  /**
   * Exception hook for post-exception handling.
   */
  @VisibleForTesting
  protected final ExceptionHook exceptionHook;

  /**
   * Flag saying whether to wait for the tasks finish on #close.
   */
  private final boolean waitForTasksFinish;

  @VisibleForTesting
  protected NonThrownExecutor(Logger logger, @Nullable ExceptionHook exceptionHook, boolean waitForTasksFinish) {
    this.executor = Executors.newSingleThreadExecutor();
    this.logger = logger;
    this.exceptionHook = exceptionHook;
    this.waitForTasksFinish = waitForTasksFinish;
  }

  public static Builder builder(Logger logger) {
    return new Builder(logger);
  }

  /**
   * Run the action in a loop.
   */
  public void execute(
      final ThrowingRunnable<Throwable> action,
      final String actionName,
      final Object... actionParams) {
    execute(action, this.exceptionHook, actionName, actionParams);
  }

  /**
   * Run the action in a loop.
   */
  public void execute(
      final ThrowingRunnable<Throwable> action,
      final ExceptionHook hook,
      final String actionName,
      final Object... actionParams) {
    executor.execute(wrapAction(action, hook, actionName, actionParams));
  }

  /**
   * Run the action in a loop and wait for completion.
   */
  public void executeSync(ThrowingRunnable<Throwable> action, String actionName, Object... actionParams) {
    try {
      executor.submit(wrapAction(action, this.exceptionHook, actionName, actionParams)).get();
    } catch (InterruptedException e) {
      handleException(e, this.exceptionHook, getActionString(actionName, actionParams));
    } catch (ExecutionException e) {
      // nonfatal exceptions are handled by wrapAction
      ExceptionUtils.rethrowIfFatalErrorOrOOM(e.getCause());
    }
  }

  @Override
  public void close() throws Exception {
    if (executor != null) {
      if (waitForTasksFinish) {
        executor.shutdown();
      } else {
        executor.shutdownNow();
      }
      // We do not expect this to actually block for long. At this point, there should
      // be very few task running in the executor, if any.
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
  }

  private <E extends Throwable> Runnable wrapAction(
      final ThrowingRunnable<E> action,
      final ExceptionHook hook,
      final String actionName,
      final Object... actionParams) {

    return () -> {
      final Supplier<String> actionString = getActionString(actionName, actionParams);
      try {
        action.run();
        logger.info("Executor executes action [{}] success!", actionString.get());
      } catch (Throwable t) {
        handleException(t, hook, actionString);
      }
    };
  }

  private void handleException(Throwable t, ExceptionHook hook, Supplier<String> actionString) {
    // if we have a JVM critical error, promote it immediately, there is a good
    // chance the
    // logging or job failing will not succeed any more
    ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
    final String errMsg = String.format("Executor executes action [%s] error", actionString.get());
    logger.error(errMsg, t);
    if (hook != null) {
      hook.apply(errMsg, t);
    }
  }

  private Supplier<String> getActionString(String actionName, Object... actionParams) {
    // avoid String.format before OOM rethrown
    return () -> String.format(actionName, actionParams);
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------
  public interface ExceptionHook {
    void apply(String errMsg, Throwable t);
  }

  /**
   * Builder for {@link NonThrownExecutor}.
   */
  public static class Builder {
    private final Logger logger;
    private ExceptionHook exceptionHook;
    private boolean waitForTasksFinish = false;

    private Builder(Logger logger) {
      this.logger = Objects.requireNonNull(logger);
    }

    public NonThrownExecutor build() {
      return new NonThrownExecutor(logger, exceptionHook, waitForTasksFinish);
    }

    public Builder exceptionHook(ExceptionHook exceptionHook) {
      this.exceptionHook = exceptionHook;
      return this;
    }

    public Builder waitForTasksFinish(boolean waitForTasksFinish) {
      this.waitForTasksFinish = waitForTasksFinish;
      return this;
    }
  }
}
