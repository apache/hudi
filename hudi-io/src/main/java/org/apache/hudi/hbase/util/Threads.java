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

package org.apache.hudi.hbase.util;

import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Thread Utility
 */
@InterfaceAudience.Private
public class Threads {
  private static final Logger LOG = LoggerFactory.getLogger(Threads.class);

  public static final UncaughtExceptionHandler LOGGING_EXCEPTION_HANDLER =
      (t, e) -> LOG.warn("Thread:{} exited with Exception:{}", t, StringUtils.stringifyException(e));

  /**
   * Utility method that sets name, daemon status and starts passed thread.
   * @param t thread to run
   * @return Returns the passed Thread <code>t</code>.
   */
  public static <T extends Thread> T setDaemonThreadRunning(T t) {
    return setDaemonThreadRunning(t, t.getName());
  }

  /**
   * Utility method that sets name, daemon status and starts passed thread.
   * @param t thread to frob
   * @param name new name
   * @return Returns the passed Thread <code>t</code>.
   */
  public static <T extends Thread> T setDaemonThreadRunning(T t, String name) {
    return setDaemonThreadRunning(t, name, null);
  }

  /**
   * Utility method that sets name, daemon status and starts passed thread.
   * @param t thread to frob
   * @param name new name
   * @param handler A handler to set on the thread. Pass null if want to use default handler.
   * @return Returns the passed Thread <code>t</code>.
   */
  public static <T extends Thread> T setDaemonThreadRunning(T t, String name,
                                                            UncaughtExceptionHandler handler) {
    t.setName(name);
    if (handler != null) {
      t.setUncaughtExceptionHandler(handler);
    }
    t.setDaemon(true);
    t.start();
    return t;
  }

  /**
   * Shutdown passed thread using isAlive and join.
   * @param t Thread to shutdown
   */
  public static void shutdown(final Thread t) {
    shutdown(t, 0);
  }

  /**
   * Shutdown passed thread using isAlive and join.
   * @param joinwait Pass 0 if we're to wait forever.
   * @param t Thread to shutdown
   */
  public static void shutdown(final Thread t, final long joinwait) {
    if (t == null) return;
    while (t.isAlive()) {
      try {
        t.join(joinwait);
      } catch (InterruptedException e) {
        LOG.warn(t.getName() + "; joinwait=" + joinwait, e);
      }
    }
  }


  /**
   * @param t Waits on the passed thread to die dumping a threaddump every
   * minute while its up.
   * @throws InterruptedException
   */
  public static void threadDumpingIsAlive(final Thread t)
      throws InterruptedException {
    if (t == null) {
      return;
    }

    while (t.isAlive()) {
      t.join(60 * 1000);
      if (t.isAlive()) {
        printThreadInfo(System.out,
            "Automatic Stack Trace every 60 seconds waiting on " +
                t.getName());
      }
    }
  }

  /**
   * If interrupted, just prints out the interrupt on STDOUT, resets interrupt and returns
   * @param millis How long to sleep for in milliseconds.
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("sleep interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Sleeps for the given amount of time even if interrupted. Preserves
   * the interrupt status.
   * @param msToWait the amount of time to sleep in milliseconds
   */
  public static void sleepWithoutInterrupt(final long msToWait) {
    long timeMillis = System.currentTimeMillis();
    long endTime = timeMillis + msToWait;
    boolean interrupted = false;
    while (timeMillis < endTime) {
      try {
        Thread.sleep(endTime - timeMillis);
      } catch (InterruptedException ex) {
        interrupted = true;
      }
      timeMillis = System.currentTimeMillis();
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Create a new CachedThreadPool with a bounded number as the maximum
   * thread size in the pool.
   *
   * @param maxCachedThread the maximum thread could be created in the pool
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @param threadFactory the factory to use when creating new threads
   * @return threadPoolExecutor the cachedThreadPool with a bounded number
   * as the maximum thread size in the pool.
   */
  public static ThreadPoolExecutor getBoundedCachedThreadPool(int maxCachedThread, long timeout,
                                                              TimeUnit unit, ThreadFactory threadFactory) {
    ThreadPoolExecutor boundedCachedThreadPool =
        new ThreadPoolExecutor(maxCachedThread, maxCachedThread, timeout, unit,
            new LinkedBlockingQueue<>(), threadFactory);
    // allow the core pool threads timeout and terminate
    boundedCachedThreadPool.allowCoreThreadTimeOut(true);
    return boundedCachedThreadPool;
  }

  /** Sets an UncaughtExceptionHandler for the thread which logs the
   * Exception stack if the thread dies.
   */
  public static void setLoggingUncaughtExceptionHandler(Thread t) {
    t.setUncaughtExceptionHandler(LOGGING_EXCEPTION_HANDLER);
  }

  private interface PrintThreadInfoHelper {

    void printThreadInfo(PrintStream stream, String title);

  }

  private static class PrintThreadInfoLazyHolder {

    public static final PrintThreadInfoHelper HELPER = initHelper();

    private static PrintThreadInfoHelper initHelper() {
      Method method = null;
      try {
        // Hadoop 2.7+ declares printThreadInfo(PrintStream, String)
        method = ReflectionUtils.class.getMethod("printThreadInfo", PrintStream.class,
            String.class);
        method.setAccessible(true);
        final Method hadoop27Method = method;
        return new PrintThreadInfoHelper() {

          @Override
          public void printThreadInfo(PrintStream stream, String title) {
            try {
              hadoop27Method.invoke(null, stream, title);
            } catch (IllegalAccessException | IllegalArgumentException e) {
              throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
              throw new RuntimeException(e.getCause());
            }
          }
        };
      } catch (NoSuchMethodException e) {
        LOG.info(
            "Can not find hadoop 2.7+ printThreadInfo method, try hadoop hadoop 2.6 and earlier", e);
      }
      try {
        // Hadoop 2.6 and earlier declares printThreadInfo(PrintWriter, String)
        method = ReflectionUtils.class.getMethod("printThreadInfo", PrintWriter.class,
            String.class);
        method.setAccessible(true);
        final Method hadoop26Method = method;
        return new PrintThreadInfoHelper() {

          @Override
          public void printThreadInfo(PrintStream stream, String title) {
            try {
              hadoop26Method.invoke(null, new PrintWriter(
                  new OutputStreamWriter(stream, StandardCharsets.UTF_8)), title);
            } catch (IllegalAccessException | IllegalArgumentException e) {
              throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
              throw new RuntimeException(e.getCause());
            }
          }
        };
      } catch (NoSuchMethodException e) {
        LOG.warn("Cannot find printThreadInfo method. Check hadoop jars linked", e);
      }
      return null;
    }
  }

  /**
   * Print all of the thread's information and stack traces. Wrapper around Hadoop's method.
   *
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  public static void printThreadInfo(PrintStream stream, String title) {
    Preconditions.checkNotNull(PrintThreadInfoLazyHolder.HELPER,
        "Cannot find method. Check hadoop jars linked").printThreadInfo(stream, title);
  }

  /**
   * Checks whether any non-daemon thread is running.
   * @return true if there are non daemon threads running, otherwise false
   */
  public static boolean isNonDaemonThreadRunning() {
    AtomicInteger nonDaemonThreadCount = new AtomicInteger();
    Set<Thread> threads =  Thread.getAllStackTraces().keySet();
    threads.forEach(t -> {
      // Exclude current thread
      if (t.getId() != Thread.currentThread().getId() && !t.isDaemon()) {
        nonDaemonThreadCount.getAndIncrement();
        LOG.info("Non daemon thread {} is still alive", t.getName());
        LOG.info(printStackTrace(t));
      }
    });
    return nonDaemonThreadCount.get() > 0;
  }

  /*
    Print stack trace of the passed thread
   */
  public static String printStackTrace(Thread t) {
    StringBuilder sb = new StringBuilder();
    for (StackTraceElement frame: t.getStackTrace()) {
      sb.append("\n").append("    ").append(frame.toString());
    }
    return sb.toString();
  }
}
