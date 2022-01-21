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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;

@InterfaceAudience.Private
public class ReflectionUtils {
  @SuppressWarnings("unchecked")
  public static <T> T instantiateWithCustomCtor(String className,
                                                Class<? >[] ctorArgTypes, Object[] ctorArgs) {
    try {
      Class<? extends T> resultType = (Class<? extends T>) Class.forName(className);
      Constructor<? extends T> ctor = resultType.getDeclaredConstructor(ctorArgTypes);
      return instantiate(className, ctor, ctorArgs);
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
          "Unable to find " + className, e);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "Unable to find suitable constructor for class " + className, e);
    }
  }

  private static <T> T instantiate(final String className, Constructor<T> ctor, Object[] ctorArgs) {
    try {
      ctor.setAccessible(true);
      return ctor.newInstance(ctorArgs);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException(
          "Unable to access specified class " + className, e);
    } catch (InstantiationException e) {
      throw new UnsupportedOperationException(
          "Unable to instantiate specified class " + className, e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Constructor threw an exception for " + className, e);
    }
  }

  public static <T> T newInstance(Class<T> type, Object... params) {
    return instantiate(type.getName(), findConstructor(type, params), params);
  }

  @SuppressWarnings("unchecked")
  public static <T> Constructor<T> findConstructor(Class<T> type, Object... paramTypes) {
    Constructor<T>[] constructors = (Constructor<T>[]) type.getDeclaredConstructors();
    for (Constructor<T> ctor : constructors) {
      Class<?>[] ctorParamTypes = ctor.getParameterTypes();
      if (ctorParamTypes.length != paramTypes.length) {
        continue;
      }

      boolean match = true;
      for (int i = 0; i < ctorParamTypes.length && match; ++i) {
        Class<?> paramType = paramTypes[i].getClass();
        match = (!ctorParamTypes[i].isPrimitive()) ? ctorParamTypes[i].isAssignableFrom(paramType) :
            ((int.class.equals(ctorParamTypes[i]) && Integer.class.equals(paramType)) ||
                (long.class.equals(ctorParamTypes[i]) && Long.class.equals(paramType)) ||
                (double.class.equals(ctorParamTypes[i]) && Double.class.equals(paramType)) ||
                (char.class.equals(ctorParamTypes[i]) && Character.class.equals(paramType)) ||
                (short.class.equals(ctorParamTypes[i]) && Short.class.equals(paramType)) ||
                (boolean.class.equals(ctorParamTypes[i]) && Boolean.class.equals(paramType)) ||
                (byte.class.equals(ctorParamTypes[i]) && Byte.class.equals(paramType)));
      }

      if (match) {
        return ctor;
      }
    }
    throw new UnsupportedOperationException(
        "Unable to find suitable constructor for class " + type.getName());
  }

  /* synchronized on ReflectionUtils.class */
  private static long previousLogTime = 0;
  private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  /**
   * Log the current thread stacks at INFO level.
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last
   */
  public static void logThreadInfo(Logger log,
                                   String title,
                                   long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        long now = System.currentTimeMillis();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        try {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          printThreadInfo(new PrintStream(buffer, false, "UTF-8"), title);
          log.info(buffer.toString(Charset.defaultCharset().name()));
        } catch (UnsupportedEncodingException ignored) {
          log.warn("Could not write thread info about '" + title +
              "' due to a string encoding issue.");
        }
      }
    }
  }

  /**
   * Print all of the thread's information and stack traces.
   *
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  private static void printThreadInfo(PrintStream stream,
                                      String title) {
    final int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (long tid: threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " +
          getTaskName(info.getThreadId(),
              info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state);
      stream.println("  Blocked count: " + info.getBlockedCount());
      stream.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime());
        stream.println("  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else  if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName());
        stream.println("  Blocked by " +
            getTaskName(info.getLockOwnerId(),
                info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame: info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  /**
   * Get and invoke the target method from the given object with given parameters
   * @param obj the object to get and invoke method from
   * @param methodName the name of the method to invoke
   * @param params the parameters for the method to invoke
   * @return the return value of the method invocation
   */
  public static Object invokeMethod(Object obj, String methodName, Object... params) {
    Method m;
    try {
      m = obj.getClass().getMethod(methodName, getParameterTypes(params));
      m.setAccessible(true);
      return m.invoke(obj, params);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException("Cannot find specified method " + methodName, e);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException("Unable to access specified method " + methodName, e);
    } catch (IllegalArgumentException e) {
      throw new UnsupportedOperationException("Illegal arguments supplied for method " + methodName,
          e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException("Method threw an exception for " + methodName, e);
    }
  }

  private static Class<?>[] getParameterTypes(Object[] params) {
    Class<?>[] parameterTypes = new Class<?>[params.length];
    for (int i = 0; i < params.length; i++) {
      parameterTypes[i] = params[i].getClass();
    }
    return parameterTypes;
  }

}
