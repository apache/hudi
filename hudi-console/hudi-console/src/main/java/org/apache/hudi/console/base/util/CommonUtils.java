/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.console.base.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public final class CommonUtils implements Serializable {

  private CommonUtils() {}

  /**
   * is empty
   *
   * @param objs handle obj
   * @return Boolean
   * @see <b>Returns true if the object is Null, returns true if the size of the collection is 0,
   *     and returns true if the iterator has no next</b>
   * @since 1.0
   */
  public static Boolean isEmpty(Object... objs) {

    if (objs == null) {
      return Boolean.TRUE;
    }

    if (objs.length == 0) {
      return Boolean.TRUE;
    }

    for (Object obj : objs) {
      if (obj == null) {
        return true;
      }

      // char sequence
      if ((obj instanceof CharSequence) && "".equals(obj.toString().trim())) {
        return true;
      }
      // collection
      if (obj instanceof Collection) {
        if (((Collection<?>) obj).isEmpty()) {
          return true;
        }
      }
      // map
      if (obj instanceof Map) {
        if (((Map<?, ?>) obj).isEmpty()) {
          return true;
        }
      }

      if (obj instanceof Iterable) {
        if (!((Iterable<?>) obj).iterator().hasNext()) {
          return true;
        }
      }

      // iterator
      if (obj instanceof Iterator) {
        if (!((Iterator<?>) obj).hasNext()) {
          return true;
        }
      }

      // file
      if (obj instanceof File) {
        if (!((File) obj).exists()) {
          return true;
        }
      }

      if ((obj instanceof Object[]) && ((Object[]) obj).length == 0) {
        return true;
      }
    }

    return false;
  }

  public static void notNull(Object obj, String message) {
    if (obj == null) {
      throw new NullPointerException(message);
    }
  }

  public static void notNull(Object obj) {
    if (obj == null) {
      throw new NullPointerException("this argument must not be null");
    }
  }

  public static void required(Boolean expression) {
    if (!expression) {
      throw new IllegalArgumentException();
    }
  }

  public static void required(Boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException("requirement failed: " + message);
    }
  }

  public static Integer getPid() {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    String name = runtime.getName();
    try {
      return Integer.parseInt(name.substring(0, name.indexOf('@')));
    } catch (Exception e) {
      return -1;
    }
  }

  public static boolean isLegalUrl(String url) {
    try {
      new URI(url);
      return true;
    } catch (Exception ignored) {
      return false;
    }
  }

  public static String stringifyException(Throwable e) {
    if (e == null) {
      return "(null)";
    }
    try {
      StringWriter writer = new StringWriter();
      PrintWriter print = new PrintWriter(writer);
      e.printStackTrace(print);
      print.close();
      writer.close();
      return writer.toString();
    } catch (Exception exp) {
      return e.getClass().getName() + " (error while printing stack trace)";
    }
  }

  public static String formatFullTime(LocalDateTime localDateTime) {
    String fullFormat = "yyyy-MM-dd HH:mm:ss";
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(fullFormat);
    return localDateTime.format(dateTimeFormatter);
  }
}
