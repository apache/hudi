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

package org.apache.hudi.exception;

import org.apache.hudi.common.util.StringUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Util class for exception analysis.
 */
public final class ExceptionUtil {
  private ExceptionUtil() {
  }

  /**
   * Returns true if error message is contained in any nested exception to provided {@link Throwable}.
   */
  public static boolean validateErrorMsg(@Nonnull Throwable t, String errorMsg) {
    if (StringUtils.isNullOrEmpty(errorMsg)) {
      return false;
    }

    for (Throwable cause : getCauses(t)) {
      if (cause.getMessage() != null && cause.getMessage().contains(errorMsg)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns true if any nested exception is an instance of the provided exception class.
   */
  public static boolean isCausedBy(@Nonnull Throwable t, @Nonnull Class<? extends Throwable> clazz) {
    for (Throwable cause : getCauses(t)) {
      if (clazz.isInstance(cause)) {
        return true;
      }
    }

    return false;
  }

  /**
   * The throwable and its causes in order, stopping before an identity repeats. Suppressed exceptions are not causes.
   * A null throwable has no causes.
   */
  public static List<Throwable> getCauses(Throwable failure) {
    List<Throwable> causes = new ArrayList<>();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
      causes.add(cause);
    }
    return causes;
  }

  /** The innermost cause, or the last cause before an identity repeats; null for a null failure. */
  public static Throwable getRootCause(Throwable failure) {
    List<Throwable> causes = getCauses(failure);
    return causes.isEmpty() ? null : causes.get(causes.size() - 1);
  }

  /**
   * Throws the provided exception as-is when it is an {@link IOException} or
   * {@link RuntimeException}, otherwise wraps it in an {@link IOException}.
   */
  public static void throwAsIOExceptionOrRuntimeException(Throwable exception) throws IOException {
    if (exception instanceof IOException) {
      throw (IOException) exception;
    }
    if (exception instanceof RuntimeException) {
      throw (RuntimeException) exception;
    }
    throw new IOException(exception);
  }
}
