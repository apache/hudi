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

/**
 * Util class for exception analysis.
 */
public final class ExceptionUtil {
  private ExceptionUtil() {
  }

  /**
   * Returns true if error message is contained in any nested exception of provided {@link Throwable}.
   */
  public static boolean validateErrorMsg(@Nonnull Throwable t, String errorMsg) {
    if (StringUtils.isNullOrEmpty(errorMsg)) {
      return false;
    }

    Throwable cause = t;
    while (cause != null) {
      if (cause.getMessage().contains(errorMsg)) {
        return true;
      }
      cause = cause.getCause();
    }

    return false;
  }
}
