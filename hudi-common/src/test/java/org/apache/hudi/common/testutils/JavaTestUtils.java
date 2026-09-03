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

package org.apache.hudi.common.testutils;

public class JavaTestUtils {

  /**
   * Return true if the exception or its nested cause contains the exception message.
   */
  public static boolean checkNestedExceptionContains(Throwable t, String errorMsg) {
    Throwable throwable = t;
    boolean res = false;
    while (throwable != null) {
      // String.valueOf rather than getMessage().contains: a null message anywhere in the chain would
      // otherwise NPE here and lose the failure the caller was trying to assert on. A TimeoutException
      // raised before its condition ever threw is one such case, and NPEs in a chain are another.
      if (String.valueOf(throwable.getMessage()).contains(errorMsg)) {
        res = true;
        break;
      }
      throwable = throwable.getCause();
    }
    return res;
  }
}
