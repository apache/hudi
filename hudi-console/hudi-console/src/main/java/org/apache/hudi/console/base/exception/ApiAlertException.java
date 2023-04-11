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

package org.apache.hudi.console.base.exception;

import org.apache.hudi.console.base.domain.ResponseCode;

import java.util.Objects;

/**
 *
 *
 * <pre>
 * An exception message that needs to be notified to front-end,
 * usually a <strong>simple</strong>, clear</strong> message, e.g:
 * <p>1. Username already exists</p>
 * <p>2. No permission, please contact the administrator</p>
 * ...
 * </pre>
 */
public class ApiAlertException extends AbstractApiException {

  public ApiAlertException(String message) {
    super(message, ResponseCode.CODE_FAIL_ALERT);
  }

  public ApiAlertException(Throwable cause) {
    super(cause, ResponseCode.CODE_FAIL_ALERT);
  }

  public ApiAlertException(String message, Throwable cause) {
    super(message, cause, ResponseCode.CODE_FAIL_ALERT);
  }

  public static void throwIfNull(Object object, String errorMessage) {
    if (Objects.isNull(object)) {
      throw new ApiAlertException(errorMessage);
    }
  }

  public static void throwIfFalse(boolean expression, String errorMessage) {
    if (!expression) {
      throw new ApiAlertException(errorMessage);
    }
  }
}
