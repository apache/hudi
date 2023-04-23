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

/**
 *
 *
 * <pre>
 * An exception message that needs to be notified to front-end.
 * </pre>
 */
public abstract class AbstractApiException extends RuntimeException {

  private final long responseCode;

  protected AbstractApiException(String message, long responseCode) {
    super(message);
    this.responseCode = responseCode;
  }

  protected AbstractApiException(Throwable cause, long responseCode) {
    super(cause);
    this.responseCode = responseCode;
  }

  protected AbstractApiException(String message, Throwable cause, long responseCode) {
    super(message, cause);
    this.responseCode = responseCode;
  }

  public long getResponseCode() {
    return responseCode;
  }
}
