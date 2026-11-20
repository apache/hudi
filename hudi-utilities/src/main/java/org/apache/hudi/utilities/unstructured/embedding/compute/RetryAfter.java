/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.compute;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Reads a provider's retry hint: Azure's {@code retry-after-ms} (milliseconds) first, else {@code Retry-After} as
 * delta-seconds (OpenAI) or as an HTTP-date (RFC 9110 allows both; the date is parsed as RFC 1123).
 */
public final class RetryAfter {

  /** Larger delta-seconds saturate instead of overflowing when converted to milliseconds. */
  private static final long MAX_SECONDS = Long.MAX_VALUE / 1000;

  private RetryAfter() {
  }

  /**
   * @return the hinted wait in milliseconds (0 for a date already past), or -1 when there is no usable hint: absent,
   *     negative or unparseable values are ignored, never an error
   */
  public static long hintMillis(String retryAfterMs, String retryAfter, long nowMillis) {
    long millis = nonNegativeLong(retryAfterMs);
    if (millis >= 0) {
      return millis;
    }
    if (retryAfter == null) {
      return -1;
    }
    long seconds = nonNegativeLong(retryAfter);
    if (seconds >= 0) {
      return seconds > MAX_SECONDS ? Long.MAX_VALUE : seconds * 1000;
    }
    try {
      long at = ZonedDateTime.parse(retryAfter.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli();
      return Math.max(0, at - nowMillis);
    } catch (DateTimeParseException e) {
      return -1;
    }
  }

  private static long nonNegativeLong(String value) {
    if (value == null) {
      return -1;
    }
    try {
      long parsed = Long.parseLong(value.trim());
      return parsed >= 0 ? parsed : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}
