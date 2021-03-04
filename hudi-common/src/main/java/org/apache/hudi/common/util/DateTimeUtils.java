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

package org.apache.hudi.common.util;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public class DateTimeUtils {

  /**
   * Parse input String to a {@link java.time.Instant}.
   * @param s Input String should be Epoch time in millisecond or ISO-8601 format.
   */
  public static Instant parseDateTime(String s) throws DateTimeParseException {
    ValidationUtils.checkArgument(Objects.nonNull(s), "Input String cannot be null.");
    try {
      return Instant.ofEpochMilli(Long.parseLong(s));
    } catch (NumberFormatException e) {
      return Instant.parse(s);
    }
  }
}
