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

package org.apache.hudi.utilities.keygen.common;

import java.io.Serializable;

/**
 * Timestamp type enum, currently only five types are supported.
 */
public enum TimestampTypeEnum implements Serializable {

  /**
   * Timestamp of the long type with length 10, such as 1592616630.
   */
  UNIX_TIMESTAMP,

  /**
   * Timestamp of string type, whose format is depended on config, see
   * {@link KeyGeneratorTimestampConfig#TIMESTAMP_INPUT_DATE_FORMAT_PROP}.
   */
  DATE_STRING,

  /**
   * A special type of {@link TimestampTypeEnum#DATE_STRING}, this aims to be compatible with more string date formats,
   * but with the expense of time accuracy.
   * <p>
   * when the timestamp type is set to MIXED, it means the input value is of
   * {@link TimestampTypeEnum#DATE_STRING} type, but might have several different string formats.
   * For example. If the timestamp type is set to MIXED, and {@link TimestampTypeEnum#DATE_STRING} set to "yyyy-MM-dd",
   * then the KeyGenerator could take in timestamp with "yyyy-MM-dd", "yyyy-MM-dd HH","yyyy-MM-dd HH:mm:ss" format,
   * but the parsed result will lose some accuracy, in "yyyy-MM-dd" format only.
   */
  MIXED,

  /**
   * Timestamp of the long type with length 13, such as 1592616630624.
   */
  EPOCHMILLISECONDS,

  /**
   * Scalar timestamp type.
   */
  SCALAR

}
