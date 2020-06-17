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

/**
 * Configs for time based key generator.
 */
public class KeyGeneratorTimestampConfig {
  /**
   * Timestamp type. must be one of "UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS, SCALAR".
   * see {@link TimestampTypeEnum}
   */
  public static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";

  /**
   * Unit of input timestamp.
   */
  public static final String INPUT_TIME_UNIT = "hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit";

  /**
   * Input timestamp date format.
   */
  public static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP =
      "hoodie.deltastreamer.keygen.timebased.input.dateformat";

  /**
   * Output timestamp date format.
   */
  public static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP =
      "hoodie.deltastreamer.keygen.timebased.output.dateformat";

  /**
   * Time zone.
   */
  public static final String TIMESTAMP_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.timezone";

}
