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

package org.apache.hudi.common.config;

import java.util.concurrent.TimeUnit;

/**
 * Timestamp-based key generator configs.
 */
@ConfigClassProperty(name = "Timestamp-based key generator configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.KEY_GENERATOR,
    description = "Configs used for TimestampBasedKeyGenerator which relies on timestamps for "
        + "the partition field. The field values are interpreted as timestamps and not just "
        + "converted to string while generating partition path value for records. Record key is "
        + "same as before where it is chosen by field name.")
public class TimestampKeyGeneratorConfig extends HoodieConfig {
  private static final String TIMESTAMP_KEYGEN_CONFIG_PREFIX = "hoodie.keygen.timebased.";
  @Deprecated
  private static final String OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX = "hoodie.deltastreamer.keygen.timebased.";

  public static final ConfigProperty<String> TIMESTAMP_TYPE_FIELD = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "timestamp.type")
      .noDefaultValue()
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "timestamp.type")
      .markAdvanced()
      .withDocumentation("Timestamp type of the field, which should be one of the timestamp types "
          + "supported: `UNIX_TIMESTAMP`, `DATE_STRING`, `MIXED`, `EPOCHMILLISECONDS`,"
          + " `EPOCHMICROSECONDS`, `SCALAR`.");

  public static final ConfigProperty<String> INPUT_TIME_UNIT = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "timestamp.scalar.time.unit")
      .defaultValue(TimeUnit.SECONDS.toString())
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "timestamp.scalar.time.unit")
      .markAdvanced()
      .withDocumentation("When timestamp type `SCALAR` is used, this specifies the time unit, "
          + "with allowed unit specified by `TimeUnit` enums (`NANOSECONDS`, `MICROSECONDS`, "
          + "`MILLISECONDS`, `SECONDS`, `MINUTES`, `HOURS`, `DAYS`).");

  //This prop can now accept list of input date formats.
  public static final ConfigProperty<String> TIMESTAMP_INPUT_DATE_FORMAT = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "input.dateformat")
      .defaultValue("")
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "input.dateformat")
      .markAdvanced()
      .withDocumentation("Input date format such as `yyyy-MM-dd'T'HH:mm:ss.SSSZ`.");

  public static final ConfigProperty<String> TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "input.dateformat.list.delimiter.regex")
      .defaultValue(",")
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "input.dateformat.list.delimiter.regex")
      .markAdvanced()
      .withDocumentation("The delimiter for allowed input date format list, usually `,`.");

  public static final ConfigProperty<String> TIMESTAMP_INPUT_TIMEZONE_FORMAT = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "input.timezone")
      .defaultValue("UTC")
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "input.timezone")
      .markAdvanced()
      .withDocumentation("Timezone of the input timestamp, such as `UTC`.");

  public static final ConfigProperty<String> TIMESTAMP_OUTPUT_DATE_FORMAT = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "output.dateformat")
      .defaultValue("")
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "output.dateformat")
      .markAdvanced()
      .withDocumentation("Output date format such as `yyyy-MM-dd'T'HH:mm:ss.SSSZ`.");

  public static final ConfigProperty<String> TIMESTAMP_OUTPUT_TIMEZONE_FORMAT = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "output.timezone")
      .defaultValue("UTC")
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "output.timezone")
      .markAdvanced()
      .withDocumentation("Timezone of the output timestamp, such as `UTC`.");

  //still keeping this prop for backward compatibility so that functionality for existing users does not break.
  @Deprecated
  public static final ConfigProperty<String> TIMESTAMP_TIMEZONE_FORMAT = ConfigProperty
      .key(TIMESTAMP_KEYGEN_CONFIG_PREFIX + "timezone")
      .defaultValue("UTC")
      .withAlternatives(OLD_TIMESTAMP_KEYGEN_CONFIG_PREFIX + "timezone")
      .markAdvanced()
      .withDocumentation("Timezone of both input and output timestamp if they are the same, such "
          + "as `UTC`.  Please use `" + TIMESTAMP_INPUT_TIMEZONE_FORMAT.key() + "` and `"
          + TIMESTAMP_OUTPUT_TIMEZONE_FORMAT.key() + "` instead if the input and output timezones "
          + "are different.");

  public static final ConfigProperty<String> DATE_TIME_PARSER = ConfigProperty
      .key("hoodie.keygen.datetime.parser.class")
      .defaultValue("org.apache.hudi.keygen.parser.HoodieDateTimeParser")
      .withAlternatives("hoodie.deltastreamer.keygen.datetime.parser.class")
      .markAdvanced()
      .withDocumentation("Date time parser class name.");
}
