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

package org.apache.hudi.keygen.constant;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TimestampKeyGeneratorConfig;

/**
 * Key generator configs.
 */
@ConfigClassProperty(name = "Key Generator Options",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    subGroupName = ConfigGroups.SubGroupNames.KEY_GENERATOR,
    areCommonConfigs = true,
    description = "")
public class KeyGeneratorOptions extends HoodieConfig {

  public static final ConfigProperty<String> URL_ENCODE_PARTITIONING = ConfigProperty
      .key("hoodie.datasource.write.partitionpath.urlencode")
      .defaultValue("false")
      .markAdvanced()
      .withDocumentation("Should we url encode the partition path value, before creating the folder structure.");

  public static final ConfigProperty<String> HIVE_STYLE_PARTITIONING_ENABLE = ConfigProperty
      .key("hoodie.datasource.write.hive_style_partitioning")
      .defaultValue("false")
      .withDocumentation("Flag to indicate whether to use Hive style partitioning.\n"
          + "If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.\n"
          + "By default false (the names of partition folders are only partition values)");

  public static final ConfigProperty<String> RECORDKEY_FIELD_NAME = ConfigProperty
      .key("hoodie.datasource.write.recordkey.field")
      .noDefaultValue()
      .withDocumentation("Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using\n"
          + "the dot notation eg: `a.b.c`");

  public static final ConfigProperty<String> SECONDARYKEY_COLUMN_NAME = ConfigProperty
      .key("hoodie.datasource.write.secondarykey.column")
      .noDefaultValue()
      .withDocumentation("Columns that constitute the secondary key component.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using\n"
          + "the dot notation eg: `a.b.c`");

  public static final ConfigProperty<String> PARTITIONPATH_FIELD_NAME = ConfigProperty
      .key("hoodie.datasource.write.partitionpath.field")
      .noDefaultValue()
      .withDocumentation("Partition path field. Value to be used at the partitionPath component of HoodieKey. "
          + "Actual value obtained by invoking .toString()");

  public static final ConfigProperty<String> KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED = ConfigProperty
      .key("hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled")
      .defaultValue("false")
      .sinceVersion("0.10.1")
      .markAdvanced()
      .withDocumentation("When set to true, consistent value will be generated for a logical timestamp type column, "
          + "like timestamp-millis and timestamp-micros, irrespective of whether row-writer is enabled. Disabled by default so "
          + "as not to break the pipeline that deploy either fully row-writer path or non row-writer path. For example, "
          + "if it is kept disabled then record key of timestamp type with value `2016-12-29 09:54:00` will be written as timestamp "
          + "`2016-12-29 09:54:00.0` in row-writer path, while it will be written as long value `1483023240000000` in non row-writer path. "
          + "If enabled, then the timestamp value will be written in both the cases.");

  /**
   * @deprecated Use {@link #URL_ENCODE_PARTITIONING} and its methods.
   */
  @Deprecated
  public static final String URL_ENCODE_PARTITIONING_OPT_KEY = URL_ENCODE_PARTITIONING.key();
  /**
   * @deprecated Use {@link #URL_ENCODE_PARTITIONING} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL = URL_ENCODE_PARTITIONING.defaultValue();
  /**
   * @deprecated Use {@link #HIVE_STYLE_PARTITIONING_ENABLE} and its methods.
   */
  @Deprecated
  public static final String HIVE_STYLE_PARTITIONING_OPT_KEY = HIVE_STYLE_PARTITIONING_ENABLE.key();
  /**
   * @deprecated Use {@link #HIVE_STYLE_PARTITIONING_ENABLE} and its methods.
   */
  @Deprecated
  public static final String DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL = HIVE_STYLE_PARTITIONING_ENABLE.defaultValue();
  /**
   * @deprecated Use {@link #RECORDKEY_FIELD_NAME} and its methods.
   */
  @Deprecated
  public static final String RECORDKEY_FIELD_OPT_KEY = RECORDKEY_FIELD_NAME.key();
  /**
   * @deprecated Use {@link #PARTITIONPATH_FIELD_NAME} and its methods.
   */
  @Deprecated
  public static final String PARTITIONPATH_FIELD_OPT_KEY = PARTITIONPATH_FIELD_NAME.key();

  /**
   * Supported configs.
   */
  @Deprecated
  public static class Config {

    // One value from TimestampType above
    @Deprecated
    public static final String TIMESTAMP_TYPE_FIELD_PROP = TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key();
    @Deprecated
    public static final String INPUT_TIME_UNIT = TimestampKeyGeneratorConfig.INPUT_TIME_UNIT.key();
    //This prop can now accept list of input date formats.
    @Deprecated
    public static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP = TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT.key();
    @Deprecated
    public static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP =
        TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX.key();
    @Deprecated
    public static final String TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP = TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_TIMEZONE_FORMAT.key();
    @Deprecated
    public static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP = TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT.key();
    //still keeping this prop for backward compatibility so that functionality for existing users does not break.
    @Deprecated
    public static final String TIMESTAMP_TIMEZONE_FORMAT_PROP = TimestampKeyGeneratorConfig.TIMESTAMP_TIMEZONE_FORMAT.key();
    @Deprecated
    public static final String TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP = TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT.key();
    @Deprecated
    public static final String DATE_TIME_PARSER_PROP = TimestampKeyGeneratorConfig.DATE_TIME_PARSER.key();
  }
}

