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

package org.apache.hudi.index.expression;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.index.expression.HoodieExpressionIndex.BLOOM_FILTER_CONFIG_MAPPING;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.DAYS_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.EXPRESSION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.FORMAT_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.LENGTH_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.PATTERN_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.POSITION_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.REGEX_GROUP_INDEX_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.REPLACEMENT_OPTION;
import static org.apache.hudi.index.expression.HoodieExpressionIndex.TRIM_STRING_OPTION;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;

public class ExpressionIndexSparkFunctions {

  private static final String SPARK_DATE_FORMAT = "date_format";
  private static final String SPARK_DAY = "day";
  private static final String SPARK_MONTH = "month";
  private static final String SPARK_YEAR = "year";
  private static final String SPARK_HOUR = "hour";
  private static final String SPARK_FROM_UNIXTIME = "from_unixtime";
  private static final String SPARK_UNIX_TIMESTAMP = "unix_timestamp";
  private static final String SPARK_TO_DATE = "to_date";
  private static final String SPARK_TO_TIMESTAMP = "to_timestamp";
  private static final String SPARK_DATE_ADD = "date_add";
  private static final String SPARK_DATE_SUB = "date_sub";
  private static final String SPARK_SUBSTRING = "substring";
  private static final String SPARK_UPPER = "upper";
  private static final String SPARK_LOWER = "lower";
  private static final String SPARK_TRIM = "trim";
  private static final String SPARK_LTRIM = "ltrim";
  private static final String SPARK_RTRIM = "rtrim";
  private static final String SPARK_LENGTH = "length";
  private static final String SPARK_REGEXP_REPLACE = "regexp_replace";
  private static final String SPARK_REGEXP_EXTRACT = "regexp_extract";
  private static final String SPARK_SPLIT = "split";
  public static final String IDENTITY_FUNCTION = HoodieExpressionIndex.IDENTITY_TRANSFORM;

  private static final Map<String, SparkFunction> SPARK_FUNCTION_MAP = new HashMap<>();
  static {
    SPARK_FUNCTION_MAP.put(IDENTITY_FUNCTION, new SparkIdentityFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_SPLIT, new SparkSplitFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_REGEXP_EXTRACT, new SparkRegexExtractFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_REGEXP_REPLACE, new SparkRegexReplaceFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_LENGTH, new SparkLengthFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_RTRIM, new SparkRTrimFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_LTRIM, new SparkLTrimFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_TRIM, new SparkTrimFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_LOWER, new SparkLowerFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_UPPER, new SparkUpperFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_SUBSTRING, new SparkSubstringFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_DATE_ADD, new SparkDateAddFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_DATE_SUB, new SparkDateSubFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_TO_TIMESTAMP, new SparkToTimestampFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_TO_DATE, new SparkToDateFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_UNIX_TIMESTAMP, new SparkUnixTimestampFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_FROM_UNIXTIME, new SparkFromUnixTimeFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_HOUR, new SparkHourFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_YEAR, new SparkYearFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_MONTH, new SparkMonthFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_DAY, new SparkDayFunction(){});
    SPARK_FUNCTION_MAP.put(SPARK_DATE_FORMAT, new SparkDateFormatFunction(){});
  }

  /**
   * Custom interface to support Spark functions
   */
  interface SparkFunction extends Serializable {

    String getFunctionName();

    Set<String> getValidOptions();

    Column apply(List<Column> columns, Map<String, String> options);

    default void validateOptions(Map<String, String> options, String indexType) {
      Set<String> validOptions = new HashSet<>(getValidOptions());
      // add bloom filters options if index type is bloom_filters
      if (indexType.equals(PARTITION_NAME_BLOOM_FILTERS)) {
        validOptions.addAll(BLOOM_FILTER_CONFIG_MAPPING.keySet());
      }
      Set<String> invalidOptions = new HashSet<>(options.keySet());
      invalidOptions.removeAll(validOptions);
      ValidationUtils.checkArgument(invalidOptions.isEmpty(), String.format("Input options %s are not valid for spark function %s", invalidOptions, this));
    }

    static SparkFunction getSparkFunction(String functionName) {
      return SPARK_FUNCTION_MAP.get(functionName);
    }
  }

  interface SparkIdentityFunction extends SparkFunction {

    default String getFunctionName() {
      return IDENTITY_FUNCTION;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("IDENTITY requires 1 column");
      }
      return columns.get(0);
    }
  }

  interface SparkSplitFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_SPLIT;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, PATTERN_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("SPLIT requires 1 column");
      }
      return functions.split(columns.get(0), options.get(PATTERN_OPTION));
    }
  }

  interface SparkRegexExtractFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_REGEXP_EXTRACT;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, PATTERN_OPTION, REGEX_GROUP_INDEX_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("REGEXP_EXTRACT requires 1 column");
      }
      return functions.regexp_extract(columns.get(0), options.get(PATTERN_OPTION), Integer.parseInt(options.get(REGEX_GROUP_INDEX_OPTION)));
    }
  }

  interface SparkRegexReplaceFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_REGEXP_REPLACE;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, PATTERN_OPTION, REPLACEMENT_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("REGEXP_REPLACE requires 1 column");
      }
      return functions.regexp_replace(columns.get(0), options.get(PATTERN_OPTION), options.get(REPLACEMENT_OPTION));
    }
  }

  interface SparkLengthFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_LENGTH;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("LENGTH requires 1 column");
      }
      return functions.length(columns.get(0));
    }
  }

  interface SparkRTrimFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_RTRIM;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, TRIM_STRING_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("RTRIM requires 1 column");
      }
      if (options.containsKey(TRIM_STRING_OPTION)) {
        return functions.rtrim(columns.get(0), options.get(TRIM_STRING_OPTION));
      } else {
        return functions.rtrim(columns.get(0));
      }
    }
  }

  interface SparkLTrimFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_LTRIM;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, TRIM_STRING_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("LTRIM requires 1 column");
      }
      if (options.containsKey(TRIM_STRING_OPTION)) {
        return functions.ltrim(columns.get(0), options.get(TRIM_STRING_OPTION));
      } else {
        return functions.ltrim(columns.get(0));
      }
    }
  }

  interface SparkTrimFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_TRIM;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, TRIM_STRING_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("TRIM requires 1 column");
      }
      if (options.containsKey(TRIM_STRING_OPTION)) {
        return functions.trim(columns.get(0), options.get(TRIM_STRING_OPTION));
      } else {
        return functions.trim(columns.get(0));
      }
    }
  }

  interface SparkLowerFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_LOWER;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("LOWER requires 1 column");
      }
      return functions.lower(columns.get(0));
    }
  }

  interface SparkUpperFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_UPPER;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("UPPER requires 1 column");
      }
      return functions.upper(columns.get(0));
    }
  }

  interface SparkSubstringFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_SUBSTRING;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, POSITION_OPTION, LENGTH_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("SUBSTRING requires 1 column");
      }
      return functions.substring(columns.get(0), Integer.parseInt(options.get(POSITION_OPTION)), Integer.parseInt(options.get(LENGTH_OPTION)));
    }
  }

  interface SparkDateAddFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_DATE_ADD;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, DAYS_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("DATE_ADD requires 1 column");
      }
      return functions.date_add(columns.get(0), Integer.parseInt(options.get(DAYS_OPTION)));
    }
  }

  interface SparkDateSubFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_DATE_SUB;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, DAYS_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("DATE_SUB requires 1 column");
      }
      return functions.date_sub(columns.get(0), Integer.parseInt(options.get(DAYS_OPTION)));
    }
  }

  interface SparkToTimestampFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_TO_TIMESTAMP;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, FORMAT_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("TO_TIMESTAMP requires 1 column");
      }
      if (options.containsKey(FORMAT_OPTION)) {
        return functions.to_timestamp(columns.get(0), options.get(FORMAT_OPTION));
      } else {
        return functions.to_timestamp(columns.get(0));
      }
    }
  }

  interface SparkToDateFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_TO_DATE;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, FORMAT_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("TO_DATE requires 1 column");
      }
      if (options.containsKey(FORMAT_OPTION)) {
        return functions.to_date(columns.get(0), options.get(FORMAT_OPTION));
      } else {
        return functions.to_date(columns.get(0));
      }
    }
  }

  interface SparkUnixTimestampFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_UNIX_TIMESTAMP;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, FORMAT_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("UNIX_TIMESTAMP requires 1 column");
      }
      if (options.containsKey(FORMAT_OPTION)) {
        return functions.unix_timestamp(columns.get(0), options.get(FORMAT_OPTION));
      } else {
        return functions.unix_timestamp(columns.get(0));
      }
    }
  }

  interface SparkFromUnixTimeFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_FROM_UNIXTIME;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, FORMAT_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("FROM_UNIXTIME requires 1 column");
      }
      if (options.containsKey(FORMAT_OPTION)) {
        return functions.from_unixtime(columns.get(0), options.get(FORMAT_OPTION));
      } else {
        return functions.from_unixtime(columns.get(0));
      }
    }
  }

  interface SparkHourFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_HOUR;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("HOUR requires 1 column");
      }
      return functions.hour(columns.get(0));
    }
  }

  interface SparkYearFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_YEAR;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("YEAR requires 1 column");
      }
      return functions.year(columns.get(0));
    }
  }

  interface SparkMonthFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_MONTH;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("MONTH requires 1 column");
      }
      return functions.month(columns.get(0));
    }
  }

  interface SparkDayFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_DAY;
    }

    @Override
    default Set<String> getValidOptions() {
      return Collections.singleton(EXPRESSION_OPTION);
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("DAY requires 1 column");
      }
      return functions.dayofmonth(columns.get(0));
    }
  }

  interface SparkDateFormatFunction extends SparkFunction {

    @Override
    default String getFunctionName() {
      return SPARK_DATE_FORMAT;
    }

    @Override
    default Set<String> getValidOptions() {
      return new HashSet<>(Arrays.asList(EXPRESSION_OPTION, FORMAT_OPTION));
    }

    @Override
    default Column apply(List<Column> columns, Map<String, String> options) {
      if (columns.size() != 1) {
        throw new IllegalArgumentException("DATE_FORMAT requires 1 column");
      }
      if (!options.containsKey(FORMAT_OPTION)) {
        throw new IllegalArgumentException("DATE_FORMAT requires format option");
      }
      return functions.date_format(columns.get(0), options.get(FORMAT_OPTION));
    }
  }
}
