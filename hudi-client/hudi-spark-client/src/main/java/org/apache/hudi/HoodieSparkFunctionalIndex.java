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

package org.apache.hudi;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.functional.HoodieFunctionalIndex;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class HoodieSparkFunctionalIndex implements HoodieFunctionalIndex<Column, Column>, Serializable {

  /**
   * Custom interface to support Spark functions
   */
  @FunctionalInterface
  interface SparkFunction extends Serializable {
    Column apply(List<Column> columns, Map<String, String> options);
  }

  /**
   * Map of Spark functions to their implementations.
   * NOTE: This is not an exhaustive list of spark-sql functions. Only the common date/timestamp and string functions have been added.
   * Add more functions as needed. However, keep the key should match the exact spark-sql function name in lowercase.
   */
  public static final Map<String, SparkFunction> SPARK_FUNCTION_MAP = CollectionUtils.createImmutableMap(
      // Date/Timestamp functions
      Pair.of(SPARK_DATE_FORMAT, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("DATE_FORMAT requires 1 column");
        }
        return functions.date_format(columns.get(0), options.get("format"));
      }),
      Pair.of(SPARK_DAY, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("DAY requires 1 column");
        }
        return functions.dayofmonth(columns.get(0));
      }),
      Pair.of(SPARK_YEAR, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("YEAR requires 1 column");
        }
        return functions.year(columns.get(0));
      }),
      Pair.of(SPARK_MONTH, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("MONTH requires 1 column");
        }
        return functions.month(columns.get(0));
      }),
      Pair.of(SPARK_HOUR, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("HOUR requires 1 column");
        }
        return functions.hour(columns.get(0));
      }),
      Pair.of(SPARK_FROM_UNIXTIME, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("FROM_UNIXTIME requires 1 column");
        }
        return functions.from_unixtime(columns.get(0), options.get("format"));
      }),
      Pair.of(SPARK_UNIX_TIMESTAMP, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("UNIX_TIMESTAMP requires 1 column");
        }
        return functions.unix_timestamp(columns.get(0), options.get("format"));
      }),
      Pair.of(SPARK_TO_DATE, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("TO_DATE requires 1 column");
        }
        return functions.to_date(columns.get(0));
      }),
      Pair.of(SPARK_TO_TIMESTAMP, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("TO_TIMESTAMP requires 1 column");
        }
        return functions.to_timestamp(columns.get(0));
      }),
      Pair.of(SPARK_DATE_ADD, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("DATE_ADD requires 1 column");
        }
        return functions.date_add(columns.get(0), Integer.parseInt(options.get("days")));
      }),
      Pair.of(SPARK_DATE_SUB, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("DATE_SUB requires 1 column");
        }
        return functions.date_sub(columns.get(0), Integer.parseInt(options.get("days")));
      }),

      // String functions
      Pair.of(SPARK_CONCAT, (columns, options) -> {
        if (columns.size() < 2) {
          throw new IllegalArgumentException("CONCAT requires at least 2 columns");
        }
        return functions.concat(columns.toArray(new Column[0]));
      }),
      Pair.of(SPARK_SUBSTRING, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("SUBSTRING requires 1 column");
        }
        return functions.substring(columns.get(0), Integer.parseInt(options.get("pos")), Integer.parseInt(options.get("len")));
      }),
      Pair.of(SPARK_LOWER, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("LOWER requires 1 column");
        }
        return functions.lower(columns.get(0));
      }),
      Pair.of(SPARK_UPPER, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("UPPER requires 1 column");
        }
        return functions.upper(columns.get(0));
      }),
      Pair.of(SPARK_TRIM, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("TRIM requires 1 column");
        }
        return functions.trim(columns.get(0));
      }),
      Pair.of(SPARK_LTRIM, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("LTRIM requires 1 column");
        }
        return functions.ltrim(columns.get(0));
      }),
      Pair.of(SPARK_RTRIM, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("RTRIM requires 1 column");
        }
        return functions.rtrim(columns.get(0));
      }),
      Pair.of(SPARK_LENGTH, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("LENGTH requires 1 column");
        }
        return functions.length(columns.get(0));
      }),
      Pair.of(SPARK_REGEXP_REPLACE, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("REGEXP_REPLACE requires 1 column");
        }
        return functions.regexp_replace(columns.get(0), options.get("pattern"), options.get("replacement"));
      }),
      Pair.of(SPARK_REGEXP_EXTRACT, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("REGEXP_EXTRACT requires 1 column");
        }
        return functions.regexp_extract(columns.get(0), options.get("pattern"), Integer.parseInt(options.get("idx")));
      }),
      Pair.of(SPARK_SPLIT, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("SPLIT requires 1 column");
        }
        return functions.split(columns.get(0), options.get("pattern"));
      }),
      Pair.of(SPARK_IDENTITY, (columns, options) -> {
        if (columns.size() != 1) {
          throw new IllegalArgumentException("IDENTITY requires 1 column");
        }
        return columns.get(0);
      })
  );

  private String indexName;
  private String indexFunction;
  private List<String> orderedSourceFields;
  private Map<String, String> options;
  private SparkFunction sparkFunction;

  public HoodieSparkFunctionalIndex() {
  }

  public HoodieSparkFunctionalIndex(String indexName, String indexFunction, List<String> orderedSourceFields, Map<String, String> options) {
    this.indexName = indexName;
    this.indexFunction = indexFunction;
    this.orderedSourceFields = orderedSourceFields;
    this.options = options;

    // Check if the function from the expression exists in our map
    this.sparkFunction = SPARK_FUNCTION_MAP.get(indexFunction);
    if (this.sparkFunction == null) {
      throw new IllegalArgumentException("Unsupported Spark function: " + indexFunction);
    }
  }

  @Override
  public String getIndexName() {
    return indexName;
  }

  @Override
  public String getIndexFunction() {
    return indexFunction;
  }

  @Override
  public List<String> getOrderedSourceFields() {
    return orderedSourceFields;
  }

  @Override
  public Column apply(List<Column> orderedSourceValues) {
    if (orderedSourceValues.size() != orderedSourceFields.size()) {
      throw new IllegalArgumentException("Mismatch in number of source values and fields in the expression");
    }
    return sparkFunction.apply(orderedSourceValues, options);
  }
}
