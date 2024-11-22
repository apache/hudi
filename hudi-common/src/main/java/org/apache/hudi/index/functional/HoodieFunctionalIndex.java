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

package org.apache.hudi.index.functional;

import java.io.Serializable;
import java.util.List;

/**
 * Interface representing a functional index in Hudi.
 *
 * @param <S> The source type of the values from the fields used in the functional index expression.
 *            Note that this assumes than an expression is operating on fields of same type.
 * @param <T> The target type after applying the transformation. Represents the type of the indexed value.
 */
public interface HoodieFunctionalIndex<S, T> extends Serializable {

  String HOODIE_FUNCTIONAL_INDEX_RELATIVE_FILE_PATH = "_hoodie_functional_index_relative_file_path";
  String HOODIE_FUNCTIONAL_INDEX_PARTITION = "_hoodie_functional_index_partition";
  String HOODIE_FUNCTIONAL_INDEX_FILE_SIZE = "_hoodie_functional_index_file_size";
  String SPARK_DATE_FORMAT = "date_format";
  String SPARK_DAY = "day";
  String SPARK_MONTH = "month";
  String SPARK_YEAR = "year";
  String SPARK_HOUR = "hour";
  String SPARK_FROM_UNIXTIME = "from_unixtime";
  String SPARK_UNIX_TIMESTAMP = "unix_timestamp";
  String SPARK_TO_DATE = "to_date";
  String SPARK_TO_TIMESTAMP = "to_timestamp";
  String SPARK_DATE_ADD = "date_add";
  String SPARK_DATE_SUB = "date_sub";
  String SPARK_CONCAT = "concat";
  String SPARK_SUBSTRING = "substring";
  String SPARK_UPPER = "upper";
  String SPARK_LOWER = "lower";
  String SPARK_TRIM = "trim";
  String SPARK_LTRIM = "ltrim";
  String SPARK_RTRIM = "rtrim";
  String SPARK_LENGTH = "length";
  String SPARK_REGEXP_REPLACE = "regexp_replace";
  String SPARK_REGEXP_EXTRACT = "regexp_extract";
  String SPARK_SPLIT = "split";
  String SPARK_IDENTITY = "identity";

  /**
   * Get the name of the index.
   *
   * @return Name of the index.
   */
  String getIndexName();

  /**
   * Get the expression associated with the index.
   *
   * @return Expression string.
   */
  String getIndexFunction();

  /**
   * Get the list of fields involved in the expression in order.
   *
   * @return List of fields.
   */
  List<String> getOrderedSourceFields();

  /**
   * Apply the transformation based on the source values and the expression.
   *
   * @param orderedSourceValues List of source values corresponding to fields in the expression.
   * @return Transformed value.
   */
  T apply(List<S> orderedSourceValues);
}
