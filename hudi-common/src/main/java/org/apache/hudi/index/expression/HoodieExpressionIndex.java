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

package org.apache.hudi.index.expression;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.util.CollectionUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface representing a expression index in Hudi.
 *
 * @param <S> The source type of the values from the fields used in the expression index expression.
 *            Note that this assumes than an expression is operating on fields of same type.
 * @param <T> The target type after applying the transformation. Represents the type of the indexed value.
 */
public interface HoodieExpressionIndex<S, T> extends Serializable {

  String HOODIE_EXPRESSION_INDEX_RELATIVE_FILE_PATH = "_hoodie_expression_index_relative_file_path";
  String HOODIE_EXPRESSION_INDEX_PARTITION = "_hoodie_expression_index_partition";
  String HOODIE_EXPRESSION_INDEX_FILE_SIZE = "_hoodie_expression_index_file_size";

  String HOODIE_EXPRESSION_INDEX_PARTITION_STAT_PREFIX = "__partition_stat__";

  String EXPRESSION_OPTION = "expr";
  String TRIM_STRING_OPTION = "trimString";
  String REGEX_GROUP_INDEX_OPTION = "idx";
  String REPLACEMENT_OPTION = "replacement";
  String PATTERN_OPTION = "pattern";
  String LENGTH_OPTION = "len";
  String POSITION_OPTION = "pos";
  String DAYS_OPTION = "days";
  String FORMAT_OPTION = "format";
  String IDENTITY_TRANSFORM = "identity";
  // Bloom filter options
  String BLOOM_FILTER_TYPE = "filtertype";
  String BLOOM_FILTER_NUM_ENTRIES = "numentries";
  String FALSE_POSITIVE_RATE = "fpp";
  String DYNAMIC_BLOOM_MAX_ENTRIES = "maxentries";
  static final Map<String, String> BLOOM_FILTER_CONFIG_MAPPING = CollectionUtils.createImmutableMap(
      new HashMap<String, String>() {
        {
          put(BLOOM_FILTER_TYPE, HoodieStorageConfig.BLOOM_FILTER_TYPE.key());
          put(BLOOM_FILTER_NUM_ENTRIES, HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE.key());
          put(FALSE_POSITIVE_RATE, HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE.key());
          put(DYNAMIC_BLOOM_MAX_ENTRIES, HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES.key());
        }
      });

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
