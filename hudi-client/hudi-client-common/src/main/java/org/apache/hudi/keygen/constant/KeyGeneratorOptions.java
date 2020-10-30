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

public class KeyGeneratorOptions {

  /**
   * Flag to indicate whether to use Hive style partitioning.
   * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
   * By default false (the names of partition folders are only partition values)
   */
  public static final String URL_ENCODE_PARTITIONING_OPT_KEY = "hoodie.datasource.write.partitionpath.urlencode";
  public static final String DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL = "false";
  public static final String HIVE_STYLE_PARTITIONING_OPT_KEY = "hoodie.datasource.write.hive_style_partitioning";
  public static final String DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL = "false";

  /**
   * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
   * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
   * the dot notation eg: `a.b.c`
   */
  public static final String RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field";
  public static final String PARTITIONPATH_FIELD_OPT_KEY = "hoodie.datasource.write.partitionpath.field";
}

