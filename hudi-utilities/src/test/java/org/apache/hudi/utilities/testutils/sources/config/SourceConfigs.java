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

package org.apache.hudi.utilities.testutils.sources.config;

/**
 * Configurations for Test Data Sources.
 */
public class SourceConfigs {

  // Used by DistributedTestDataSource only. Number of partitions where each partitions generates test-data
  public static final String NUM_SOURCE_PARTITIONS_PROP = "hoodie.deltastreamer.source.test.num_partitions";
  public static final Integer DEFAULT_NUM_SOURCE_PARTITIONS = 10;

  // Maximum number of unique records generated for the run
  public static final String MAX_UNIQUE_RECORDS_PROP = "hoodie.deltastreamer.source.test.max_unique_records";
  public static final Integer DEFAULT_MAX_UNIQUE_RECORDS = Integer.MAX_VALUE;

  // Use Rocks DB for storing datagen keys
  public static final String USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS =
      "hoodie.deltastreamer.source.test.datagen.use_rocksdb_for_storing_existing_keys";
  public static final Boolean DEFAULT_USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS = false;

  // Base Dir for storing datagen keys
  public static final String ROCKSDB_BASE_DIR_FOR_TEST_DATAGEN_KEYS =
      "hoodie.deltastreamer.source.test.datagen.rocksdb_base_dir";

}
