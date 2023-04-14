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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigProperty;

/**
 * Configurations for Test Data Sources.
 */
public class SourceTestConfig {

  public static final ConfigProperty<Integer> NUM_SOURCE_PARTITIONS_PROP = ConfigProperty
      .key("hoodie.deltastreamer.source.test.num_partitions")
      .defaultValue(10)
      .withDocumentation("Used by DistributedTestDataSource only. Number of partitions where each partitions generates test-data");

  public static final ConfigProperty<Integer> MAX_UNIQUE_RECORDS_PROP = ConfigProperty
      .key("hoodie.deltastreamer.source.test.max_unique_records")
      .defaultValue(Integer.MAX_VALUE)
      .withDocumentation("Maximum number of unique records generated for the run");

  public static final ConfigProperty<Boolean> USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS = ConfigProperty
      .key("hoodie.deltastreamer.source.test.datagen.use_rocksdb_for_storing_existing_keys")
      .defaultValue(false)
      .withDocumentation("If true, uses Rocks DB for storing datagen keys");

  public static final ConfigProperty<String> ROCKSDB_BASE_DIR_FOR_TEST_DATAGEN_KEYS = ConfigProperty
      .key("hoodie.deltastreamer.source.test.datagen.rocksdb_base_dir")
      .noDefaultValue()
      .withDocumentation("Base Dir for storing datagen keys");

}
