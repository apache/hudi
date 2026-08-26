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

package org.apache.hudi.metrics;

/**
 * Counter names for the record index lookup phase. Collection itself is generic -- see
 * {@link ExecutorMetricRegistry} and {@link ExecutorMetrics}.
 */
public class RecordIndexMetricNames {

  /** Scoping, prefix and reporter naming all live on the enum entry. */
  public static final String REGISTRY_NAME = ExecutorMetricRegistry.RECORD_INDEX_LOOKUP.registryName();

  public static final String KEY_COUNT = "lookup_record_index_key_count";
  public static final String KEY_HIT_COUNT = "lookup_record_index_key_hit_count";
  public static final String KEY_MISS_COUNT = "lookup_record_index_key_miss_count";
  public static final String SHARDS_READ = "lookup_record_index_shards_read";
  /** Wall-clock spent in the shard read, summed across shards. Revives the third dead upstream constant,
   * {@code HoodieMetadataMetrics.LOOKUP_RECORD_INDEX_TIME_STR}. */
  public static final String LOOKUP_TIME = "lookup_record_index_time";

  private RecordIndexMetricNames() {
  }
}
