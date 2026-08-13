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

package org.apache.hudi.metadata;

import java.io.Serializable;

/**
 * Sink for per-shard record index lookup stats, called once per shard actually read.
 *
 * <p>Deliberately a single method. Collection happens in engine-agnostic code so that every engine
 * shares one definition of each count; only the transport differs. A Spark implementation ships
 * stats to the driver through an accumulator; a Flink implementation would report to a
 * {@code MetricGroup} per subtask.
 *
 * <p>Implementations are captured in engine closures and must be serializable, and they must never
 * throw: instrumentation must not be able to fail a write.
 */
@FunctionalInterface
public interface RecordIndexLookupStatsCollector extends Serializable {

  /**
   * Discards everything. Used whenever collection is disabled, and as the default for callers that
   * do not ask for stats. Identity-compared at call sites to skip instrumentation entirely.
   */
  RecordIndexLookupStatsCollector NOOP = stats -> {
  };

  void collect(RecordIndexShardLookupStats stats);
}
