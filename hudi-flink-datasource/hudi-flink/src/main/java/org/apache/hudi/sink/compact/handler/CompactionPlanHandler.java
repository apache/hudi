/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact.handler;

import org.apache.hudi.sink.compact.CompactionPlanEvent;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Closeable;

/**
 * Abstraction for compaction plan scheduling in the Flink compaction pipeline.
 *
 * <p>The interface hides whether the caller is interacting with a single table-specific handler
 * or a composite handler that coordinates both data-table and metadata-table scheduling.
 *
 * <p>Implementations are responsible for collecting pending compaction operations, emitting
 * {@link CompactionPlanEvent}s, and rolling back inflight compaction instants during recovery.
 */
public interface CompactionPlanHandler extends Closeable {
  void registerMetrics(MetricGroup metricGroup);

  void rollbackCompaction();

  void collectCompactionOperations(
      long checkpointId,
      Output<StreamRecord<CompactionPlanEvent>> output);

  @Override
  void close();
}
