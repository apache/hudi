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

import org.apache.hudi.sink.compact.CompactionCommitEvent;

import org.apache.flink.metrics.MetricGroup;

import java.io.Closeable;

/**
 * Abstraction for compaction commit handling in the Flink compaction pipeline.
 *
 * <p>The interface hides whether commit coordination is performed by a single table-specific
 * implementation or by a composite handler that routes commit events for the data table and the
 * metadata table.
 *
 * <p>Implementations are responsible for buffering commit events, checking commit readiness, and
 * completing or rolling back compaction instants when necessary.
 */
public interface CompactionCommitHandler extends Closeable {
  void registerMetrics(MetricGroup metricGroup);

  void commitIfNecessary(CompactionCommitEvent event);

  @Override
  void close();
}
