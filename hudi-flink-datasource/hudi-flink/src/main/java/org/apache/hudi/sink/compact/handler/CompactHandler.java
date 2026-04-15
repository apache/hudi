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
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.utils.NonThrownExecutor;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.Closeable;

/**
 * Abstraction for compaction execution in the Flink compaction pipeline.
 *
 * <p>The interface lets the operator execute compaction work without knowing whether the work is
 * handled by a single table-specific implementation or delegated through a composite handler that
 * routes between data-table and metadata-table compaction.
 *
 * <p>Implementations are responsible for executing compaction operations and emitting the
 * corresponding {@link CompactionCommitEvent}s.
 */
public interface CompactHandler extends Closeable {
  void registerMetrics(MetricGroup metricGroup);

  void compact(@Nullable NonThrownExecutor executor,
               CompactionPlanEvent event,
               Collector<CompactionCommitEvent> collector,
               boolean needReloadMetaClient) throws Exception;

  @Override
  void close();
}
