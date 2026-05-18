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

/**
 * Composite handler for compaction execution services of the data table and metadata table.
 */
public class CompositeCompactHandler extends CompositeTableServiceHandler<CompactHandler> implements CompactHandler {

  CompositeCompactHandler(CompactHandler dataTableHandler, CompactHandler metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  @Override
  public void registerMetrics(MetricGroup metricGroup) {
    forEachHandler(compactHandler -> compactHandler.registerMetrics(metricGroup));
  }

  @Override
  public void compact(@Nullable NonThrownExecutor executor,
                      CompactionPlanEvent event,
                      Collector<CompactionCommitEvent> collector,
                      boolean needReloadMetaClient) throws Exception {
    getHandler(event.isMetadataTable()).compact(executor, event, collector, needReloadMetaClient);
  }

  @Override
  public void close() {
    forEachHandler(CompactHandler::close);
  }
}
