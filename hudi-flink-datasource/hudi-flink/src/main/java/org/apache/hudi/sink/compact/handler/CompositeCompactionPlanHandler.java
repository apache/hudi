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

/**
 * Composite handler for compaction plan services of the data table and metadata table.
 */
public class CompositeCompactionPlanHandler extends CompositeTableServiceHandler<CompactionPlanHandler>
    implements CompactionPlanHandler {

  CompositeCompactionPlanHandler(CompactionPlanHandler dataTableHandler, CompactionPlanHandler metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  @Override
  public void rollbackCompaction() {
    forEachHandler(CompactionPlanHandler::rollbackCompaction);
  }

  @Override
  public void registerMetrics(MetricGroup metricGroup) {
    forEachHandler(compactionPlanHandler -> compactionPlanHandler.registerMetrics(metricGroup));
  }

  @Override
  public void collectCompactionOperations(long checkpointId,
                                          Output<StreamRecord<CompactionPlanEvent>> output) {
    forEachHandler(compactionPlanHandler -> compactionPlanHandler.collectCompactionOperations(checkpointId, output));
  }

  @Override
  public void close() {
    forEachHandler(CompactionPlanHandler::close);
  }
}
