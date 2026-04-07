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

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Composite handler for compaction plan services of the data table and metadata table.
 */
public class CompositeCompactionPlanHandler extends CompositeTableServiceHandler<CompactionPlanHandler> {

  CompositeCompactionPlanHandler(Option<CompactionPlanHandler> dataTableHandler, Option<CompactionPlanHandler> metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  public static CompositeCompactionPlanHandler create(Configuration conf, RuntimeContext runtimeContext) {
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, runtimeContext);
    return new CompositeCompactionPlanHandler(
        OptionsResolver.needsAsyncCompaction(conf)
            ? Option.of(new CompactionPlanHandler(writeClient))
            : Option.empty(),
        OptionsResolver.needsAsyncMetadataCompaction(conf)
            ? Option.of(new MetadataCompactionPlanHandler(StreamerUtil.createMetadataWriteClient(writeClient)))
            : Option.empty());
  }

  public void rollbackCompaction() {
    forEachHandler(CompactionPlanHandler::rollbackCompaction);
  }

  public void collectCompactionOperations(long checkpointId,
                                          FlinkCompactionMetrics compactionMetrics,
                                          Output<StreamRecord<CompactionPlanEvent>> output) {
    forEachHandler(compactionPlanHandler -> compactionPlanHandler.collectCompactionOperations(checkpointId, compactionMetrics, output));
  }

  @Override
  public void close() {
    forEachHandler(CompactionPlanHandler::close);
  }
}
