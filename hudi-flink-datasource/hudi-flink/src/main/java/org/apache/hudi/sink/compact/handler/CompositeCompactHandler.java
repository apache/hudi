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
import org.apache.hudi.metrics.FlinkCompactionMetrics;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.utils.NonThrownExecutor;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * Composite handler for compaction execution services of the data table and metadata table.
 */
public class CompositeCompactHandler extends CompositeTableServiceHandler<Lazy<CompactHandler>> {

  CompositeCompactHandler(Option<Lazy<CompactHandler>> dataTableHandler, Option<Lazy<CompactHandler>> metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  public static CompositeCompactHandler create(Configuration conf, RuntimeContext runtimeContext, int taskId) {
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, runtimeContext);
    return new CompositeCompactHandler(
        Option.of(Lazy.lazily(() -> new CompactHandler(writeClient, taskId))),
        Option.of(Lazy.lazily(() -> new MetadataCompactHandler(StreamerUtil.createMetadataWriteClient(writeClient), taskId))));
  }

  public void compact(@Nullable NonThrownExecutor executor,
                      CompactionPlanEvent event,
                      Collector<CompactionCommitEvent> collector,
                      boolean needReloadMetaClient,
                      FlinkCompactionMetrics compactionMetrics) throws Exception {
    getHandler(event.isMetadataTable()).get().compact(executor, event, collector, needReloadMetaClient, compactionMetrics);
  }

  @Override
  public void close() {
    forEachHandler(lazyHandler -> {
      if (lazyHandler.isInitialized()) {
        lazyHandler.get().close();
      }
    });
  }
}
