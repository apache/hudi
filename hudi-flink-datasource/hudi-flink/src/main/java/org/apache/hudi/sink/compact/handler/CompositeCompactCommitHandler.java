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
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Composite handler for compaction commit services of the data table and metadata table.
 */
public class CompositeCompactCommitHandler extends CompositeTableServiceHandler<Lazy<CompactCommitHandler>> {

  CompositeCompactCommitHandler(Option<Lazy<CompactCommitHandler>> dataTableHandler, Option<Lazy<CompactCommitHandler>> metadataTableHandler) {
    super(dataTableHandler, metadataTableHandler);
  }

  public static CompositeCompactCommitHandler create(Configuration conf, RuntimeContext runtimeContext) {
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, runtimeContext);
    return new CompositeCompactCommitHandler(
        Option.of(Lazy.lazily(() -> new CompactCommitHandler(conf, writeClient))),
        Option.of(Lazy.lazily(() -> new MetadataCompactCommitHandler(conf, StreamerUtil.createMetadataWriteClient(writeClient)))));
  }

  public void commitIfNecessary(CompactionCommitEvent event, FlinkCompactionMetrics compactionMetrics) {
    getHandler(event.isMetadataTable()).get().commitIfNecessary(event, compactionMetrics);
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
