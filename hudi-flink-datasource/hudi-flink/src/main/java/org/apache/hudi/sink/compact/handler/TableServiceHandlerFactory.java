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
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Factory for table service handlers used by the Flink compaction and clean pipeline.
 */
public final class TableServiceHandlerFactory {
  private TableServiceHandlerFactory() {
  }

  public static CompactionPlanHandler createCompactionPlanHandler(Configuration conf, RuntimeContext runtimeContext) {
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, runtimeContext);
    boolean needsDataCompaction = OptionsResolver.needsAsyncCompaction(conf);
    boolean needsMetadataCompaction = OptionsResolver.needsAsyncMetadataCompaction(conf);
    if (needsDataCompaction && needsMetadataCompaction) {
      return new CompositeCompactionPlanHandler(
          new DataTableCompactionPlanHandler(writeClient),
          new MetadataTableCompactionPlanHandler(StreamerUtil.createMetadataWriteClient(writeClient)));
    }
    if (needsMetadataCompaction) {
      return new MetadataTableCompactionPlanHandler(StreamerUtil.createMetadataWriteClient(writeClient));
    }
    return new DataTableCompactionPlanHandler(writeClient);
  }

  public static CompactHandler createCompactHandler(Configuration conf, RuntimeContext runtimeContext, int taskId) {
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, runtimeContext);
    boolean needsDataCompaction = OptionsResolver.needsAsyncCompaction(conf);
    boolean needsMetadataCompaction = OptionsResolver.needsAsyncMetadataCompaction(conf);
    if (needsDataCompaction && needsMetadataCompaction) {
      return new CompositeCompactHandler(
          new DataTableCompactHandler(writeClient, taskId),
          new MetadataTableCompactHandler(StreamerUtil.createMetadataWriteClient(writeClient), taskId));
    }
    if (needsMetadataCompaction) {
      return new MetadataTableCompactHandler(StreamerUtil.createMetadataWriteClient(writeClient), taskId);
    }
    return new DataTableCompactHandler(writeClient, taskId);
  }

  public static CompactionCommitHandler createCompactionCommitHandler(Configuration conf, RuntimeContext runtimeContext) {
    HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, runtimeContext);
    boolean needsDataCompaction = OptionsResolver.needsAsyncCompaction(conf);
    boolean needsMetadataCompaction = OptionsResolver.needsAsyncMetadataCompaction(conf);
    if (needsDataCompaction && needsMetadataCompaction) {
      return new CompositeCompactionCommitHandler(
          new DataTableCompactionCommitHandler(conf, writeClient),
          new MetadataTableCompactionCommitHandler(conf, StreamerUtil.createMetadataWriteClient(writeClient)));
    }
    if (needsMetadataCompaction) {
      return new MetadataTableCompactionCommitHandler(conf, StreamerUtil.createMetadataWriteClient(writeClient));
    }
    return new DataTableCompactionCommitHandler(conf, writeClient);
  }

  public static CleanHandler createCleanHandler(Configuration conf, HoodieFlinkWriteClient writeClient) {
    if (OptionsResolver.isStreamingIndexWriteEnabled(conf)) {
      return new CompositeCleanHandler(
          new DefaultCleanHandler(writeClient),
          new DefaultCleanHandler(StreamerUtil.createMetadataWriteClient(writeClient)));
    }
    return new DefaultCleanHandler(writeClient);
  }
}
