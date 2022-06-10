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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.FlinkHoodieIndexFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.specific.SpecificRecordBase;

import java.util.List;

import static org.apache.hudi.common.data.HoodieList.getList;

/**
 * Impl of a flink hoodie table.
 */
public abstract class HoodieFlinkTable<T>
    extends HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>>
    implements ExplicitWriteHandleTable<T> {

  protected HoodieFlinkTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static <T> HoodieFlinkTable<T> create(HoodieWriteConfig config, HoodieFlinkEngineContext context) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())))
            .setFileSystemRetryConfig(config.getFileSystemRetryConfig()).build();
    return HoodieFlinkTable.create(config, context, metaClient);
  }

  public static <T> HoodieFlinkTable<T> create(HoodieWriteConfig config,
                                               HoodieFlinkEngineContext context,
                                               HoodieTableMetaClient metaClient) {
    final HoodieFlinkTable<T> hoodieFlinkTable;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        hoodieFlinkTable = new HoodieFlinkCopyOnWriteTable<>(config, context, metaClient);
        break;
      case MERGE_ON_READ:
        hoodieFlinkTable = new HoodieFlinkMergeOnReadTable<>(config, context, metaClient);
        break;
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
    return hoodieFlinkTable;
  }

  public static HoodieWriteMetadata<List<WriteStatus>> convertMetadata(
      HoodieWriteMetadata<HoodieData<WriteStatus>> metadata) {
    return metadata.clone(metadata.getWriteStatuses().collectAsList());
  }

  @Override
  protected HoodieIndex getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return FlinkHoodieIndexFactory.createIndex((HoodieFlinkEngineContext) context, config);
  }

  /**
   * Fetch instance of {@link HoodieTableMetadataWriter}.
   *
   * @return instance of {@link HoodieTableMetadataWriter}
   */
  @Override
  public <T extends SpecificRecordBase> Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp,
                                                                                            Option<T> actionMetadata) {
    if (config.isMetadataTableEnabled()) {
      return Option.of(FlinkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config,
          context, actionMetadata, Option.of(triggeringInstantTimestamp)));
    } else {
      return Option.empty();
    }
  }
}
