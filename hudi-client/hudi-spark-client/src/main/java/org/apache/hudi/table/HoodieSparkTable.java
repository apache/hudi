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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

public abstract class HoodieSparkTable<T extends HoodieRecordPayload>
    extends HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private boolean isMetadataAvailabilityUpdated = false;
  private boolean isMetadataTableAvailable;

  protected HoodieSparkTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context) {
    return create(config, context, false);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context,
                                                                           boolean refreshTimeline) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    return HoodieSparkTable.create(config, (HoodieSparkEngineContext) context, metaClient, refreshTimeline);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config,
                                                                           HoodieSparkEngineContext context,
                                                                           HoodieTableMetaClient metaClient) {
    return create(config, context, metaClient, false);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config,
                                                                           HoodieSparkEngineContext context,
                                                                           HoodieTableMetaClient metaClient,
                                                                           boolean refreshTimeline) {
    HoodieSparkTable hoodieSparkTable;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        hoodieSparkTable = new HoodieSparkCopyOnWriteTable<>(config, context, metaClient);
        break;
      case MERGE_ON_READ:
        hoodieSparkTable = new HoodieSparkMergeOnReadTable<>(config, context, metaClient);
        break;
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
    if (refreshTimeline) {
      hoodieSparkTable.getHoodieView().sync();
    }
    return hoodieSparkTable;
  }

  @Override
  protected HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return SparkHoodieIndex.createIndex(config);
  }

  /**
   * Fetch instance of {@link HoodieTableMetadataWriter}.
   *
   * @return instance of {@link HoodieTableMetadataWriter}
   */
  @Override
  public Option<HoodieTableMetadataWriter> getMetadataWriter() {
    synchronized (this) {
      if (!isMetadataAvailabilityUpdated) {
        // this code assumes that if metadata availability is updated once it will not change. please revisit this logic if that's not the case.
        // this is done to avoid repeated calls to fs.exists().
        try {
          isMetadataTableAvailable = config.isMetadataTableEnabled()
              && metaClient.getFs().exists(new Path(HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath())));
        } catch (IOException e) {
          throw new HoodieMetadataException("Checking existence of metadata table failed", e);
        }
        isMetadataAvailabilityUpdated = true;
      }
    }
    if (isMetadataTableAvailable) {
      return Option.of(SparkHoodieBackedTableMetadataWriter.create(context.getHadoopConf().get(), config, context));
    } else {
      return Option.empty();
    }
  }
}
