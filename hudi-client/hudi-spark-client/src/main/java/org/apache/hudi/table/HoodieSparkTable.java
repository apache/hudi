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

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
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
import org.apache.hudi.index.SparkHoodieIndexFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

import static org.apache.hudi.data.HoodieJavaRDD.getJavaRDD;

public abstract class HoodieSparkTable<T extends HoodieRecordPayload>
    extends HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private volatile boolean isMetadataTableExists = false;

  protected HoodieSparkTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config, HoodieEngineContext context) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
            .setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
            .setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))).build();
    return HoodieSparkTable.create(config, (HoodieSparkEngineContext) context, metaClient);
  }

  public static <T extends HoodieRecordPayload> HoodieSparkTable<T> create(HoodieWriteConfig config,
                                                                           HoodieSparkEngineContext context,
                                                                           HoodieTableMetaClient metaClient) {
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
    if (metaClient.getTableConfig().isMetadataTableEnabled()) {
      hoodieSparkTable.getHoodieView().sync();
    }
    return hoodieSparkTable;
  }

  public static HoodieWriteMetadata<JavaRDD<WriteStatus>> convertMetadata(
      HoodieWriteMetadata<HoodieData<WriteStatus>> metadata) {
    return metadata.clone(getJavaRDD(metadata.getWriteStatuses()));
  }

  @Override
  protected HoodieIndex getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return SparkHoodieIndexFactory.createIndex(config);
  }

  /**
   * Fetch instance of {@link HoodieTableMetadataWriter}.
   *
   * @return instance of {@link HoodieTableMetadataWriter}
   */
  @Override
  public <T extends SpecificRecordBase> Option<HoodieTableMetadataWriter> getMetadataWriter(String triggeringInstantTimestamp,
                                                                                            Option<T> actionMetadata) {
    if (metaClient.getTableConfig().isMetadataTableEnabled() || config.isMetadataTableEnabled()) {
      // Create the metadata table writer. First time after the upgrade this creation might trigger
      // metadata table bootstrapping. Bootstrapping process could fail and checking the table
      // existence after the creation is needed.
      final HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(
          context.getHadoopConf().get(), config, context, actionMetadata, Option.of(triggeringInstantTimestamp));
      try {
        if (isMetadataTableExists || metaClient.getTableConfig().isMetadataTableEnabled()
            || metaClient.getFs().exists(new Path(HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath())))) {
          isMetadataTableExists = true;
          return Option.of(metadataWriter);
        }
      } catch (IOException e) {
        throw new HoodieMetadataException("Checking existence of metadata table failed", e);
      }
    }

    return Option.empty();
  }
}
