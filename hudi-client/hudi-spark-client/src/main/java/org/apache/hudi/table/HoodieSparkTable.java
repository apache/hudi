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

import org.apache.hudi.SparkHoodieRDDData;
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
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import static org.apache.hudi.table.action.commit.SparkCommitHelper.getRdd;

public abstract class HoodieSparkTable<T extends HoodieRecordPayload>
    extends HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

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
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieSparkCopyOnWriteTable<>(config, context, metaClient);
      case MERGE_ON_READ:
        return new HoodieSparkMergeOnReadTable<>(config, context, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  public static <T extends HoodieRecordPayload> Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> convertBulkInsertPartitioner(
      Option<BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner) {
    Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> partitionerOption = Option.empty();
    if (userDefinedBulkInsertPartitioner.isPresent()) {
      partitionerOption = Option.of(convertBulkInsertPartitioner(userDefinedBulkInsertPartitioner.get()));
    }
    return partitionerOption;
  }

  public static <T extends HoodieRecordPayload> BulkInsertPartitioner<HoodieData<HoodieRecord<T>>> convertBulkInsertPartitioner(
      BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> bulkInsertPartitioner) {
    return new BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>() {
      @Override
      public HoodieData<HoodieRecord<T>> repartitionRecords(
          HoodieData<HoodieRecord<T>> records, int outputSparkPartitions) {
        return SparkHoodieRDDData.of(bulkInsertPartitioner.repartitionRecords(
            getRdd(records), outputSparkPartitions));
      }

      @Override
      public boolean arePartitionRecordsSorted() {
        return bulkInsertPartitioner.arePartitionRecordsSorted();
      }
    };
  }

  public static HoodieWriteMetadata<JavaRDD<WriteStatus>> convertMetadata(
      HoodieWriteMetadata<HoodieData<WriteStatus>> metadata) {
    return metadata.clone(getRdd(metadata.getWriteStatuses()));
  }

  public static HoodieBootstrapWriteMetadata<JavaRDD<WriteStatus>> convertBootstrapMetadata(
      HoodieBootstrapWriteMetadata<HoodieData<WriteStatus>> metadata) {
    Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> metadataBootstrapResult =
        metadata.getMetadataBootstrapResult();
    Option<HoodieWriteMetadata<HoodieData<WriteStatus>>> fullBootstrapResult =
        metadata.getFullBootstrapResult();
    Option<HoodieWriteMetadata<JavaRDD<WriteStatus>>> newMetadataBootstrapResult = Option.empty();
    Option<HoodieWriteMetadata<JavaRDD<WriteStatus>>> newFullBootstrapResult = Option.empty();
    if (metadataBootstrapResult.isPresent()) {
      newMetadataBootstrapResult = Option.of(metadataBootstrapResult.get()
          .clone(getRdd(metadataBootstrapResult.get().getWriteStatuses())));
    }
    if (fullBootstrapResult.isPresent()) {
      newFullBootstrapResult = Option.of(fullBootstrapResult.get()
          .clone(getRdd(fullBootstrapResult.get().getWriteStatuses())));
    }

    return new HoodieBootstrapWriteMetadata<>(
        newMetadataBootstrapResult, newFullBootstrapResult);
  }

  @Override
  protected HoodieIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
    return SparkHoodieIndex.createIndex(config);
  }
}
