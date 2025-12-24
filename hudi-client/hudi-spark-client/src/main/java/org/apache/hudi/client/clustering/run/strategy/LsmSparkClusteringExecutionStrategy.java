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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.lsm.MergeSorter;
import org.apache.hudi.io.lsm.RecordReader;
import org.apache.hudi.io.lsm.SparkRecordMergeWrapper;
import org.apache.hudi.io.lsm.SparkRecordReader;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.LSM_CLUSTERING_OUT_PUT_LEVEL;
import static org.apache.hudi.config.HoodieClusteringConfig.LSM_CLUSTERING_USING_STREAMING_COPY;

public class LsmSparkClusteringExecutionStrategy<T> extends
    ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(LsmSparkClusteringExecutionStrategy.class);

  public LsmSparkClusteringExecutionStrategy(HoodieTable table,
                                             HoodieEngineContext engineContext,
                                             HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime) {
    ValidationUtils.checkArgument(getWriteConfig().getBooleanOrDefault("hoodie.datasource.write.row.writer.enable", false), "Please set hoodie.datasource.write.row.writer.enable true");

    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    final List<ClusteringGroupInfo> clusteringGroupInfos = clusteringPlan.getInputGroups().stream().map(ClusteringGroupInfo::create).collect(Collectors.toList());
    StructType structSchema = HoodieInternalRowUtils.getCachedSchema(schema);
    Function1<PartitionedFile, Iterator<InternalRow>> parquetReader = getParquetReader(engineContext, structSchema);

    int parallelism = clusteringGroupInfos.size();
    JavaRDD<ClusteringGroupInfo> groupInfosJavaRDD = engineContext.parallelize(clusteringGroupInfos, parallelism);
    LOG.info("number of partitions for clustering " + groupInfosJavaRDD.getNumPartitions());
    List<List<WriteStatus>> writeStatuses = groupInfosJavaRDD
        .map(groupInfo -> {
          return runClusteringForGroupAsyncAsRow(groupInfo, parquetReader, clusteringPlan.getStrategy().getStrategyParams(),
              Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(true), instantTime, getWriteConfig());
        }).collect();

    List<WriteStatus> statusList = writeStatuses.stream().flatMap(writeStatus -> writeStatus.stream()).collect(Collectors.toList());

    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(HoodieJavaRDD.of(statusList, (HoodieSparkEngineContext) getEngineContext(), parallelism));
    return writeMetadata;
  }

  protected List<WriteStatus> runClusteringForGroupAsyncAsRow(ClusteringGroupInfo clusteringGroupInfo,
                                                              Function1<PartitionedFile, Iterator<InternalRow>> parquetReader,
                                                              Map<String, String> strategyParams,
                                                              boolean shouldPreserveHoodieMetadata,
                                                              String instantTime,
                                                              HoodieWriteConfig writeConfig) {
    SparkAdapter sparkAdapter = SparkAdapterSupport$.MODULE$.sparkAdapter();
    List<ClusteringOperation> operations = clusteringGroupInfo.getOperations();
    final Schema schema = AvroSchemaCache.intern(HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(writeConfig.getSchema())));
    List<RecordReader<HoodieRecord>> fileReaders = operations.stream().map(operation -> {
      PartitionedFile partitionedFile = sparkAdapter.getSparkPartitionedFileUtils().createPartitionedFile(
          InternalRow.empty(), new Path(operation.getDataFilePath()), 0, Long.MAX_VALUE);
      try {
        Iterator<InternalRow> iterator = parquetReader.apply(partitionedFile);
        return new SparkRecordReader(iterator, schema);
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      }
    }).collect(Collectors.toList());

    // 判断是否是 major compaction
    boolean isIgnoreDelete = "1".equals(clusteringGroupInfo.getExtraMeta().get(LSM_CLUSTERING_OUT_PUT_LEVEL));
    HoodieRecordMerger recordMerger = writeConfig.getRecordMerger();
    TypedProperties props = HoodiePayloadConfig.newBuilder().withPayloadOrderingField(writeConfig.getPreCombineField()).build().getProps();
    StructType structSchema = HoodieInternalRowUtils.getCachedSchema(schema);
    RecordReader<InternalRow> reader = new MergeSorter().mergeSort(fileReaders, new SparkRecordMergeWrapper(isIgnoreDelete, recordMerger,
        schema, schema, props, structSchema), getHoodieSparkRecordComparator());

    try {
      Iterator<InternalRow> iterator = reader.read();
      return performClusteringWithRecordsAsRow(iterator, 1, instantTime, strategyParams, structSchema,
          null, shouldPreserveHoodieMetadata, clusteringGroupInfo.getExtraMeta());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected Function1<PartitionedFile, Iterator<InternalRow>> getParquetReader(JavaSparkContext jsc, StructType schema) {
    SQLContext sqlContext = new SQLContext(jsc.sc());
    Map<String, String> params = new HashMap<>();
    params.put("path", getHoodieTable().getMetaClient().getTableConfig().getBasePath());
    return HoodieSparkUtils.getParquetReader(sqlContext.sparkSession(), schema, params, new Configuration(jsc.hadoopConfiguration()));
  }

  public static Comparator<HoodieRecord> getHoodieSparkRecordComparator() {
    return new Comparator<HoodieRecord>() {
      @Override
      public int compare(HoodieRecord o1, HoodieRecord o2) {
        HoodieSparkRecord record1 = (HoodieSparkRecord) o1;
        HoodieSparkRecord record2 = (HoodieSparkRecord) o2;
        return record1.getUtf8RecordKey().compare(record2.getUtf8RecordKey());
      }
    };
  }

  public List<WriteStatus> performClusteringWithRecordsAsRow(Iterator<InternalRow> iterator,
                                                             int numOutputGroups,
                                                             String instantTime, Map<String, String> strategyParams,
                                                             StructType schema,
                                                             List<HoodieFileGroupId> fileGroupIdList,
                                                             boolean shouldPreserveHoodieMetadata,
                                                             Map<String, String> extraMetadata) {
    TypedProperties props = getWriteConfig().getProps();
    props.put(LSM_CLUSTERING_OUT_PUT_LEVEL, extraMetadata.get(LSM_CLUSTERING_OUT_PUT_LEVEL));
    props.put(LSM_CLUSTERING_USING_STREAMING_COPY, extraMetadata.get(LSM_CLUSTERING_USING_STREAMING_COPY));
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder()
        .withProps(props)
        .withOperation(WriteOperationType.CLUSTER.name())
        .build();
    newConfig.setValue(HoodieStorageConfig.PARQUET_RECORDKEY_BLOOM_FILTER_ENABLED, String.valueOf(getWriteConfig().parquetRecordkeyClusteringBloomFilterEnabled()));
    // inputRecords 已经有序且去重，直接写即可，不需要shuffle和merge
    return HoodieDatasetBulkInsertHelper.bulkInsertWithLsmClustering(iterator, schema, instantTime, getHoodieTable(), newConfig,
        true, shouldPreserveHoodieMetadata);
  }
}
