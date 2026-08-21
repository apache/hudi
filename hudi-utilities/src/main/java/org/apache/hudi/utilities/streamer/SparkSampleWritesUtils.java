/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.internal.config.Network$;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.hudi.common.table.HoodieTableMetaClient.SAMPLE_WRITES_FOLDER_PATH;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SAMPLE_WRITES_ENABLED;
import static org.apache.hudi.utilities.config.HoodieStreamerConfig.SAMPLE_WRITES_SIZE;

/**
 * The utilities class is dedicated to estimating average record size by writing sample incoming records
 * to `.hoodie/.aux/.sample_writes/<instant time>/<epoch millis>` and reading the commit metadata.
 * <p>
 * TODO handle sample_writes sub-path clean-up w.r.t. rollback and insert overwrite. (HUDI-6044)
 */
@Slf4j
public class SparkSampleWritesUtils {

  /**
   * The sampled records are shipped to the executor inside a single Spark task
   * ({@code jsc.parallelize(samples, 1)}). If their total serialized size approaches the RPC frame
   * limit, launching that task fails with "exceeds max allowed: spark.rpc.message.maxSize". We
   * therefore cap the sample at this fraction of {@code spark.rpc.message.maxSize}; the remaining
   * headroom absorbs the task closure, RDD metadata, and the difference between the Kryo estimate
   * used here and the serializer Spark actually uses when shipping the task.
   */
  private static final double SAMPLE_WRITES_TASK_BYTES_FRACTION = 0.5;

  private static final long BYTES_PER_MB = 1048576;

  public static Option<HoodieWriteConfig> getWriteConfigWithRecordSizeEstimate(JavaSparkContext jsc, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieWriteConfig writeConfig) {
    if (!writeConfig.getBoolean(SAMPLE_WRITES_ENABLED)) {
      log.debug("Skip overwriting record size estimate as it's disabled.");
      return Option.empty();
    }
    HoodieTableMetaClient metaClient = getMetaClient(jsc, writeConfig.getBasePath());
    if (metaClient.isTimelineNonEmpty()) {
      log.info("Skip overwriting record size estimate due to timeline is non-empty.");
      return Option.empty();
    }
    try {
      Pair<Boolean, String> result = doSampleWrites(jsc, recordsOpt, writeConfig);
      if (result.getLeft()) {
        long avgSize = getAvgSizeFromSampleWrites(jsc, result.getRight());
        log.info("Overwriting record size estimate to {}", avgSize);
        TypedProperties props = writeConfig.getProps();
        props.put(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(avgSize));
        return Option.of(HoodieWriteConfig.newBuilder().withProperties(props).build());
      }
    } catch (IOException e) {
      log.error("Not overwriting record size estimate for table {} due to error when doing sample writes.", writeConfig.getTableName(), e);
    }
    return Option.empty();
  }

  private static Pair<Boolean, String> doSampleWrites(JavaSparkContext jsc, Option<JavaRDD<HoodieRecord>> recordsOpt, HoodieWriteConfig writeConfig)
      throws IOException {
    String uniqueId = UUID.randomUUID().toString();
    final String sampleWritesBasePath = getSampleWritesBasePath(jsc, writeConfig, uniqueId);
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(String.format("%s_samples_%s", writeConfig.getTableName(), uniqueId))
        .setCDCEnabled(false)
        .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), sampleWritesBasePath);
    TypedProperties props = writeConfig.getProps();
    props.put(SAMPLE_WRITES_ENABLED.key(), "false");
    final HoodieWriteConfig sampleWriteConfig = HoodieWriteConfig.newBuilder()
        .withProps(props)
        .withTableServicesEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withSchemaEvolutionEnable(false)
        .withBulkInsertParallelism(1)
        .withPath(sampleWritesBasePath)
        .build();
    Pair<Boolean, String> emptyRes = Pair.of(false, null);
    try (SparkRDDWriteClient sampleWriteClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), sampleWriteConfig, Option.empty())) {
      int size = writeConfig.getIntOrDefault(SAMPLE_WRITES_SIZE);
      long maxSampleBytes = resolveMaxSampleBytes(jsc);
      return recordsOpt.map(records -> {
        // Collapse to a single partition, then take a sample bounded by both record count and total
        // serialized bytes on the executor. The sample is later shipped inside one Spark task, so
        // bounding its serialized size keeps that task under spark.rpc.message.maxSize even when the
        // source records are large.
        List<HoodieRecord> samples = records.coalesce(1)
            .mapPartitions((FlatMapFunction<Iterator<HoodieRecord>, HoodieRecord>) sourceRecords ->
                takeBoundedSample(sourceRecords, size, maxSampleBytes))
            .collect();
        if (samples.isEmpty()) {
          return emptyRes;
        }
        String instantTime = sampleWriteClient.startCommit();
        JavaRDD<WriteStatus> writeStatusRDD = sampleWriteClient.bulkInsert(jsc.parallelize(samples, 1), instantTime);
        if (writeStatusRDD.filter(WriteStatus::hasErrors).count() > 0) {
          log.error("sample writes for table {} failed with errors.", writeConfig.getTableName());
          if (log.isTraceEnabled()) {
            log.trace("Printing out the top 100 errors");
            writeStatusRDD.filter(WriteStatus::hasErrors).take(100).forEach(ws -> {
              log.trace("Global error :", ws.getGlobalError());
              ws.getErrors().forEach((key, throwable) ->
                  log.trace("Error for key: {}", key, throwable));
            });
          }
          return emptyRes;
        } else {
          sampleWriteClient.commit(instantTime, writeStatusRDD);
          return Pair.of(true, sampleWritesBasePath);
        }
      }).orElse(emptyRes);
    }
  }

  /**
   * Resolves the maximum total serialized size (in bytes) allowed for the sampled records, derived
   * as {@link #SAMPLE_WRITES_TASK_BYTES_FRACTION} of the cluster's {@code spark.rpc.message.maxSize}
   * (read from {@link org.apache.spark.internal.config.Network#RPC_MESSAGE_MAX_SIZE}, which also
   * supplies Spark's default). Deriving it from the RPC limit keeps the bound correct if operators
   * raise that limit.
   */
  private static long resolveMaxSampleBytes(JavaSparkContext jsc) {
    int rpcMaxSizeMb = (Integer) jsc.getConf().get(Network$.MODULE$.RPC_MESSAGE_MAX_SIZE());
    return (long) (rpcMaxSizeMb * BYTES_PER_MB * SAMPLE_WRITES_TASK_BYTES_FRACTION);
  }

  /**
   * Takes up to {@code maxCount} records from {@code sourceRecords}, stopping early once the
   * accumulated serialized size would exceed {@code maxBytes}. Each sampled record is re-keyed with
   * an empty partition path so the whole sample writes to a single non-partitioned file, instead of
   * fanning out into one tiny file per source partition and skewing the estimate. At least one
   * record is always retained so a single oversized record still yields an estimate.
   *
   * @param sourceRecords the source records to sample from
   * @param maxCount      the maximum number of records to sample
   * @param maxBytes      the maximum total serialized size (in bytes) of the sampled records
   * @return an iterator over the bounded sample
   */
  static Iterator<HoodieRecord> takeBoundedSample(Iterator<HoodieRecord> sourceRecords, int maxCount, long maxBytes) throws IOException {
    List<HoodieRecord> samples = new ArrayList<>();
    long accumulatedBytes = 0L;
    while (sourceRecords.hasNext() && samples.size() < maxCount) {
      HoodieRecord source = sourceRecords.next();
      HoodieRecord sample = source.newInstance(new HoodieKey(source.getRecordKey(), ""));
      long recordBytes = SerializationUtils.serialize(sample).length;
      if (!samples.isEmpty() && accumulatedBytes + recordBytes > maxBytes) {
        break;
      }
      samples.add(sample);
      accumulatedBytes += recordBytes;
    }
    return samples.iterator();
  }

  private static String getSampleWritesBasePath(JavaSparkContext jsc, HoodieWriteConfig writeConfig, String uniqueId) throws IOException {
    StoragePath basePath = new StoragePath(writeConfig.getBasePath(), SAMPLE_WRITES_FOLDER_PATH + StoragePath.SEPARATOR + uniqueId);
    HoodieStorage storage = getMetaClient(jsc, writeConfig.getBasePath()).getStorage();
    if (storage.exists(basePath)) {
      storage.deleteDirectory(basePath);
    }
    return basePath.toString();
  }

  private static long getAvgSizeFromSampleWrites(JavaSparkContext jsc, String sampleWritesBasePath) throws IOException {
    HoodieTableMetaClient metaClient = getMetaClient(jsc, sampleWritesBasePath);
    Option<HoodieInstant> lastInstantOpt = metaClient.getCommitTimeline().filterCompletedInstants().lastInstant();
    checkState(lastInstantOpt.isPresent(), "The only completed instant should be present in sample_writes table.");
    HoodieInstant instant = lastInstantOpt.get();
    HoodieCommitMetadata commitMetadata =
        metaClient.getCommitTimeline().readCommitMetadata(instant);
    long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
    long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
    return (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
  }

  private static HoodieTableMetaClient getMetaClient(JavaSparkContext jsc, String basePath) {
    FileSystem fs = HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration());
    return HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(fs.getConf())).setBasePath(basePath).build();
  }
}
