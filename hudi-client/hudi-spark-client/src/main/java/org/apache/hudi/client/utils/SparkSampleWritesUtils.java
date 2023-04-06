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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.CachingPath;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.apache.hudi.common.table.HoodieTableMetaClient.SAMPLE_WRITES_FOLDER_PATH;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.config.HoodieCompactionConfig.COPY_ON_WRITE_RECORD_SIZE_ESTIMATE;
import static org.apache.hudi.config.HoodieWriteConfig.SAMPLE_WRITES_ENABLED;
import static org.apache.hudi.config.HoodieWriteConfig.SAMPLE_WRITES_SIZE;

/**
 * The utilities class is dedicated to estimating average record size by writing sample incoming records
 * to `.hoodie/.aux/.sample_writes/<instant time>/<epoch millis>` and reading the commit metadata.
 *
 * TODO handle sample_writes sub-path clean-up w.r.t. rollback and insert overwrite. (HUDI-6044)
 */
public class SparkSampleWritesUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSampleWritesUtils.class);

  public static void overwriteRecordSizeEstimateIfNeeded(JavaSparkContext jsc, JavaRDD<HoodieRecord> records, HoodieWriteConfig writeConfig, String instantTime) {
    if (!writeConfig.getBoolean(SAMPLE_WRITES_ENABLED)) {
      LOG.debug("Skip overwriting record size estimate as it's disabled.");
      return;
    }
    HoodieTableMetaClient metaClient = getMetaClient(jsc, writeConfig.getBasePath());
    if (metaClient.isTimelineNonEmpty()) {
      LOG.info("Skip overwriting record size estimate due to timeline is non-empty.");
      return;
    }
    try {
      Pair<Boolean, String> result = doSampleWrites(jsc, records, writeConfig, instantTime);
      if (result.getLeft()) {
        long avgSize = getAvgSizeFromSampleWrites(jsc, result.getRight());
        LOG.info("Overwriting record size estimate to " + avgSize);
        writeConfig.setValue(COPY_ON_WRITE_RECORD_SIZE_ESTIMATE, String.valueOf(avgSize));
      }
    } catch (IOException e) {
      LOG.error(String.format("Not overwriting record size estimate for table %s due to error when doing sample writes.", writeConfig.getTableName()), e);
    }
  }

  private static Pair<Boolean, String> doSampleWrites(JavaSparkContext jsc, JavaRDD<HoodieRecord> records, HoodieWriteConfig writeConfig, String instantTime) throws IOException {
    long now = Instant.now().toEpochMilli();
    Path basePath = new CachingPath(writeConfig.getBasePath(), SAMPLE_WRITES_FOLDER_PATH + Path.SEPARATOR + instantTime + Path.SEPARATOR + now);
    final String sampleWritesBasePath = basePath.toString();
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(String.format("%s_samples_%s_%s", writeConfig.getTableName(), instantTime, now))
        .setCDCEnabled(false)
        .initTable(jsc.hadoopConfiguration(), sampleWritesBasePath);
    HoodieWriteConfig sampleWriteConfig = HoodieWriteConfig.newBuilder()
        .withProps(writeConfig.getProps())
        .withPath(sampleWritesBasePath)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withSampleWritesEnabled(false)
        .withTableServicesEnabled(false)
        .withSchemaEvolutionEnable(false)
        .withBulkInsertParallelism(1)
        .withAutoCommit(true)
        .build();
    try (SparkRDDWriteClient sampleWriteClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), sampleWriteConfig, Option.empty())) {
      int size = writeConfig.getIntOrDefault(SAMPLE_WRITES_SIZE);
      List<HoodieRecord> samples = records.coalesce(1).take(size);
      sampleWriteClient.startCommitWithTime(instantTime);
      JavaRDD<WriteStatus> writeStatusRDD = sampleWriteClient.bulkInsert(jsc.parallelize(samples, 1), instantTime);
      if (writeStatusRDD.filter(WriteStatus::hasErrors).count() > 0) {
        LOG.error(String.format("sample writes for table %s failed with errors.", writeConfig.getTableName()));
        if (LOG.isTraceEnabled()) {
          LOG.trace("Printing out the top 100 errors");
          writeStatusRDD.filter(WriteStatus::hasErrors).take(100).forEach(ws -> {
            LOG.trace("Global error :", ws.getGlobalError());
            ws.getErrors().forEach((key, throwable) ->
                LOG.trace(String.format("Error for key: %s", key), throwable));
          });
        }
        return Pair.of(false, null);
      } else {
        return Pair.of(true, sampleWritesBasePath);
      }
    }
  }

  private static long getAvgSizeFromSampleWrites(JavaSparkContext jsc, String sampleWritesBasePath) throws IOException {
    HoodieTableMetaClient metaClient = getMetaClient(jsc, sampleWritesBasePath);
    Option<HoodieInstant> lastInstantOpt = metaClient.getCommitTimeline().filterCompletedInstants().lastInstant();
    checkState(lastInstantOpt.isPresent(), "The only completed instant should be present in sample_writes table.");
    HoodieInstant instant = lastInstantOpt.get();
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
        .fromBytes(metaClient.getCommitTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
    long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
    return (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
  }

  private static HoodieTableMetaClient getMetaClient(JavaSparkContext jsc, String basePath) {
    FileSystem fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    return HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(basePath).build();
  }
}
