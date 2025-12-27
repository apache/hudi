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

package org.apache.hudi.client.utils;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTablePreCommitFileSystemView;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;
import org.apache.hudi.util.JavaScalaConverters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Spark validator utils to verify and run any pre-commit validators configured.
 */
public class SparkValidatorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSparkCommitActionExecutor.class);

  /**
   * Check configured pre-commit validators and run them. Note that this only works for COW tables
   * <p>
   * Throw error if there are validation failures.
   */
  public static void runValidators(HoodieWriteConfig config,
                                   HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata,
                                   HoodieEngineContext context,
                                   HoodieTable table,
                                   String instantTime) {
    if (StringUtils.isNullOrEmpty(config.getPreCommitValidators())) {
      LOG.info("no validators configured.");
    } else {
      if (!writeMetadata.getWriteStats().isPresent()) {
        writeMetadata.setWriteStats(writeMetadata.getWriteStatuses().map(WriteStatus::getStat).collectAsList());
      }
      Set<String> partitionsModified = writeMetadata.getWriteStats().get().stream().map(HoodieWriteStat::getPartitionPath).collect(Collectors.toSet());
      SQLContext sqlContext = SQLContext.getOrCreate(HoodieSparkEngineContext.getSparkContext(context).sc());
      // Refresh timeline to ensure validator sees the any other operations done on timeline (async operations such as other clustering/compaction/rollback)
      table.getMetaClient().reloadActiveTimeline();
      Dataset<Row> afterState = getRecordsFromPendingCommits(sqlContext, partitionsModified, writeMetadata, table, instantTime);
      Dataset<Row> beforeState = getRecordsFromCommittedFiles(sqlContext, partitionsModified, table, afterState.schema());

      Stream<SparkPreCommitValidator> validators = Arrays.stream(config.getPreCommitValidators().split(","))
          .map(validatorClass -> ((SparkPreCommitValidator) ReflectionUtils.loadClass(validatorClass,
              new Class<?>[] {HoodieSparkTable.class, HoodieEngineContext.class, HoodieWriteConfig.class},
              table, context, config)));

      boolean allSuccess = validators.map(v -> runValidatorAsync(v, writeMetadata, beforeState, afterState, instantTime)).map(CompletableFuture::join)
          .reduce(true, Boolean::logicalAnd);

      if (allSuccess) {
        LOG.info("All validations succeeded");
      } else {
        LOG.error("At least one pre-commit validation failed");
        throw new HoodieValidationException("At least one pre-commit validation failed");
      }
    }
  }

  /**
   * Run validators in a separate thread pool for parallelism. Each of validator can submit a distributed spark job if needed.
   */
  private static CompletableFuture<Boolean> runValidatorAsync(SparkPreCommitValidator validator, HoodieWriteMetadata<?> writeMetadata,
                                                              Dataset<Row> beforeState, Dataset<Row> afterState, String instantTime) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        validator.validate(instantTime, writeMetadata, beforeState, afterState);
        LOG.info("validation complete for {}", validator.getClass().getName());
        return true;
      } catch (HoodieValidationException e) {
        LOG.error("validation failed for {}", validator.getClass().getName(), e);
        return false;
      }
    });
  }

  /**
   * Get records from partitions modified as a dataset.
   * Note that this only works for COW tables.
   *
   * @param sqlContext          Spark {@link SQLContext} instance.
   * @param partitionsAffected  A set of affected partitions.
   * @param table               {@link HoodieTable} instance.
   * @param newStructTypeSchema The {@link StructType} schema from after state.
   * @return The records in Dataframe from committed files.
   */
  public static Dataset<Row> getRecordsFromCommittedFiles(SQLContext sqlContext,
                                                          Set<String> partitionsAffected,
                                                          HoodieTable table,
                                                          StructType newStructTypeSchema) {
    List<String> committedFiles = partitionsAffected.stream()
        .flatMap(partition -> table.getBaseFileOnlyView().getLatestBaseFiles(partition).map(BaseFile::getPath))
        .collect(Collectors.toList());

    if (committedFiles.isEmpty()) {
      try {
        return sqlContext.createDataFrame(
            sqlContext.emptyDataFrame().rdd(),
            AvroConversionUtils.convertAvroSchemaToStructType(
                new TableSchemaResolver(table.getMetaClient()).getTableSchema().toAvroSchema()));
      } catch (Exception e) {
        LOG.warn("Cannot get table schema from before state.", e);
        LOG.warn("Using the schema from after state (current transaction) to create the empty Spark dataframe: {}", newStructTypeSchema);
        return sqlContext.createDataFrame(
            sqlContext.emptyDataFrame().rdd(), newStructTypeSchema);
      }
    }
    return readRecordsForBaseFiles(sqlContext, committedFiles);
  }

  /**
   * Get records from specified list of data files.
   */
  public static Dataset<Row> readRecordsForBaseFiles(SQLContext sqlContext, List<String> baseFilePaths) {
    return sqlContext.read().parquet(JavaScalaConverters.convertJavaListToScalaSeq(baseFilePaths));
  }

  /**
   * Get reads from partitions modified including any inflight commits.
   * Note that this only works for COW tables
   */
  public static Dataset<Row> getRecordsFromPendingCommits(SQLContext sqlContext, 
                                                          Set<String> partitionsAffected, 
                                                          HoodieWriteMetadata<?> writeMetadata,
                                                          HoodieTable table,
                                                          String instantTime) {

    // build file system view with pending commits
    HoodieTablePreCommitFileSystemView fsView = new HoodieTablePreCommitFileSystemView(table.getMetaClient(),
        table.getHoodieView(),
        writeMetadata.getWriteStats().get(),
        writeMetadata.getPartitionToReplaceFileIds(),
        instantTime);

    List<String> newFiles = partitionsAffected.stream()
        .flatMap(partition ->  fsView.getLatestBaseFiles(partition).map(BaseFile::getPath))
        .collect(Collectors.toList());

    if (newFiles.isEmpty()) {
      return sqlContext.emptyDataFrame();
    }

    return readRecordsForBaseFiles(sqlContext, newFiles);
  }
}
