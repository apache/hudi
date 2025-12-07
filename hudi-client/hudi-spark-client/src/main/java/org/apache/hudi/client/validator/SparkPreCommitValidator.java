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

package org.apache.hudi.client.validator;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validator can be configured pre-commit. 
 */
public abstract class SparkPreCommitValidator<T, I, K, O extends HoodieData<WriteStatus>> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkPreCommitValidator.class);

  private final HoodieSparkTable<T> table;
  @Getter
  private final HoodieEngineContext engineContext;
  @Getter
  private final HoodieWriteConfig writeConfig;
  private final HoodieMetrics metrics;

  protected SparkPreCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.table = table;
    this.engineContext = engineContext;
    this.writeConfig = writeConfig;
    this.metrics = new HoodieMetrics(writeConfig, table.getStorage());
  }
  
  protected Set<String> getPartitionsModified(HoodieWriteMetadata<O> writeResult) {
    Set<String> partitionsModified;
    if (writeResult.getWriteStats().isPresent()) {
      partitionsModified = writeResult.getWriteStats().get().stream().map(HoodieWriteStat::getPartitionPath).collect(Collectors.toSet());
    } else {
      partitionsModified = new HashSet<>(writeResult.getWriteStatuses().map(WriteStatus::getPartitionPath).collectAsList());
    }
    return partitionsModified;
  }

  /**
   * Verify the data written as part of specified instant. 
   * Throw HoodieValidationException if any unexpected data is written (Example: data files are not readable for some reason).
   */
  public void validate(String instantTime, HoodieWriteMetadata<O> writeResult, Dataset<Row> before, Dataset<Row> after) throws HoodieValidationException {
    HoodieTimer timer = HoodieTimer.start();
    try {
      validateRecordsBeforeAndAfter(before, after, getPartitionsModified(writeResult));
    } finally {
      long duration = timer.endTimer();
      LOG.info(getClass() + " validator took " + duration + " ms" + ", metrics on? " + getWriteConfig().isMetricsOn());
      publishRunStats(instantTime, duration);
    }
  }

  /**
   * Publish pre-commit validator run stats for a given commit action.
   */
  private void publishRunStats(String instantTime, long duration) {
    // Record validator duration metrics.
    if (getWriteConfig().isMetricsOn()) {
      HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
      Option<HoodieInstant> currentInstant = metaClient.getActiveTimeline()
          .findInstantsAfterOrEquals(instantTime, 1)
          .firstInstant();
      metrics.reportMetrics(currentInstant.get().getAction(), getClass().getSimpleName(), duration);
    }
  }

  /**
   * Takes input of RDD 1) before clustering and 2) after clustering. Perform required validation 
   * and throw error if validation fails
   */
  protected abstract void validateRecordsBeforeAndAfter(Dataset<Row> before,
                                                        Dataset<Row> after,
                                                        Set<String> partitionsAffected);

  public HoodieTable getHoodieTable() {
    return this.table;
  }

  protected Dataset<Row> executeSqlQuery(SQLContext sqlContext,
                                         String sqlQuery,
                                         String tableName,
                                         String logLabel) {
    String queryWithTempTableName = sqlQuery.replaceAll(
        HoodiePreCommitValidatorConfig.VALIDATOR_TABLE_VARIABLE, tableName);
    LOG.info("Running query ({}): {}", logLabel, queryWithTempTableName);
    return sqlContext.sql(queryWithTempTableName);
  }
}
