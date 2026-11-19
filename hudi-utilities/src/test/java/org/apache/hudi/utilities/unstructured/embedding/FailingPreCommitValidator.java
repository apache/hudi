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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.validator.SparkPreCommitValidator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test pre-commit validator that fails every commit it runs for, AFTER the write: Hudi runs pre-commit validators once the
 * round's files are written and before the instant completes, so the instant stays INFLIGHT with its files on storage.
 * {@link #RECORDS_WRITTEN} holds what the failed commit wrote, from its write stats, so a test can show its data was on storage.
 */
public class FailingPreCommitValidator<T, I, K, O extends HoodieData<WriteStatus>> extends SparkPreCommitValidator<T, I, K, O> {

  /** Records of the pending commit the last run saw (local mode: the driver JVM runs the validator). */
  public static final AtomicLong RECORDS_WRITTEN = new AtomicLong(-1);

  public FailingPreCommitValidator(HoodieSparkTable<T> table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    super(table, engineContext, config);
  }

  @Override
  public void validate(String instantTime, HoodieWriteMetadata<O> writeResult, Dataset<Row> before, Dataset<Row> after) {
    // the write stats of the files this commit wrote (a MOR upsert writes log files, which the pending-commit snapshot omits)
    RECORDS_WRITTEN.set(writeResult.getWriteStats().get().stream().mapToLong(HoodieWriteStat::getNumWrites).sum());
    throw new HoodieValidationException("injected pre-commit failure after the round's files were written");
  }

  @Override
  protected void validateRecordsBeforeAndAfter(Dataset<Row> before, Dataset<Row> after, Set<String> partitionsAffected) {
    throw new HoodieValidationException("injected pre-commit failure after the round's files were written");
  }
}
