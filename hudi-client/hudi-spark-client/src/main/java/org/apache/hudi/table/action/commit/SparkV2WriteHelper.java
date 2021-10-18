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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.HoodieRowWriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.table.HoodieBaseTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.Duration;
import java.time.Instant;

public class SparkV2WriteHelper {

  private SparkV2WriteHelper() {
  }

  private static class WriteHelperHolder {

    private static final SparkV2WriteHelper HELPER = new SparkV2WriteHelper();
  }

  public static SparkV2WriteHelper newInstance() {
    return WriteHelperHolder.HELPER;
  }

  public HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> write(
      String instantTime,
      Dataset<Row> inputDf,
      HoodieEngineContext context,
      HoodieBaseTable<Dataset<Row>, Dataset<HoodieKey>, Dataset<HoodieRowWriteStatus>> table,
      boolean shouldCombine,
      int shuffleParallelism,
      SparkV2UpsertCommitActionExecutor executor,
      boolean performTagging) {
    Dataset<Row> dedupedDf = combineOnCondition(shouldCombine, inputDf, shuffleParallelism, table);
    Instant lookupBegin = Instant.now();
    Dataset<Row> taggedDf = tagOnCondition(performTagging, dedupedDf, context, table);
    Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());
    HoodieWriteMetadata<Dataset<HoodieRowWriteStatus>> result = executor.execute(taggedDf);
    result.setIndexLookupDuration(indexLookupDuration);
    return result;
  }

  private Dataset<Row> combineOnCondition(
      boolean condition, Dataset<Row> df, int parallelism, HoodieBaseTable<Dataset<Row>, Dataset<HoodieKey>, Dataset<HoodieRowWriteStatus>> table) {
    return condition ? deduplicateRecords(df) : df;
  }

  private Dataset<Row> deduplicateRecords(Dataset<Row> df) {
    // TODO(rxu) impl. dedup
    return df;
  }

  private Dataset<Row> tagOnCondition(boolean condition, Dataset<Row> dedupedDf, HoodieEngineContext context,
      HoodieBaseTable<Dataset<Row>, Dataset<HoodieKey>, Dataset<HoodieRowWriteStatus>> table) {
    return condition ? table.getIndexDelegate().tagLocation(dedupedDf, context, table) : dedupedDf;
  }
}
