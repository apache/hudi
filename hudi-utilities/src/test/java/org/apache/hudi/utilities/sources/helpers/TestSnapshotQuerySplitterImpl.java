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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer.QueryContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

/**
 * An implementation of {@link SnapshotLoadQuerySplitter} that limits the number of rows
 * ingested per batch based on `test.snapshot.load.max.row.count` config.
 */
public class TestSnapshotQuerySplitterImpl extends SnapshotLoadQuerySplitter {

  public static final String MAX_ROWS_PER_BATCH = "test.snapshot.load.max.row.count";

  /**
   * Constructor initializing the properties.
   *
   * @param properties Configuration properties for the splitter.
   */
  public TestSnapshotQuerySplitterImpl(TypedProperties properties) {
    super(properties);
  }

  @Override
  public Option<CheckpointWithPredicates> getNextCheckpointWithPredicates(Dataset<Row> df, QueryContext queryContext) {
    int maxRowsPerBatch = properties.getInteger(MAX_ROWS_PER_BATCH, 1);
    List<String> instantTimeList = queryContext.getInstantTimeList();
    Map<String, String> instantToCompletionTimeMap = queryContext.getInstants().stream()
        .collect(Collectors.toMap(HoodieInstant::requestedTime, HoodieInstant::getCompletionTime));
    Map<String, Triple<Long, String, String>> completionTimeToStats =
        df.select(col(COMMIT_TIME_METADATA_FIELD), col(PARTITION_PATH_METADATA_FIELD))
            .filter(col(COMMIT_TIME_METADATA_FIELD).isin(instantTimeList.toArray()))
            .groupBy(col(COMMIT_TIME_METADATA_FIELD))
            .agg(
                count(COMMIT_TIME_METADATA_FIELD).alias("count"),
                min(PARTITION_PATH_METADATA_FIELD).alias("min_partition_path"),
                max(PARTITION_PATH_METADATA_FIELD).alias("max_partition_path"))
            .collectAsList().stream()
            .collect(Collectors.toMap(
                row -> instantToCompletionTimeMap.get(row.getString(0)),
                row -> Triple.of(row.getLong(1), row.getString(2), row.getString(3))));
    if (!completionTimeToStats.isEmpty()) {
      List<String> sortedCompletionTime =
          completionTimeToStats.keySet().stream().sorted().collect(Collectors.toList());
      long rowCount = 0;
      String minPartitionPath = null;
      String maxPartitionPath = null;
      String endCompletionTime = queryContext.getMaxCompletionTime();
      for (String completionTime : sortedCompletionTime) {
        Triple<Long, String, String> stats = completionTimeToStats.get(completionTime);
        endCompletionTime = completionTime;
        rowCount += stats.getLeft();
        if (minPartitionPath == null || stats.getMiddle().compareTo(minPartitionPath) < 0) {
          minPartitionPath = stats.getMiddle();
        }

        if (maxPartitionPath == null || stats.getRight().compareTo(maxPartitionPath) > 0) {
          maxPartitionPath = stats.getRight();
        }

        if (rowCount >= maxRowsPerBatch) {
          break;
        }
      }

      String partitionFilter = String.format(
          "partition_path >= '%s' and partition_path <= '%s'", minPartitionPath, maxPartitionPath);
      return Option.of(new CheckpointWithPredicates(endCompletionTime, partitionFilter));
    } else {
      return Option.empty();
    }
  }
}
