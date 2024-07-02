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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

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
  public Option<String> getNextCheckpoint(Dataset<Row> df, String beginCheckpointStr, Option<SourceProfileSupplier> sourceProfileSupplierOption) {
    List<Row> row = df.filter(col(COMMIT_TIME_METADATA_FIELD).gt(lit(beginCheckpointStr)))
        .orderBy(col(COMMIT_TIME_METADATA_FIELD)).limit(1).collectAsList();
    return Option.ofNullable(row.size() > 0 ? row.get(0).getAs(COMMIT_TIME_METADATA_FIELD) : null);
  }

  @Override
  public Option<CheckpointWithPredicates> getNextCheckpointWithPredicates(Dataset<Row> df, String beginCheckpointStr) {
    int maxRowsPerBatch = properties.getInteger(MAX_ROWS_PER_BATCH, 1);
    List<Row> row = df.select(col(COMMIT_TIME_METADATA_FIELD)).filter(col(COMMIT_TIME_METADATA_FIELD).gt(lit(beginCheckpointStr)))
        .orderBy(col(COMMIT_TIME_METADATA_FIELD)).limit(maxRowsPerBatch).collectAsList();
    if (!row.isEmpty()) {
      String endInstant = row.get(row.size() - 1).getAs(COMMIT_TIME_METADATA_FIELD);
      List<Row> minMax =
          df.filter(col(COMMIT_TIME_METADATA_FIELD).gt(lit(beginCheckpointStr)))
              .filter(col(COMMIT_TIME_METADATA_FIELD).leq(endInstant))
              .select(PARTITION_PATH_METADATA_FIELD).agg(min(PARTITION_PATH_METADATA_FIELD).alias("min_partition_path"), max(PARTITION_PATH_METADATA_FIELD).alias("max_partition_path"))
              .collectAsList();
      String partitionFilter = String.format("partition_path >= '%s' and partition_path <= '%s'", minMax.get(0).getAs("min_partition_path"), minMax.get(0).getAs("max_partition_path"));
      return Option.of(new CheckpointWithPredicates(endInstant, partitionFilter));
    } else {
      return Option.empty();
    }
  }
}
