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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * An implementation of {@link SnapshotLoadQuerySplitter} that limits the number of rows
 * ingested per batch based on `test.snapshot.load.max.row.count` config but does not have a partition filter.
 */
public class TestSnapshotQuerySplitterWithoutPartitionFilterImpl extends TestSnapshotQuerySplitterImpl {

  /**
   * Constructor initializing the properties.
   *
   * @param properties Configuration properties for the splitter.
   */
  public TestSnapshotQuerySplitterWithoutPartitionFilterImpl(TypedProperties properties) {
    super(properties);
  }

  @Override
  public Option<CheckpointWithPredicates> getNextCheckpointWithPredicates(Dataset<Row> df, QueryContext queryContext) {
    Option<CheckpointWithPredicates> checkpointWithPredicatesOption = super.getNextCheckpointWithPredicates(df, queryContext);
    return checkpointWithPredicatesOption.map(checkpoint -> new CheckpointWithPredicates(checkpoint.getEndCompletionTime(), null));
  }
}
