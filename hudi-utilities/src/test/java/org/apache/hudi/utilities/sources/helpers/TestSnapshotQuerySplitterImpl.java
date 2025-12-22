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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class TestSnapshotQuerySplitterImpl extends SnapshotLoadQuerySplitter {

  private static final String COMMIT_TIME_METADATA_FIELD = HoodieRecord.COMMIT_TIME_METADATA_FIELD;

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
}
