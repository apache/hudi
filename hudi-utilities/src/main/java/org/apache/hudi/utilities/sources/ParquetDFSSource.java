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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.ParquetDFSSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;

/**
 * DFS Source that reads parquet data.
 */
public class ParquetDFSSource extends RowSource {

  private final DFSPathSelector pathSelector;

  public ParquetDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = DFSPathSelector.createSourceSelector(props, this.sparkContext.hadoopConfiguration());
  }

  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    Pair<Option<String>, Checkpoint> selectPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(sparkContext, lastCheckpoint, sourceLimit);
    return selectPathsWithMaxModificationTime.getLeft()
        .map(pathStr -> Pair.of(Option.of(fromFiles(pathStr)), selectPathsWithMaxModificationTime.getRight()))
        .orElseGet(() -> Pair.of(Option.empty(), selectPathsWithMaxModificationTime.getRight()));
  }

  private Dataset<Row> fromFiles(String pathStr) {
    boolean mergeSchemaOption = getBooleanWithAltKeys(this.props, ParquetDFSSourceConfig.PARQUET_DFS_MERGE_SCHEMA);
    return sparkSession.read().option("mergeSchema", mergeSchemaOption).parquet(pathStr.split(","));
  }
}
