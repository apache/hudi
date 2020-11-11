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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.hudi.utilities.sources.selector.AbstractDFSPathSelector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DFS Source that reads parquet data.
 */
public class ParquetDFSSource extends RowSource {

  private static final Logger LOG = LogManager.getLogger(HoodieDeltaStreamer.class);

  private final AbstractDFSPathSelector pathSelector;

  public ParquetDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = UtilHelpers.createSourceSelector(props, this.sparkContext.hadoopConfiguration());
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<String>, String> selectPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(lastCkptStr, sourceLimit);
    return selectPathsWithMaxModificationTime.getLeft()
        .map(pathStr -> Pair.of(Option.of(fromFiles(pathStr)), selectPathsWithMaxModificationTime.getRight()))
        .orElseGet(() -> Pair.of(Option.empty(), selectPathsWithMaxModificationTime.getRight()));
  }

  private Dataset<Row> fromFiles(String pathStr) {
    LOG.error("-------------pathStr" + pathStr);
    Dataset<Row> parquet = sparkSession.read().parquet(pathStr.split(","));
    parquet.show(2000);
    //return sparkSession.read().parquet(pathStr.split(","));
    return parquet;
  }
}
