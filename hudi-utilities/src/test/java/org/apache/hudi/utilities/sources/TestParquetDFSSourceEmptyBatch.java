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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TestParquetDFSSourceEmptyBatch extends ParquetDFSSource {

  public static String RETURN_EMPTY_BATCH = "test.dfs.source.return.empty.batches.for";
  public static String DEFAULT_RETURN_EMPTY_BATCH = "";
  public List<Integer> emptyBatches;
  private int counter = 0;

  public TestParquetDFSSourceEmptyBatch(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                                        SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    String[] emptyBatchesStr = props.getString(RETURN_EMPTY_BATCH, DEFAULT_RETURN_EMPTY_BATCH).split(",");
    this.emptyBatches = Arrays.stream(emptyBatchesStr).filter(entry -> !StringUtils.isNullOrEmpty(entry)).map(entry -> Integer.parseInt(entry)).collect(Collectors.toList());
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<Dataset<Row>>, String> toReturn = super.fetchNextBatch(lastCkptStr, sourceLimit);
    if (emptyBatches.contains(counter++)) {
      return Pair.of(Option.empty(), toReturn.getRight());
    }
    return toReturn;
  }
}
