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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.schema.RowBasedSchemaProvider;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.util.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class RowSource extends Source<Dataset<Row>> {

  public RowSource(TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider, SourceType.ROW);
  }

  protected abstract Pair<Optional<Dataset<Row>>, String> fetchNextBatch(Optional<String> lastCkptStr,
      long sourceLimit);

  @Override
  protected final InputBatch<Dataset<Row>> fetchNewData(Optional<String> lastCkptStr, long sourceLimit) {
    Pair<Optional<Dataset<Row>>, String> res = fetchNextBatch(lastCkptStr, sourceLimit);
    return res.getKey().map(dsr -> {
      SchemaProvider rowSchemaProvider = new RowBasedSchemaProvider(dsr.schema());
      return new InputBatch<>(res.getKey(), res.getValue(), rowSchemaProvider);
    }).orElseGet(() -> new InputBatch<>(res.getKey(), res.getValue()));
  }
}
