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
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class RowSource extends Source<Dataset<Row>> {

  public RowSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider, SourceType.ROW);
  }

  protected abstract Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit);

  @Override
  protected final InputBatch<Dataset<Row>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<Dataset<Row>>, String> res = fetchNextBatch(lastCkptStr, sourceLimit);
    if (overriddenSchemaProvider != null) {
      return new InputBatch<>(res.getKey(), res.getValue(), overriddenSchemaProvider);
    }
    return res.getKey().map(dsr -> {
      SchemaProvider rowSchemaProvider =
          UtilHelpers.createRowBasedSchemaProvider(dsr.schema(), props, sparkContext);
      return new InputBatch<>(res.getKey(), res.getValue(), rowSchemaProvider);
    }).orElseGet(() -> new InputBatch<>(res.getKey(), res.getValue()));
  }
}
