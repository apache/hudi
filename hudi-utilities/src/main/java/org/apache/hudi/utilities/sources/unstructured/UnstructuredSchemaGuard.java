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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The last entry of an unstructured job's transformer chain: checks the round's rows against the table's contract
 * ({@link UnstructuredSchemaContract#checkOutput}) on the driver, with no Spark job, and returns them. A refusal fails the round,
 * whose instant the streamer rolls back. It backs the start-time plan of the chain for a transformer whose output depends on the
 * data. The source adds it; a job does not name it.
 *
 * <p>A column the rows name twice resolves to the last one, at the first one's place.
 */
public class UnstructuredSchemaGuard implements Transformer {

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties props) {
    UnstructuredMode mode = UnstructuredMode.from(props);
    StructType fixed = UnstructuredSchemaContract.contract(mode, props, Option.empty());
    StructType schema = UnstructuredSchemaContract.lastWins(mode, fixed, rows.schema(), "the rows of the round");
    Dataset<Row> resolved = schema.fields().length == rows.schema().fields().length ? rows : lastWins(rows);
    UnstructuredSchemaContract.checkOutput(mode, UnstructuredSchemaContract.contract(mode, props, Option.empty()),
        fixed, resolved.schema(), "the rows of the round", ConfigUtils.getBooleanWithAltKeys(props, HoodieErrorTableConfig.ERROR_TABLE_ENABLED));
    return resolved;
  }

  /** {@code rows} with each column name once: the last column of a name, at the first one's place. */
  private static Dataset<Row> lastWins(Dataset<Row> rows) {
    StructField[] fields = rows.schema().fields();
    String[] positional = new String[fields.length];
    Map<String, Integer> last = new LinkedHashMap<>();
    for (int i = 0; i < fields.length; i++) {
      positional[i] = "_c" + i;
      last.put(fields[i].name(), i);
    }
    Dataset<Row> renamed = rows.toDF(positional);
    List<Column> columns = new ArrayList<>();
    for (Map.Entry<String, Integer> column : last.entrySet()) {
      StructField field = fields[column.getValue()];
      columns.add(renamed.col(positional[column.getValue()]).as(field.name(), field.metadata()));
    }
    return renamed.select(columns.toArray(new Column[0]));
  }

  /** The chain's plan checks the schemas itself; the guard changes none. */
  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties props) {
    return incomingStruct;
  }
}
