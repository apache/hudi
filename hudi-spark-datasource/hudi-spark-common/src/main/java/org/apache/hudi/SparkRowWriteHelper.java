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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class to assist in deduplicating Rows for BulkInsert with Rows.
 */
public class SparkRowWriteHelper {

  private SparkRowWriteHelper() {
  }

  private static class WriteHelperHolder {
    private static final SparkRowWriteHelper SPARK_WRITE_HELPER = new SparkRowWriteHelper();
  }

  public static SparkRowWriteHelper newInstance() {
    return SparkRowWriteHelper.WriteHelperHolder.SPARK_WRITE_HELPER;
  }

  public Dataset<Row> deduplicateRows(Dataset<Row> inputDf, String preCombineField, boolean isGlobalIndex) {
    return inputDf.groupByKey((MapFunction<Row, String>) value ->
            isGlobalIndex
                ? (value.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD))
                : (value.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD) + "+" + value.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD)), Encoders.STRING())
        .reduceGroups((ReduceFunction<Row>) (v1, v2) ->
            ((Comparable) v1.getAs(preCombineField)).compareTo(v2.getAs(preCombineField)) >= 0 ? v1 : v2)
        .map((MapFunction<Tuple2<String, Row>, Row>) value -> value._2, getEncoder(inputDf.schema()));
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }
}
