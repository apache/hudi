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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

/**
 * A built-in partitioner that does local sorting for each spark partitions after coalesce for bulk insert operation, corresponding to the {@code BulkInsertSortMode.PARTITION_SORT} mode.
 */
public class RDDPartitionSortPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>> {

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputSparkPartitions) {
    ExpressionEncoder encoder = getEncoder(rows.schema());
    return rows.coalesce(outputSparkPartitions).mapPartitions((MapPartitionsFunction<Row, Row>) input -> {
      // Sort locally in partition
      List<Row> recordList = new ArrayList<>();
      for (; input.hasNext(); ) {
        recordList.add(input.next());
      }
      Collections.sort(recordList, Comparator.comparing(o -> (o.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD) + "+" + o.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD))));
      return recordList.iterator();
    }, encoder);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }
}
