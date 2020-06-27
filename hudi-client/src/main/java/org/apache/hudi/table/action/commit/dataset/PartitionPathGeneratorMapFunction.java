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

package org.apache.hudi.table.action.commit.dataset;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Partition Path Generator For Row Writer.
 */
public class PartitionPathGeneratorMapFunction implements MapFunction<Row, String> {

  private static final String DEFAULT_PARTITION_PATH = "default";
  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  private final List<String> rowKeyFields;
  private final List<Integer> rowKeyFieldPos;
  private final boolean hiveStylePartitioning;

  public PartitionPathGeneratorMapFunction(StructType structType, List<String> rowKeyFields,
      boolean hiveStylePartitioning) {
    this.hiveStylePartitioning = hiveStylePartitioning;
    this.rowKeyFields = rowKeyFields;
    this.rowKeyFieldPos = rowKeyFields.stream()
        .map(f -> (Integer)(structType.getFieldIndex(f).get()))
        .collect(Collectors.toList());
  }

  @Override
  public String call(Row row) throws Exception {
    return IntStream.range(0, rowKeyFields.size()).mapToObj(idx -> {
      String field = rowKeyFields.get(idx);
      Integer fieldPos = rowKeyFieldPos.get(idx);
      if (row.isNullAt(fieldPos)) {
        return hiveStylePartitioning ? field + "=" + DEFAULT_PARTITION_PATH : DEFAULT_PARTITION_PATH;
      }

      String fieldVal = row.getAs(field).toString();
      if (fieldVal.isEmpty()) {
        return hiveStylePartitioning ? field + "=" + DEFAULT_PARTITION_PATH : DEFAULT_PARTITION_PATH;
      }

      return hiveStylePartitioning ? field + "=" + fieldVal : fieldVal;
    }).collect(Collectors.joining(DEFAULT_PARTITION_PATH_SEPARATOR));
  }
}
