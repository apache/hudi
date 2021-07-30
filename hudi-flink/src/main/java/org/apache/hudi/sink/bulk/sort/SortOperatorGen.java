/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk.sort;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Tools to generate the sort operator.
 */
public class SortOperatorGen {
  private final int[] sortIndices;
  private final LogicalType[] sortTypes;
  private final TableConfig tableConfig = new TableConfig();

  public SortOperatorGen(RowType rowType, String[] sortFields) {
    this.sortIndices = Arrays.stream(sortFields).mapToInt(rowType::getFieldIndex).toArray();
    List<RowType.RowField> fields = rowType.getFields();
    this.sortTypes = Arrays.stream(sortIndices).mapToObj(idx -> fields.get(idx).getType()).toArray(LogicalType[]::new);
  }

  public OneInputStreamOperator<RowData, RowData> createSortOperator() {
    SortCodeGenerator codeGen = createSortCodeGenerator();
    return new SortOperator(
        codeGen.generateNormalizedKeyComputer("SortComputer"),
        codeGen.generateRecordComparator("SortComparator"));
  }

  private SortCodeGenerator createSortCodeGenerator() {
    boolean[] padBooleans = new boolean[sortIndices.length];
    IntStream.range(0, sortIndices.length).forEach(i -> padBooleans[i] = true);
    return new SortCodeGenerator(tableConfig, sortIndices, sortTypes, padBooleans, padBooleans);
  }
}
