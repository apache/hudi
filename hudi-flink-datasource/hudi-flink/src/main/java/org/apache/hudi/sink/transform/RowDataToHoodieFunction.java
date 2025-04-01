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

package org.apache.hudi.sink.transform;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.sink.bulk.RowDataKeyGen;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Function that converts Flink {@link RowData} into {@link HoodieFlinkInternalRow}.
 */
public class RowDataToHoodieFunction<I extends RowData, O extends HoodieFlinkInternalRow>
    extends RichMapFunction<I, O> {
  RowDataKeyGen keyGen;

  public RowDataToHoodieFunction(RowType rowType, Configuration config) {
    this.keyGen = RowDataKeyGen.instance(config, rowType);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public O map(I row) throws Exception {
    return (O) new HoodieFlinkInternalRow(
        keyGen.getRecordKey(row),
        keyGen.getPartitionPath(row),
        HoodieOperation.fromValue(row.getRowKind().toByteValue()).getName(),
        row);
  }
}
