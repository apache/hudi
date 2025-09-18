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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Key generator for {@link RowData} that use an auto key generator.
 */
public class AutoRowDataKeyGen extends RowDataKeyGen {
  private final int taskId;
  private final String instantTime;
  private int rowId;

  public AutoRowDataKeyGen(
      int taskId,
      String instantTime,
      String partitionFields,
      RowType rowType,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath,
      boolean useCompkexKeygenNewEncoding) {
    super(Option.empty(), partitionFields, rowType, hiveStylePartitioning, encodePartitionPath, false, Option.empty(),
            useCompkexKeygenNewEncoding);
    this.taskId = taskId;
    this.instantTime = instantTime;
  }

  public static RowDataKeyGen instance(Configuration conf, RowType rowType, int taskId, String instantTime) {
    return new AutoRowDataKeyGen(taskId, instantTime, conf.get(FlinkOptions.PARTITION_PATH_FIELD),
        rowType, conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING), conf.get(FlinkOptions.URL_ENCODE_PARTITIONING),
        OptionsResolver.useComplexKeygenNewEncoding(conf));
  }

  @Override
  public String getRecordKey(RowData rowData) {
    return HoodieRecord.generateSequenceId(instantTime, taskId, rowId++);
  }
}
