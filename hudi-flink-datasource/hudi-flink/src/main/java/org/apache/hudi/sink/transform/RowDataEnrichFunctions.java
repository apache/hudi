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
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Utilities for {@link RowDataEnrichFunction} to handle rate limit if it was set.
 */
public abstract class RowDataEnrichFunctions {
  private RowDataEnrichFunctions() {
  }

  /**
   * Creates {@link RowDataEnrichFunctions} instance based on the given configuration.
   */
  public static RichMapFunction<RowData, HoodieFlinkInternalRow> create(Configuration conf, RowType rowType) {
    if (conf.getLong(FlinkOptions.WRITE_RATE_LIMIT) > 0) {
      return new RowDataEnrichFunctionWithRateLimit<>(conf, rowType);
    } else {
      return new RowDataEnrichFunction<>(conf, rowType);
    }
  }
}
