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
import org.apache.hudi.common.util.RateLimiter;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.concurrent.TimeUnit;

/**
 * {@link RowDataEnrichFunction} with rate limiting according to {@link FlinkOptions#WRITE_RATE_LIMIT}.
 */
final class RowDataEnrichFunctionWithRateLimit<I extends RowData, O extends HoodieFlinkInternalRow> extends RowDataEnrichFunction<I, O> {
  /**
   * Total rate limit per second for the whole job set by config
   */
  private final double totalLimit;

  /**
   * Rate limit per second for the subtask, which depends on parallelism
   */
  private transient RateLimiter rateLimiter;

  RowDataEnrichFunctionWithRateLimit(Configuration config, RowType rowType) {
    super(config, rowType);
    this.totalLimit = config.getLong(FlinkOptions.WRITE_RATE_LIMIT);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.rateLimiter =
        RateLimiter.create((int) totalLimit / getRuntimeContext().getNumberOfParallelSubtasks(), TimeUnit.SECONDS);
  }

  @Override
  public O map(I record) throws Exception {
    rateLimiter.acquire(1);
    return super.map(record);
  }
}
