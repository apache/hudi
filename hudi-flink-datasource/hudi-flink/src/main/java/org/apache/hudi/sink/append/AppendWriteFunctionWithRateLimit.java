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

package org.apache.hudi.sink.append;

import org.apache.hudi.common.util.RateLimiter;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Append write function with configurable rate limit.
 */
public class AppendWriteFunctionWithRateLimit<I>
    extends AppendWriteFunction<I> {
  /**
   * Total rate limit per second for this job.
   */
  private final double totalLimit;

  /**
   * Rate limit per second for per task.
   */
  private transient RateLimiter rateLimiter;

  public AppendWriteFunctionWithRateLimit(Configuration config,  RowType recordRowType, RowType writerRowType) {
    super(config, recordRowType, writerRowType);
    this.totalLimit = config.get(FlinkOptions.WRITE_RATE_LIMIT);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.rateLimiter =
        RateLimiter.create((int) totalLimit / RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()), TimeUnit.SECONDS);
  }

  @Override
  public void processElement(I value, Context ctx, Collector<RowData> out) throws Exception {
    rateLimiter.acquire(1);
    super.processElement(value, ctx, out);
  }
}
