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

import org.apache.hudi.adapter.RateLimiterAdapter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Function that transforms RowData to a HoodieRecord with RateLimit.
 */
public class RowDataToHoodieFunctionWithRateLimit<I extends RowData, O extends HoodieRecord>
    extends RowDataToHoodieFunction<I, O> {
  /**
   * Total rate limit per second for this job.
   */
  private final double totalLimit;

  /**
   * Rate limit per second for per task.
   */
  private transient RateLimiterAdapter rateLimiter;

  public RowDataToHoodieFunctionWithRateLimit(RowType rowType, Configuration config) {
    super(rowType, config);
    this.totalLimit = config.getLong(FlinkOptions.WRITE_RATE_LIMIT);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.rateLimiter =
        RateLimiterAdapter.create(totalLimit / getRuntimeContext().getNumberOfParallelSubtasks());
  }

  @Override
  public O map(I i) throws Exception {
    rateLimiter.acquire();
    return super.map(i);
  }
}
