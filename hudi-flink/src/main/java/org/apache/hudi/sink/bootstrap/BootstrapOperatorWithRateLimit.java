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

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * The operator to load index from existing hoodieTable with RateLimit.
 */
public class BootstrapOperatorWithRateLimit<I, O extends HoodieRecord>
    extends BootstrapOperator<I, O> {

  /**
   * Total rate limit per second for this job.
   */
  private final double totalLimit;

  /**
   * Rate limit per second for per task.
   */
  private transient RateLimiter rateLimiter;

  public BootstrapOperatorWithRateLimit(Configuration conf) {
    super(conf);
    this.totalLimit = conf.getLong(FlinkOptions.INDEX_BOOTSTRAP_RATE_LIMIT);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    this.rateLimiter =
        RateLimiter.create(totalLimit / getRuntimeContext().getNumberOfParallelSubtasks());
    super.initializeState(context);
  }

  @Override
  public void collectRecord(StreamRecord streamRecord) {
    rateLimiter.acquire();
    super.collectRecord(streamRecord);
  }
}
