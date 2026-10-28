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

package org.apache.hudi.async;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieSparkCompactor;
import org.apache.hudi.common.engine.HoodieEngineContext;

import java.util.function.Predicate;

/**
 * Async Compaction Service used by Structured Streaming. Here, async compaction is run in daemon mode to prevent
 * blocking shutting down the Spark application.
 */
public class SparkStreamingAsyncCompactService extends AsyncCompactService {

  private static final long serialVersionUID = 1L;

  // whether a write client was retired by the sink, i.e. closed by the sink when it replaced its client
  private final transient Predicate<BaseHoodieWriteClient> isRetiredClient;

  public SparkStreamingAsyncCompactService(HoodieEngineContext context, BaseHoodieWriteClient client) {
    this(context, client, c -> false);
  }

  public SparkStreamingAsyncCompactService(HoodieEngineContext context, BaseHoodieWriteClient client,
      Predicate<BaseHoodieWriteClient> isRetiredClient) {
    super(context, client, true);
    this.isRetiredClient = isRetiredClient;
  }

  /**
   * A compaction run that failed while using a write client the sink has since retired is retryable: the sink closes
   * the previous client when it re-creates its client (e.g. on a schema change), which can fail a run still using it.
   * Any other failure is not retryable and stops the service, which fails the next micro-batch with that failure.
   */
  @Override
  protected boolean isRetryableFailure(BaseHoodieWriteClient runClient, Exception failure) {
    return isRetiredClient.test(runClient);
  }

  @Override
  protected BaseCompactor createCompactor(BaseHoodieWriteClient client) {
    return new HoodieSparkCompactor(client, this.context);
  }
}
