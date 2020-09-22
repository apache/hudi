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

import org.apache.hudi.client.AbstractCompactor;
import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.client.HoodieSparkCompactor;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Async Compaction Service used by Structured Streaming. Here, async compaction is run in daemon mode to prevent
 * blocking shutting down the Spark application.
 */
public class SparkStreamingAsyncCompactService extends AsyncCompactService {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(SparkStreamingAsyncCompactService.class);
  private transient JavaSparkContext jssc;

  public SparkStreamingAsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client) {
    super(context, client);
    this.jssc = HoodieSparkEngineContext.getSparkContext(context);
  }

  public SparkStreamingAsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client, boolean runInDaemonMode) {
    super(context, client, runInDaemonMode);
    this.jssc = HoodieSparkEngineContext.getSparkContext(context);
  }

  @Override
  protected AbstractCompactor createCompactor(AbstractHoodieWriteClient client) {
    return new HoodieSparkCompactor(client);
  }
}
