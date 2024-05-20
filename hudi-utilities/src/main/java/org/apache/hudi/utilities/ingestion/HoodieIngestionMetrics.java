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

package org.apache.hudi.utilities.ingestion;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.storage.HoodieStorage;

import com.codahale.metrics.Timer;

import java.io.Serializable;

/**
 * For reporting metrics related to ingesting data via {@link HoodieIngestionService}.
 */
public abstract class HoodieIngestionMetrics implements Serializable {

  protected final HoodieStorage storage;

  protected final HoodieMetricsConfig writeConfig;

  public HoodieIngestionMetrics(HoodieWriteConfig writeConfig, HoodieStorage storage) {
    this(writeConfig.getMetricsConfig(), storage);
  }

  public HoodieIngestionMetrics(HoodieMetricsConfig writeConfig, HoodieStorage storage) {
    this.writeConfig = writeConfig;
    this.storage = storage;
  }

  public abstract Timer.Context getOverallTimerContext();

  public abstract Timer.Context getHiveSyncTimerContext();

  public abstract Timer.Context getMetaSyncTimerContext();

  public abstract void updateStreamerMetrics(long durationNanos);

  public abstract void updateStreamerMetaSyncMetrics(String syncClassShortName, long syncTimeNanos);

  public abstract void updateStreamerSyncMetrics(long syncEpochTimeMs);

  public abstract void updateStreamerHeartbeatTimestamp(long heartbeatTimestampMs);

  public abstract void updateStreamerSourceDelayCount(String sourceMetricName, long sourceDelayCount);

  public abstract void updateStreamerSourceNewMessageCount(String sourceMetricName, long sourceNewMessageCount);

  public abstract void updateStreamerSourceParallelism(int sourceParallelism);

  public abstract void updateStreamerSourceBytesToBeIngestedInSyncRound(long sourceBytesToBeIngested);

  public abstract void shutdown();
}
