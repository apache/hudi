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

import com.codahale.metrics.Timer;

import java.io.Serializable;

/**
 * For reporting metrics related to ingesting data via {@link HoodieIngestionService}.
 */
public abstract class HoodieIngestionMetrics implements Serializable {

  protected final HoodieWriteConfig writeConfig;

  public HoodieIngestionMetrics(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }

  public abstract Timer.Context getOverallTimerContext();

  public abstract Timer.Context getHiveSyncTimerContext();

  public abstract Timer.Context getMetaSyncTimerContext();

  public abstract void updateDeltaStreamerMetrics(long durationNanos);

  public abstract void updateDeltaStreamerMetaSyncMetrics(String syncClassShortName, long syncTimeNanos);

  public abstract void updateDeltaStreamerSyncMetrics(long syncEpochTimeMs);

  public abstract void updateDeltaStreamerHeartbeatTimestamp(long heartbeatTimestampMs);

  public abstract void updateDeltaStreamerSourceDelayCount(String sourceMetricName, long sourceDelayCount);

  public abstract void updateDeltaStreamerSourceNewMessageCount(String sourceMetricName, long sourceNewMessageCount);

  public abstract void shutdown();
}
