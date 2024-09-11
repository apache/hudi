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

import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.streamer.PostWriteTerminationStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hudi.utilities.ingestion.HoodieIngestionService.HoodieIngestionConfig.INGESTION_IS_CONTINUOUS;
import static org.apache.hudi.utilities.ingestion.HoodieIngestionService.HoodieIngestionConfig.INGESTION_MIN_SYNC_INTERNAL_SECONDS;

/**
 * A generic service to facilitate running data ingestion.
 */
public abstract class HoodieIngestionService extends HoodieAsyncService {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIngestionService.class);

  protected HoodieIngestionConfig ingestionConfig;

  public HoodieIngestionService(HoodieIngestionConfig ingestionConfig) {
    this.ingestionConfig = ingestionConfig;
  }

  /**
   * Entrypoint to start ingestion.
   * <p>
   * Depends on the ingestion mode, this method will
   * <li>either start a loop as implemented in {@link #startService()} for continuous mode
   * <li>or do one-time ingestion as implemented in {@link #ingestOnce()} for non-continuous mode
   */
  public void startIngestion() {
    if (ingestionConfig.getBoolean(INGESTION_IS_CONTINUOUS)) {
      LOG.info("Ingestion service starts running in continuous mode");
      start(this::onIngestionCompletes);
      try {
        waitForShutdown();
      } catch (Exception e) {
        throw new HoodieIngestionException("Ingestion service was shut down with exception.", e);
      }
      LOG.info("Ingestion service (continuous mode) has been shut down.");
    } else {
      LOG.info("Ingestion service starts running in run-once mode");
      ingestOnce();
      LOG.info("Ingestion service (run-once mode) has been shut down.");
    }
  }

  /**
   * The main loop for running ingestion in continuous mode.
   */
  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    return Pair.of(CompletableFuture.supplyAsync(() -> {
      try {
        while (!isShutdownRequested()) {
          long ingestionStartEpochMillis = System.currentTimeMillis();
          ingestOnce();
          boolean requested = requestShutdownIfNeeded(Option.empty());
          if (!requested) {
            sleepBeforeNextIngestion(ingestionStartEpochMillis);
          }
        }
      } finally {
        executor.shutdownNow();
      }
      return true;
    }, executor), executor);
  }

  /**
   * For the main ingestion logic.
   * <p>
   * In continuous mode, this will be executed in a loop with sleeps in between.
   */
  public abstract void ingestOnce();

  /**
   * To determine if shutdown should be requested to allow gracefully terminate the ingestion in continuous mode.
   * <p>
   * Subclasses should implement the logic to make the decision. If the shutdown condition is met, the implementation
   * should call {@link #shutdown(boolean)} to indicate the request.
   *
   * @see PostWriteTerminationStrategy
   */
  protected boolean requestShutdownIfNeeded(Option<HoodieData<WriteStatus>> lastWriteStatus) {
    return false;
  }

  protected void sleepBeforeNextIngestion(long ingestionStartEpochMillis) {
    try {
      long minSyncInternalSeconds = ingestionConfig.getLongOrDefault(INGESTION_MIN_SYNC_INTERNAL_SECONDS);
      long sleepMs = minSyncInternalSeconds * 1000 - (System.currentTimeMillis() - ingestionStartEpochMillis);
      if (sleepMs > 0) {
        LOG.info(String.format("Last ingestion took less than min sync interval: %d s; sleep for %.2f s",
            minSyncInternalSeconds, sleepMs / 1000.0));
        Thread.sleep(sleepMs);
      }
    } catch (InterruptedException e) {
      throw new HoodieIngestionException("Ingestion service (continuous mode) was interrupted during sleep.", e);
    }
  }

  /**
   * A callback method to be invoked after ingestion completes.
   * <p>
   * For continuous mode, this is invoked once after exiting the ingestion loop.
   */
  protected boolean onIngestionCompletes(boolean hasError) {
    return true;
  }

  public abstract Option<HoodieIngestionMetrics> getMetrics();

  public void close() {
    if (!isShutdown()) {
      shutdown(true);
    }
  }

  public static class HoodieIngestionConfig extends HoodieConfig {

    public static final ConfigProperty<Boolean> INGESTION_IS_CONTINUOUS = ConfigProperty
        .key("hoodie.utilities.ingestion.is.continuous")
        .defaultValue(false)
        .markAdvanced()
        .withDocumentation("Indicate if the ingestion runs in a continuous loop.");

    public static final ConfigProperty<Integer> INGESTION_MIN_SYNC_INTERNAL_SECONDS = ConfigProperty
        .key("hoodie.utilities.ingestion.min.sync.internal.seconds")
        .defaultValue(0)
        .markAdvanced()
        .withDocumentation("the minimum sync interval of each ingestion in continuous mode");

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {

      private final HoodieIngestionConfig ingestionConfig = new HoodieIngestionConfig();

      public Builder isContinuous(boolean isContinuous) {
        this.ingestionConfig.setValue(INGESTION_IS_CONTINUOUS, String.valueOf(isContinuous));
        return this;
      }

      public Builder withMinSyncInternalSeconds(int minSyncInternalSeconds) {
        this.ingestionConfig.setValue(INGESTION_MIN_SYNC_INTERNAL_SECONDS, String.valueOf(minSyncInternalSeconds));
        return this;
      }

      public HoodieIngestionConfig build() {
        ingestionConfig.setDefaults(HoodieIngestionConfig.class.getName());
        return ingestionConfig;
      }
    }
  }
}
