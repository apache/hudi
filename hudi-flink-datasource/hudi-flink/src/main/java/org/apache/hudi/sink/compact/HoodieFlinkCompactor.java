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

package org.apache.hudi.sink.compact;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.common.util.RetryHelper;

import com.beust.jcommander.JCommander;
import org.apache.flink.client.deployment.application.ApplicationExecutionException;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hudi.sink.compact.AsyncCompactionService.NO_EXECUTE_KEYWORD;

/**
 * Flink hudi compaction program that can be executed manually.
 */
public class HoodieFlinkCompactor {

  protected static final Logger LOG = LoggerFactory.getLogger(HoodieFlinkCompactor.class);

  /**
   * Flink Execution Environment.
   */
  private final AsyncCompactionService compactionScheduleService;

  public HoodieFlinkCompactor(AsyncCompactionService service) {
    this.compactionScheduleService = service;
  }

  public static void main(String[] args) throws Exception {
    FlinkCompactionConfig cfg = getFlinkCompactionConfig(args);
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    // Validate configuration
    if (cfg.retryLastFailedJob && cfg.maxProcessingTimeMs <= 0) {
      LOG.warn("--retry-last-failed-job is enabled but --job-max-processing-time-ms is not set or <= 0. "
          + "The retry-last-failed feature will have no effect.");
    }

    if (cfg.serviceMode) {
      // Service mode: existing behavior without retry wrapper
      // Service mode loop provides implicit retry semantics
      AsyncCompactionService service = cfg.metadataTable ? new AsyncMetadataCompactionService(cfg, conf) : new AsyncCompactionService(cfg, conf);
      new HoodieFlinkCompactor(service).start(true);
    } else {
      // Single-run mode: wrap with retry logic
      new RetryHelper<Void, RuntimeException>(0, cfg.retry, 0, "java.lang.RuntimeException", "Flink compaction")
          .start(() -> {
            AsyncCompactionService service;
            try {
              service = cfg.metadataTable ? new AsyncMetadataCompactionService(cfg, conf) : new AsyncCompactionService(cfg, conf);
            } catch (Exception e) {
              throw new RuntimeException("Failed to create AsyncCompactionService", e);
            }
            try {
              new HoodieFlinkCompactor(service).start(false);
            } catch (ApplicationExecutionException aee) {
              if (aee.getMessage() != null && aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
                LOG.info("Compaction is not performed - no work to do");
                // Not a failure, no need to retry
              } else {
                throw new RuntimeException(aee);
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            } finally {
              service.shutDown();
            }
            return null;
          });
    }
  }

  /**
   * Main method to start compaction service.
   */
  public void start(boolean serviceMode) throws Exception {
    if (serviceMode) {
      compactionScheduleService.start(null);
      try {
        compactionScheduleService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      } finally {
        LOG.info("Shut down hoodie flink compactor");
      }
    } else {
      LOG.info("Hoodie Flink Compactor running only single round");
      try {
        compactionScheduleService.compact();
      } catch (ApplicationExecutionException aee) {
        if (aee.getMessage() != null && aee.getMessage().contains(NO_EXECUTE_KEYWORD)) {
          LOG.info("Compaction is not performed");
        } else {
          throw aee;
        }
      } catch (Exception e) {
        LOG.error("Got error running delta sync once. Shutting down", e);
        throw e;
      } finally {
        LOG.info("Shut down hoodie flink compactor");
      }
    }
  }

  public static FlinkCompactionConfig getFlinkCompactionConfig(String[] args) {
    FlinkCompactionConfig cfg = new FlinkCompactionConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    return cfg;
  }
}
