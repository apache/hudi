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

import com.beust.jcommander.JCommander;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.async.HoodieAsyncTableService;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;
import org.apache.hudi.util.StreamerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    FlinkCompactionConfig cfg = getFlinkCompactionConfig(args);
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    AsyncCompactionService service = new AsyncCompactionService(cfg, conf, env);
    new HoodieFlinkCompactor(service).start(cfg.compactionMode);
  }

  /**
   * Main method to start compaction service.
   */
  public void start(String mode) throws Exception {
    CompactionMode compactionMode = CompactionMode.valueOf(mode);
    if (compactionMode == CompactionMode.SERVICE) {
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
      } catch (Exception e) {
        LOG.error("Got error running delta sync once. Shutting down", e);
        throw e;
      } finally {
        LOG.info("Shut down hoodie flink compactor");
      }
    }
  }

  public void shutDown() throws Exception {
    if (compactionScheduleService != null) {
      compactionScheduleService.shutDown();
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

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * Schedules compaction in service.
   */
  public static class AsyncCompactionService extends HoodieAsyncTableService {
    private static final long serialVersionUID = 1L;

    /**
     * Flink Compaction Config.
     */
    private final FlinkCompactionConfig cfg;

    /**
     * Flink Config.
     */
    private final Configuration conf;

    /**
     * Write Client.
     */
    private final HoodieFlinkWriteClient<?> writeClient;

    /**
     * Flink Execution Environment.
     */
    private final StreamExecutionEnvironment env;

    /**
     * Executor Service.
     */
    private final ExecutorService executor;

    private final String tableName;

    private final Path bashPath;

    public AsyncCompactionService(FlinkCompactionConfig cfg, Configuration conf, StreamExecutionEnvironment env) throws Exception {
      this.cfg = cfg;
      this.conf = conf;
      this.env = env;
      this.executor = Executors.newFixedThreadPool(1);

      // create metaClient
      HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);

      // get the table name
      conf.setString(FlinkOptions.TABLE_NAME, metaClient.getTableConfig().getTableName());

      // set table schema
      CompactionUtil.setAvroSchema(conf, metaClient);

      // infer changelog mode
      CompactionUtil.inferChangelogMode(conf, metaClient);

      this.writeClient = StreamerUtil.createWriteClient(conf);
      this.writeConfig = writeClient.getConfig();
      HoodieFlinkTable<?> table = writeClient.getHoodieTable();
      this.tableName = table.getMetaClient().getTableConfig().getTableName();
      this.bashPath = table.getMetaClient().getBasePathV2();
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        boolean error = false;

        try {
          while (!isShutdownRequested()) {
            try {
              compact();
              Thread.sleep(cfg.minCompactionIntervalSeconds * 1000);
            } catch (Exception e) {
              LOG.error("Shutting down compaction service due to exception", e);
              error = true;
              throw new HoodieException(e.getMessage(), e);
            }
          }
        } finally {
          shutdownAsyncService(error);
        }
        return true;
      }, executor), executor);
    }

    private void compact() throws Exception {
      Boolean isStreamingMode = CompactionMode.valueOf(cfg.compactionMode) == CompactionMode.STREAMING;

      env.addSource(new CompactionPlanSourceFunction(conf, bashPath.toString(), isStreamingMode))
          .name("compaction_source")
          .uid("uid_compaction_source")
          .rebalance()
          .transform("compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new ProcessOperator<>(new CompactFunction(conf)))
          .setParallelism(cfg.compactionTasks)
          .addSink(new CompactionCommitSink(conf))
          .name("clean_commits")
          .uid("uid_clean_commits")
          .setParallelism(1);

      env.execute("flink_hudi_compaction_" + tableName);
    }

    /**
     * Shutdown async services like compaction/clustering as DeltaSync is shutdown.
     */
    public void shutdownAsyncService(boolean error) {
      LOG.info("Gracefully shutting down compactor. Error ?" + error);
      executor.shutdown();
      writeClient.close();
    }

    @VisibleForTesting
    public void shutDown() {
      shutdownAsyncService(false);
    }
  }
}
