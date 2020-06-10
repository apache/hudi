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

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.embedded.SparkEmbeddedTimelineService;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SparkCompactionAdminClient extends BaseCompactionAdminClient {
  private static final Logger LOG = LogManager.getLogger(SparkCompactionAdminClient.class);

  public SparkCompactionAdminClient(HoodieEngineContext context, String basePath) {
    super(context, basePath);
  }

  @Override
  public List<ValidationOpResult> validateCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant, int parallelism) throws IOException {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    HoodieCompactionPlan plan = getCompactionPlan(metaClient, compactionInstant);
    HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());

    if (plan.getOperations() != null) {
      List<CompactionOperation> ops = plan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      return jsc.parallelize(ops, parallelism).map(op -> {
        try {
          return validateCompactionOperation(metaClient, compactionInstant, op, Option.of(fsView));
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      }).collect();
    }
    return new ArrayList<>();
  }

  @Override
  public List<RenameOpResult> runRenamingOps(HoodieTableMetaClient metaClient, List<Pair<HoodieLogFile, HoodieLogFile>> renameActions, int parallelism, boolean dryRun) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    if (renameActions.isEmpty()) {
      LOG.info("No renaming of log-files needed. Proceeding to removing file-id from compaction-plan");
      return new ArrayList<>();
    } else {
      LOG.info("The following compaction renaming operations needs to be performed to un-schedule");
      if (!dryRun) {
        return jsc.parallelize(renameActions, parallelism).map(lfPair -> {
          try {
            LOG.info("RENAME " + lfPair.getLeft().getPath() + " => " + lfPair.getRight().getPath());
            renameLogFile(metaClient, lfPair.getLeft(), lfPair.getRight());
            return new RenameOpResult(lfPair, true, Option.empty());
          } catch (IOException e) {
            LOG.error("Error renaming log file", e);
            LOG.error("\n\n\n***NOTE Compaction is in inconsistent state. Try running \"compaction repair "
                + lfPair.getLeft().getBaseCommitTime() + "\" to recover from failure ***\n\n\n");
            return new RenameOpResult(lfPair, false, Option.of(e));
          }
        }).collect();
      } else {
        LOG.info("Dry-Run Mode activated for rename operations");
        return renameActions.parallelStream().map(lfPair -> new RenameOpResult(lfPair, false, false, Option.empty()))
            .collect(Collectors.toList());
      }
    }
  }

  @Override
  public List<Pair<HoodieLogFile, HoodieLogFile>> getRenamingActionsForUnschedulingCompactionPlan(HoodieTableMetaClient metaClient, String compactionInstant, int parallelism, Option<HoodieTableFileSystemView> fsViewOpt, boolean skipValidation) throws IOException {
    HoodieTableFileSystemView fsView = fsViewOpt.isPresent() ? fsViewOpt.get()
        : new HoodieTableFileSystemView(metaClient, metaClient.getCommitsAndCompactionTimeline());
    HoodieCompactionPlan plan = getCompactionPlan(metaClient, compactionInstant);
    if (plan.getOperations() != null) {
      LOG.info(
          "Number of Compaction Operations :" + plan.getOperations().size() + " for instant :" + compactionInstant);
      List<CompactionOperation> ops = plan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      return ((HoodieSparkEngineContext) context).getJavaSparkContext().parallelize(ops, parallelism).flatMap(op -> {
        try {
          return getRenamingActionsForUnschedulingCompactionOperation(metaClient, compactionInstant, op,
              Option.of(fsView), skipValidation).iterator();
        } catch (IOException ioe) {
          throw new HoodieIOException(ioe.getMessage(), ioe);
        } catch (CompactionValidationException ve) {
          throw new HoodieException(ve);
        }
      }).collect();
    }
    LOG.warn("No operations for compaction instant : " + compactionInstant);
    return new ArrayList<>();
  }

  @Override
  public void startEmbeddedServerView() {
    timeServerLock.lock();
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (!timelineServer.isPresent()) {
        // Run Embedded Timeline Server
        LOG.info("Starting Timeline service !!");
        timelineServer = Option.of(new SparkEmbeddedTimelineService(context,
            config.getClientSpecifiedViewStorageConfig()));
        try {
          timelineServer.get().startServer();
          // Allow executor to find this newly instantiated timeline service
          config.setViewStorageConfig(timelineServer.get().getRemoteFileSystemViewConfig());
        } catch (IOException e) {
          LOG.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        LOG.info("Timeline Server already running. Not restarting the service");
      }
    } else {
      LOG.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
    timeServerLock.unlock();
  }

}
