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

package org.apache.hudi.table.service.manager;

import org.apache.hudi.client.HoodieTableServiceManagerClient;
import org.apache.hudi.table.service.manager.entity.Action;
import org.apache.hudi.table.service.manager.entity.Engine;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.entity.InstanceStatus;
import org.apache.hudi.table.service.manager.handlers.ActionHandler;
import org.apache.hudi.table.service.manager.store.MetadataStore;
import org.apache.hudi.table.service.manager.util.InstanceUtil;

import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Locale;

/**
 * Main REST Handler class that handles and delegates calls to timeline relevant handlers.
 */
public class RequestHandler {

  private static final Logger LOG = LogManager.getLogger(RequestHandler.class);

  private final Javalin app;
  private final ActionHandler actionHandler;

  public RequestHandler(Javalin app,
                        Configuration conf,
                        MetadataStore metadataStore) {
    this.app = app;
    this.actionHandler = new ActionHandler(conf, metadataStore);
  }

  public void register() {
    registerCompactionAPI();
    registerClusteringAPI();
    registerCleanAPI();
  }

  /**
   * Register Compaction API calls.
   */
  private void registerCompactionAPI() {
    app.get(HoodieTableServiceManagerClient.EXECUTE_COMPACTION, new ViewHandler(ctx -> {
      for (String instant : ctx.validatedQueryParam(HoodieTableServiceManagerClient.INSTANT_PARAM).getOrThrow().split(",")) {
        Instance instance = Instance.builder()
            .basePath(ctx.validatedQueryParam(HoodieTableServiceManagerClient.BASEPATH_PARAM).getOrThrow())
            .dbName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.DATABASE_NAME_PARAM).getOrThrow())
            .tableName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.TABLE_NAME_PARAM).getOrThrow())
            .action(Action.COMPACTION.getValue())
            .instant(instant)
            .executionEngine(Engine.valueOf(ctx.validatedQueryParam(HoodieTableServiceManagerClient.EXECUTION_ENGINE).getOrThrow().toUpperCase(Locale.ROOT)))
            .userName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.USERNAME).getOrThrow())
            .queue(ctx.validatedQueryParam(HoodieTableServiceManagerClient.QUEUE).getOrThrow())
            .resource(ctx.validatedQueryParam(HoodieTableServiceManagerClient.RESOURCE).getOrThrow())
            .parallelism(ctx.validatedQueryParam(HoodieTableServiceManagerClient.PARALLELISM).getOrThrow())
            .status(InstanceStatus.SCHEDULED.getStatus())
            .build();
        InstanceUtil.checkArgument(instance);
        actionHandler.scheduleCompaction(instance);
      }
    }));
  }

  /**
   * Register Clustering API calls.
   */
  private void registerClusteringAPI() {
    app.get(HoodieTableServiceManagerClient.EXECUTE_CLUSTERING, new ViewHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(HoodieTableServiceManagerClient.BASEPATH_PARAM).getOrThrow())
          .dbName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.DATABASE_NAME_PARAM).getOrThrow())
          .tableName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.TABLE_NAME_PARAM).getOrThrow())
          .action(Action.CLUSTERING.getValue())
          .instant(ctx.validatedQueryParam(HoodieTableServiceManagerClient.INSTANT_PARAM).getOrThrow())
          .executionEngine(Engine.valueOf(ctx.validatedQueryParam(HoodieTableServiceManagerClient.EXECUTION_ENGINE).getOrThrow().toUpperCase(Locale.ROOT)))
          .userName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.USERNAME).getOrThrow())
          .queue(ctx.validatedQueryParam(HoodieTableServiceManagerClient.QUEUE).getOrThrow())
          .resource(ctx.validatedQueryParam(HoodieTableServiceManagerClient.RESOURCE).getOrThrow())
          .parallelism(ctx.validatedQueryParam(HoodieTableServiceManagerClient.PARALLELISM).getOrThrow())
          .status(InstanceStatus.SCHEDULED.getStatus())
          .build();
      InstanceUtil.checkArgument(instance);
      actionHandler.scheduleClustering(instance);
    }));
  }

  /**
   * Register Clean API calls.
   */
  private void registerCleanAPI() {
    app.get(HoodieTableServiceManagerClient.EXECUTE_CLEAN, new ViewHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(HoodieTableServiceManagerClient.BASEPATH_PARAM).getOrThrow())
          .dbName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.DATABASE_NAME_PARAM).getOrThrow())
          .tableName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.TABLE_NAME_PARAM).getOrThrow())
          .action(Action.CLEAN.getValue())
          .instant(ctx.validatedQueryParam(HoodieTableServiceManagerClient.INSTANT_PARAM).getOrThrow())
          .executionEngine(Engine.valueOf(ctx.validatedQueryParam(HoodieTableServiceManagerClient.EXECUTION_ENGINE).getOrThrow().toUpperCase(Locale.ROOT)))
          .userName(ctx.validatedQueryParam(HoodieTableServiceManagerClient.USERNAME).getOrThrow())
          .queue(ctx.validatedQueryParam(HoodieTableServiceManagerClient.QUEUE).getOrThrow())
          .resource(ctx.validatedQueryParam(HoodieTableServiceManagerClient.RESOURCE).getOrThrow())
          .parallelism(ctx.validatedQueryParam(HoodieTableServiceManagerClient.PARALLELISM).getOrThrow())
          .status(InstanceStatus.SCHEDULED.getStatus())
          .build();
      InstanceUtil.checkArgument(instance);
      actionHandler.scheduleClustering(instance);
    }));
  }

  /**
   * Used for logging and performing refresh check.
   */
  private class ViewHandler implements Handler {

    private final Handler handler;

    ViewHandler(Handler handler) {
      this.handler = handler;
    }

    @Override
    public void handle(@NotNull Context context) throws Exception {
      boolean success = true;
      long beginTs = System.currentTimeMillis();
      long handleTimeTaken = 0;
      try {
        long handleBeginMs = System.currentTimeMillis();
        handler.handle(context);
        long handleEndMs = System.currentTimeMillis();
        handleTimeTaken = handleEndMs - handleBeginMs;
      } catch (RuntimeException re) {
        success = false;
        LOG.error("Got runtime exception servicing request " + context.queryString(), re);
        throw re;
      } finally {
        long endTs = System.currentTimeMillis();
        long timeTakenMillis = endTs - beginTs;
        LOG.info(String.format(
            "TimeTakenMillis[Total=%d, handle=%d], Success=%s, Query=%s, Host=%s",
            timeTakenMillis, handleTimeTaken, success, context.queryString(), context.host()));
      }
    }
  }
}
