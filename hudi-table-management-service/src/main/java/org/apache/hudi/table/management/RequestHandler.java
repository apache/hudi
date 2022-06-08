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

package org.apache.hudi.table.management;

import org.apache.hudi.table.management.entity.Action;
import org.apache.hudi.table.management.entity.Engine;
import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.entity.InstanceStatus;
import org.apache.hudi.table.management.handlers.ActionHandler;
import org.apache.hudi.table.management.store.MetadataStore;
import org.apache.hudi.table.management.util.InstanceUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Main REST Handler class that handles and delegates calls to timeline relevant handlers.
 */
public class RequestHandler {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

  private final Javalin app;
  private final ActionHandler actionHandler;

  public RequestHandler(Javalin app,
                        Configuration conf,
                        MetadataStore metadataStore) throws IOException {
    this.app = app;
    this.actionHandler = new ActionHandler(conf, metadataStore);
  }

  public void register() {
    registerCommonAPI();
    registerCompactionAPI();
    registerClusteringAPI();
  }

  private void writeValueAsString(Context ctx, Object obj) throws JsonProcessingException {
    boolean prettyPrint = ctx.queryParam("pretty") != null;
    long beginJsonTs = System.currentTimeMillis();
    String result =
        prettyPrint ? OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj) : OBJECT_MAPPER.writeValueAsString(obj);
    long endJsonTs = System.currentTimeMillis();
    LOG.debug("Jsonify TimeTaken=" + (endJsonTs - beginJsonTs));
    ctx.result(result);
  }

  /**
   * Register Compaction API calls.
   */
  private void registerCommonAPI() {
    app.get(HoodieTableManagerClient.REGISTER, new ViewHandler(ctx -> {
    }));
  }

  /**
   * Register Compaction API calls.
   */
  private void registerCompactionAPI() {
    app.get(HoodieTableManagerClient.SUBMIT_COMPACTION, new ViewHandler(ctx -> {
      for (String instant : ctx.validatedQueryParam(HoodieTableManagerClient.INSTANT_PARAM).getOrThrow().split(",")) {
        Instance instance = Instance.builder()
            .basePath(ctx.validatedQueryParam(HoodieTableManagerClient.BASEPATH_PARAM).getOrThrow())
            .dbName(ctx.validatedQueryParam(HoodieTableManagerClient.DATABASE_NAME_PARAM).getOrThrow())
            .tableName(ctx.validatedQueryParam(HoodieTableManagerClient.TABLE_NAME_PARAM).getOrThrow())
            .action(Action.COMPACTION.getValue())
            .instant(instant)
            .executionEngine(Engine.valueOf(ctx.validatedQueryParam(HoodieTableManagerClient.EXECUTION_ENGINE).getOrThrow()))
            .owner(ctx.validatedQueryParam(HoodieTableManagerClient.USERNAME).getOrThrow())
            .queue(ctx.validatedQueryParam(HoodieTableManagerClient.QUEUE).getOrThrow())
            .resource(ctx.validatedQueryParam(HoodieTableManagerClient.RESOURCE).getOrThrow())
            .parallelism(ctx.validatedQueryParam(HoodieTableManagerClient.PARALLELISM).getOrThrow())
            .status(InstanceStatus.SCHEDULED.getStatus())
            .build();
        InstanceUtil.checkArgument(instance);
        actionHandler.scheduleCompaction(instance);
      }
    }));

    app.get(HoodieTableManagerClient.REMOVE_COMPACTION, new ViewHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(HoodieTableManagerClient.BASEPATH_PARAM).getOrThrow())
          .dbName(ctx.validatedQueryParam(HoodieTableManagerClient.DATABASE_NAME_PARAM).getOrThrow())
          .tableName(ctx.validatedQueryParam(HoodieTableManagerClient.TABLE_NAME_PARAM).getOrThrow())
          .instant(ctx.validatedQueryParam(HoodieTableManagerClient.INSTANT_PARAM).getOrThrow())
          .status(InstanceStatus.INVALID.getStatus())
          .isDeleted(true)
          .build();
      actionHandler.removeCompaction(instance);
    }));
  }

  /**
   * Register Compaction API calls.
   */
  private void registerClusteringAPI() {
    app.get(HoodieTableManagerClient.SUBMIT_CLUSTERING, new ViewHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(HoodieTableManagerClient.BASEPATH_PARAM).getOrThrow())
          .dbName(ctx.validatedQueryParam(HoodieTableManagerClient.DATABASE_NAME_PARAM).getOrThrow())
          .tableName(ctx.validatedQueryParam(HoodieTableManagerClient.TABLE_NAME_PARAM).getOrThrow())
          .action(Action.CLUSTERING.getValue())
          .instant(ctx.validatedQueryParam(HoodieTableManagerClient.INSTANT_PARAM).getOrThrow())
          .executionEngine(Engine.valueOf(ctx.validatedQueryParam(HoodieTableManagerClient.EXECUTION_ENGINE).getOrThrow()))
          .owner(ctx.validatedQueryParam(HoodieTableManagerClient.USERNAME).getOrThrow())
          .queue(ctx.validatedQueryParam(HoodieTableManagerClient.QUEUE).getOrThrow())
          .resource(ctx.validatedQueryParam(HoodieTableManagerClient.RESOURCE).getOrThrow())
          .parallelism(ctx.validatedQueryParam(HoodieTableManagerClient.PARALLELISM).getOrThrow())
          .status(InstanceStatus.SCHEDULED.getStatus())
          .build();
      InstanceUtil.checkArgument(instance);
      actionHandler.scheduleClustering(instance);
    }));

    app.get(HoodieTableManagerClient.REMOVE_CLUSTERING, new ViewHandler(ctx -> {
      Instance instance = Instance.builder()
          .basePath(ctx.validatedQueryParam(HoodieTableManagerClient.BASEPATH_PARAM).getOrThrow())
          .dbName(ctx.validatedQueryParam(HoodieTableManagerClient.DATABASE_NAME_PARAM).getOrThrow())
          .tableName(ctx.validatedQueryParam(HoodieTableManagerClient.TABLE_NAME_PARAM).getOrThrow())
          .instant(ctx.validatedQueryParam(HoodieTableManagerClient.INSTANT_PARAM).getOrThrow())
          .status(InstanceStatus.INVALID.getStatus())
          .isDeleted(true)
          .build();
      actionHandler.removeClustering(instance);
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
      boolean synced = false;
      long refreshCheckTimeTaken = 0;
      long handleTimeTaken = 0;
      long finalCheckTimeTaken = 0;
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
            "TimeTakenMillis[Total=%d, Refresh=%d, handle=%d, Check=%d], "
                + "Success=%s, Query=%s, Host=%s, synced=%s",
            timeTakenMillis, refreshCheckTimeTaken, handleTimeTaken, finalCheckTimeTaken, success,
            context.queryString(), context.host(), synced));
      }
    }
  }
}
