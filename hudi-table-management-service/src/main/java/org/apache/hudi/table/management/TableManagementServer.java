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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.common.TableManagementServiceConfig;
import org.apache.hudi.table.management.service.BaseService;
import org.apache.hudi.table.management.service.CleanService;
import org.apache.hudi.table.management.service.ExecutorService;
import org.apache.hudi.table.management.service.MonitorService;
import org.apache.hudi.table.management.service.RetryService;
import org.apache.hudi.table.management.service.ScheduleService;
import org.apache.hudi.table.management.store.MetadataStore;

import com.beust.jcommander.JCommander;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A standalone table management service.
 */
public class TableManagementServer {

  private static final Logger LOG = LoggerFactory.getLogger(TableManagementServer.class);

  private int serverPort;
  private final Configuration conf;
  private final TableManagementServiceConfig config;
  private final transient FileSystem fs;
  private transient Javalin app = null;
  private List<BaseService> services;
  private MetadataStore metadataStore;

  public TableManagementServer(int serverPort, Configuration conf, TableManagementServiceConfig config)
      throws IOException {
    this.config = config;
    this.conf = FSUtils.prepareHadoopConf(conf);
    this.fs = FileSystem.get(conf);
    this.serverPort = serverPort;
    this.metadataStore = initMetadataStore();
  }

  public TableManagementServer(TableManagementServiceConfig config) throws IOException {
    this(config.serverPort, new Configuration(), config);
  }

  public int startService() throws IOException {
    app = Javalin.create();
    RequestHandler requestHandler = new RequestHandler(app, conf, metadataStore);
    app.get("/", ctx -> ctx.result("Hello World"));
    requestHandler.register();
    app.start(serverPort);
    registerService();
    initAndStartRegisterService();
    return serverPort;
  }

  private MetadataStore initMetadataStore() {
    String className = ServiceConfig.getInstance()
        .getString(ServiceConfig.ServiceConfVars.MetadataStoreClass);
    MetadataStore metadataStore = ReflectionUtils.loadClass(className);
    metadataStore.init();
    LOG.info("Finish init metastore: " + className);
    return metadataStore;
  }

  private void registerService() {
    services = new ArrayList<>();
    ExecutorService executorService = new ExecutorService();
    services.add(executorService);
    services.add(new ScheduleService(executorService, metadataStore));
    services.add(new RetryService(metadataStore));
    services.add(new MonitorService());
    services.add(new CleanService());
  }

  private void initAndStartRegisterService() {
    for (BaseService service : services) {
      service.init();
      service.startService();
    }
  }

  private void stopRegisterService() {
    for (BaseService service : services) {
      service.stop();
    }
  }

  public void run() throws IOException {
    startService();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println(
                      "*** shutting down Table management service since JVM is shutting down");
                  try {
                    TableManagementServer.this.stop();
                  } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                  }
                  System.err.println("*** Table management service shut down");
                }));
  }

  /**
   * Stop serving requests and shutdown resources.
   */
  public void stop() throws InterruptedException {
    LOG.info("Stopping Table management Service");
    this.app.stop();
    this.app = null;
    stopRegisterService();
    LOG.info("Stopped Table management Service");
  }

  public static void main(String[] args) throws Exception {
    final TableManagementServiceConfig cfg = new TableManagementServiceConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
      System.exit(1);
    }
    TableManagementServer service = new TableManagementServer(cfg);
    service.run();
  }
}
