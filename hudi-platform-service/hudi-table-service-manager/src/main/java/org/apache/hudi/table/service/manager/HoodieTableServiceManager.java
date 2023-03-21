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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.table.service.manager.common.CommandConfig;
import org.apache.hudi.table.service.manager.common.HoodieTableServiceManagerConfig;
import org.apache.hudi.table.service.manager.service.BaseService;
import org.apache.hudi.table.service.manager.service.CleanService;
import org.apache.hudi.table.service.manager.service.ExecutorService;
import org.apache.hudi.table.service.manager.service.MonitorService;
import org.apache.hudi.table.service.manager.service.RestoreService;
import org.apache.hudi.table.service.manager.service.RetryService;
import org.apache.hudi.table.service.manager.service.ScheduleService;
import org.apache.hudi.table.service.manager.store.MetadataStore;

import com.beust.jcommander.JCommander;
import io.javalin.Javalin;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class of hoodie table service manager.
 *
 * @Experimental
 * @since 0.13.0
 */
public class HoodieTableServiceManager {

  private static final Logger LOG = LogManager.getLogger(HoodieTableServiceManager.class);

  private final int serverPort;
  private final Configuration conf;
  private transient Javalin app = null;
  private List<BaseService> services;
  private final MetadataStore metadataStore;
  private final HoodieTableServiceManagerConfig tableServiceManagerConfig;

  public HoodieTableServiceManager(CommandConfig config) {
    this.conf = FSUtils.prepareHadoopConf(new Configuration());
    this.tableServiceManagerConfig = CommandConfig.toTableServiceManagerConfig(config);
    this.serverPort = config.serverPort;
    this.metadataStore = initMetadataStore();
  }

  public void startService() {
    app = Javalin.create();
    RequestHandler requestHandler = new RequestHandler(app, conf, metadataStore);
    app.get("/", ctx -> ctx.result("Hello World"));
    requestHandler.register();
    app.start(serverPort);
    registerService();
    initAndStartRegisterService();
  }

  private MetadataStore initMetadataStore() {
    String metadataStoreClass = tableServiceManagerConfig.getMetadataStoreClass();
    MetadataStore metadataStore = (MetadataStore) ReflectionUtils.loadClass(metadataStoreClass,
        new Class<?>[] {HoodieTableServiceManagerConfig.class}, tableServiceManagerConfig);
    metadataStore.init();
    LOG.info("Finish init metastore : " + metadataStoreClass);
    return metadataStore;
  }

  private void registerService() {
    services = new ArrayList<>();
    ExecutorService executorService = new ExecutorService(metadataStore);
    services.add(executorService);
    services.add(new ScheduleService(executorService, metadataStore));
    services.add(new RetryService(metadataStore));
    services.add(new RestoreService(metadataStore));
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
                      "*** shutting down table service manager since JVM is shutting down");
                  try {
                    HoodieTableServiceManager.this.stop();
                  } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                  }
                  System.err.println("*** Table table service manager shut down");
                }));
  }

  /**
   * Stop serving requests and shutdown resources.
   */
  public void stop() throws InterruptedException {
    if (app != null) {
      LOG.info("Stop table service manager...");
      this.app.stop();
      this.app = null;
    }
    stopRegisterService();
  }

  public static void main(String[] args) throws Exception {
    System.out.println("SPARK_HOME = " + System.getenv("SPARK_HOME"));
    final CommandConfig cfg = new CommandConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
      cmd.usage();
      System.exit(1);
    }
    HoodieTableServiceManager service = new HoodieTableServiceManager(cfg);
    service.run();
  }
}
