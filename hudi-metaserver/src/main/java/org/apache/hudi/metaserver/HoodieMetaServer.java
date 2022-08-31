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

package org.apache.hudi.metaserver;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metaserver.service.HoodieMetaServerService;
import org.apache.hudi.metaserver.service.HoodieMetaServerProxyHandler;
import org.apache.hudi.metaserver.service.PartitionService;
import org.apache.hudi.metaserver.service.TableService;
import org.apache.hudi.metaserver.service.TimelineService;
import org.apache.hudi.metaserver.store.RelationDBBasedStore;
import org.apache.hudi.metaserver.store.MetadataStore;
import org.apache.hudi.metaserver.thrift.MetaStoreException;
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaServer;
import org.apache.hudi.metaserver.util.TServerSocketWrapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;

import java.lang.reflect.Proxy;

/**
 * Main class of hoodie meta server.
 */
public class HoodieMetaServer {

  private static final Logger LOG = LogManager.getLogger(HoodieMetaServer.class);

  private static TServer server;
  private static Thread serverThread;
  private static MetadataStore metadataStore;
  private static HoodieMetaServerService metaServerService;

  public static void main(String[] args) {
    startServer();
  }

  public static void startServer() {
    try {
      if (server != null) {
        return;
      }
      metadataStore = new RelationDBBasedStore();
      // service
      TableService tableService = new TableService(metadataStore);
      PartitionService partitionService = new PartitionService(metadataStore);
      TimelineService timelineService = new TimelineService(metadataStore);
      HoodieMetaServerService hoodieMetaServerService = new HoodieMetaServerService(tableService,
          partitionService, timelineService);
      HoodieMetaServerProxyHandler proxyHandler = new HoodieMetaServerProxyHandler(hoodieMetaServerService);

      // start a thrift server
      ThriftHoodieMetaServer.Iface proxy = (ThriftHoodieMetaServer.Iface) Proxy
          .newProxyInstance(HoodieMetaServerProxyHandler.class.getClassLoader(),
              new Class[]{ThriftHoodieMetaServer.Iface.class}, proxyHandler);
      ThriftHoodieMetaServer.Processor processor = new ThriftHoodieMetaServer.Processor(proxy);
      TServerTransport serverTransport = new TServerSocketWrapper(9090);
      server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
      LOG.info("Starting the server");
      serverThread = new Thread(() -> server.serve());
      serverThread.start();
    } catch (Exception e) {
      LOG.error("Fail to start the server", e);
      System.exit(1);
    }
  }

  public static ThriftHoodieMetaServer.Iface getEmbeddedMetaServer() {
    if (metadataStore == null) {
      synchronized (HoodieMetaServer.class) {
        if (metadataStore == null) {
          // TODO: add metastore factory.
          metadataStore = new RelationDBBasedStore();
          try {
            metadataStore.initStore();
          } catch (MetaStoreException e) {
            throw new HoodieIOException("Fail to init the embedded metastore," + e);
          }
          TableService tableService = new TableService(metadataStore);
          PartitionService partitionService = new PartitionService(metadataStore);
          TimelineService timelineService = new TimelineService(metadataStore);
          metaServerService = new HoodieMetaServerService(tableService, partitionService, timelineService);
        }
      }
    }
    return metaServerService;
  }

  // only for test
  public static MetadataStore getMetadataStore() {
    return metadataStore;
  }

  public static void stop() {
    if (server != null) {
      LOG.info("Stop the server...");
      server.stop();
      serverThread.interrupt();
      server = null;
    }
    if (metadataStore != null) {
      metadataStore.close();
    }
  }
}
