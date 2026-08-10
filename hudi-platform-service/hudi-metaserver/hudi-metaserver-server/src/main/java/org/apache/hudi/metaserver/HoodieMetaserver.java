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

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metaserver.service.HoodieMetaserverGateway;
import org.apache.hudi.metaserver.service.HoodieMetaserverProxyHandler;
import org.apache.hudi.metaserver.service.TableService;
import org.apache.hudi.metaserver.service.TimelineService;
import org.apache.hudi.metaserver.store.MetaserverStorage;
import org.apache.hudi.metaserver.store.RelationalDBBasedStorage;
import org.apache.hudi.metaserver.thrift.MetaserverStorageException;
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaserver;
import org.apache.hudi.metaserver.util.TServerSocketWrapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;

import java.lang.reflect.Proxy;

/**
 * Main class of hoodie meta server.
 *
 * @since 0.13.0
 * @Experimental
 */
@Slf4j
public class HoodieMetaserver {

  private static TServer server;
  private static Thread serverThread;
  private static volatile MetaserverStorage metaserverStorage;
  private static HoodieMetaserverGateway metaserverGateway;

  public static void main(String[] args) {
    startServer();
  }

  public static void startServer() {
    try {
      if (server != null) {
        return;
      }
      metaserverStorage = new RelationalDBBasedStorage();
      try {
        metaserverStorage.initStorage();
      } catch (MetaserverStorageException e) {
        throw new HoodieException("Fail to init the Metaserver's storage." + e);
      }
      // service
      TableService tableService = new TableService(metaserverStorage);
      TimelineService timelineService = new TimelineService(metaserverStorage);
      HoodieMetaserverGateway hoodieMetaserverGateway = new HoodieMetaserverGateway(tableService, timelineService);
      HoodieMetaserverProxyHandler proxyHandler = new HoodieMetaserverProxyHandler(hoodieMetaserverGateway);

      // start a thrift server
      ThriftHoodieMetaserver.Iface proxy = (ThriftHoodieMetaserver.Iface) Proxy
          .newProxyInstance(HoodieMetaserverProxyHandler.class.getClassLoader(),
              new Class[]{ThriftHoodieMetaserver.Iface.class}, proxyHandler);
      ThriftHoodieMetaserver.Processor processor = new ThriftHoodieMetaserver.Processor(proxy);
      TServerTransport serverTransport = new TServerSocketWrapper(9090);
      server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
      log.info("Starting the server");
      serverThread = new Thread(() -> server.serve());
      serverThread.start();
    } catch (Exception e) {
      log.error("Failed to start Metaserver.", e);
      System.exit(1);
    }
  }

  public static ThriftHoodieMetaserver.Iface getEmbeddedMetaserver() {
    if (metaserverStorage == null) {
      synchronized (HoodieMetaserver.class) {
        if (metaserverStorage == null) {
          // TODO: add metaserver factory.
          metaserverStorage = new RelationalDBBasedStorage();
          try {
            metaserverStorage.initStorage();
          } catch (MetaserverStorageException e) {
            throw new HoodieException("Fail to init the Metaserver's storage." + e);
          }
          TableService tableService = new TableService(metaserverStorage);
          TimelineService timelineService = new TimelineService(metaserverStorage);
          metaserverGateway = new HoodieMetaserverGateway(tableService, timelineService);
        }
      }
    }
    return metaserverGateway;
  }

  // only for test
  public static MetaserverStorage getMetaserverStorage() {
    return metaserverStorage;
  }

  public static void stopServer() {
    if (server != null) {
      log.info("Stop the server...");
      server.stop();
      serverThread.interrupt();
      server = null;
    }
    if (metaserverStorage != null) {
      metaserverStorage.close();
    }
  }
}
