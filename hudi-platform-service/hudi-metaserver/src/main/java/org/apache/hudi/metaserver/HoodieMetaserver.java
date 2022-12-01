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

import jdk.jfr.Experimental;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metaserver.service.HoodieMetaserverService;
import org.apache.hudi.metaserver.service.HoodieMetaserverProxyHandler;
import org.apache.hudi.metaserver.service.TableService;
import org.apache.hudi.metaserver.service.TimelineService;
import org.apache.hudi.metaserver.store.RelationalDBBasedStorage;
import org.apache.hudi.metaserver.store.MetaserverStorage;
import org.apache.hudi.metaserver.thrift.MetaserverStorageException;
import org.apache.hudi.metaserver.thrift.ThriftHoodieMetaserver;
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
@Experimental
public class HoodieMetaserver {

  private static final Logger LOG = LogManager.getLogger(HoodieMetaserver.class);

  private static TServer server;
  private static Thread serverThread;
  private static MetaserverStorage metaserverStorage;
  private static HoodieMetaserverService metaserverService;

  public static void main(String[] args) {
    startServer();
  }

  public static void startServer() {
    try {
      if (server != null) {
        return;
      }
      metaserverStorage = new RelationalDBBasedStorage();
      // service
      TableService tableService = new TableService(metaserverStorage);
      TimelineService timelineService = new TimelineService(metaserverStorage);
      HoodieMetaserverService hoodieMetaserverService = new HoodieMetaserverService(tableService, timelineService);
      HoodieMetaserverProxyHandler proxyHandler = new HoodieMetaserverProxyHandler(hoodieMetaserverService);

      // start a thrift server
      ThriftHoodieMetaserver.Iface proxy = (ThriftHoodieMetaserver.Iface) Proxy
          .newProxyInstance(HoodieMetaserverProxyHandler.class.getClassLoader(),
              new Class[]{ThriftHoodieMetaserver.Iface.class}, proxyHandler);
      ThriftHoodieMetaserver.Processor processor = new ThriftHoodieMetaserver.Processor(proxy);
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

  public static ThriftHoodieMetaserver.Iface getEmbeddedMetaserver() {
    if (metaserverStorage == null) {
      synchronized (HoodieMetaserver.class) {
        if (metaserverStorage == null) {
          // TODO: add metastore factory.
          metaserverStorage = new RelationalDBBasedStorage();
          try {
            metaserverStorage.initStorage();
          } catch (MetaserverStorageException e) {
            throw new HoodieIOException("Fail to init the embedded metastore," + e);
          }
          TableService tableService = new TableService(metaserverStorage);
          TimelineService timelineService = new TimelineService(metaserverStorage);
          metaserverService = new HoodieMetaserverService(tableService, timelineService);
        }
      }
    }
    return metaserverService;
  }

  // only for test
  public static MetaserverStorage getMetadataStore() {
    return metaserverStorage;
  }

  public static void stop() {
    if (server != null) {
      LOG.info("Stop the server...");
      server.stop();
      serverThread.interrupt();
      server = null;
    }
    if (metaserverStorage != null) {
      metaserverStorage.close();
    }
  }
}
