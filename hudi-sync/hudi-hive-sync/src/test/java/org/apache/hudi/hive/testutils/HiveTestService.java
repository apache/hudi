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

package org.apache.hudi.hive.testutils;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.TUGIBasedProcessor;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.TUGIContainingTransport;
import org.apache.hive.service.server.HiveServer2;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HiveTestService {

  private static final Logger LOG = LogManager.getLogger(HiveTestService.class);

  private static final int CONNECTION_TIMEOUT = 30000;

  /**
   * Configuration settings.
   */
  private Configuration hadoopConf;
  private String workDir;
  private String bindIP = "127.0.0.1";
  private int metastorePort = 9083;
  private int serverPort = 9999;
  private boolean clean = true;

  private Map<String, String> sysProps = new HashMap<>();
  private ExecutorService executorService;
  private TServer tServer;
  private HiveServer2 hiveServer;

  public HiveTestService(Configuration configuration) throws IOException {
    this.workDir = Files.createTempDirectory(System.currentTimeMillis() + "-").toFile().getAbsolutePath();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public HiveServer2 start() throws IOException {
    Objects.requireNonNull(workDir, "The work dir must be set before starting cluster.");

    if (hadoopConf == null) {
      hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    }

    String localHiveLocation = getHiveLocation(workDir);
    if (clean) {
      LOG.info("Cleaning Hive cluster data at: " + localHiveLocation + " and starting fresh.");
      File file = new File(localHiveLocation);
      FileIOUtils.deleteDirectory(file);
    }

    HiveConf serverConf = configureHive(hadoopConf, localHiveLocation);

    executorService = Executors.newSingleThreadExecutor();
    tServer = startMetaStore(bindIP, metastorePort, serverConf);

    serverConf.set("hive.in.test", "true");
    hiveServer = startHiveServer(serverConf);

    String serverHostname;
    if (bindIP.equals("0.0.0.0")) {
      serverHostname = "localhost";
    } else {
      serverHostname = bindIP;
    }
    if (!waitForServerUp(serverConf, serverHostname, metastorePort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for startup of standalone server");
    }

    LOG.info("Hive Minicluster service started.");
    return hiveServer;
  }

  public void stop() {
    resetSystemProperties();
    if (tServer != null) {
      try {
        tServer.stop();
      } catch (Exception e) {
        LOG.error("Stop meta store failed", e);
      }
    }
    if (hiveServer != null) {
      try {
        hiveServer.stop();
      } catch (Exception e) {
        LOG.error("Stop hive server failed", e);
      }
    }
    if (executorService != null) {
      executorService.shutdownNow();
    }
    LOG.info("Hive Minicluster service shut down.");
    tServer = null;
    hiveServer = null;
    hadoopConf = null;
  }

  private HiveConf configureHive(Configuration conf, String localHiveLocation) throws IOException {
    conf.set("hive.metastore.local", "false");
    conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://" + bindIP + ":" + metastorePort);
    conf.set(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, bindIP);
    conf.setInt(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, serverPort);
    // The following line to turn of SASL has no effect since HiveAuthFactory calls
    // 'new HiveConf()'. This is fixed by https://issues.apache.org/jira/browse/HIVE-6657,
    // in Hive 0.14.
    // As a workaround, the property is set in hive-site.xml in this module.
    // conf.set(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, "NOSASL");
    File localHiveDir = new File(localHiveLocation);
    localHiveDir.mkdirs();
    File metastoreDbDir = new File(localHiveDir, "metastore_db");
    conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
        "jdbc:derby:" + metastoreDbDir.getPath() + ";create=true");
    File derbyLogFile = new File(localHiveDir, "derby.log");
    derbyLogFile.createNewFile();
    setSystemProperty("derby.stream.error.file", derbyLogFile.getPath());
    setSystemProperty("derby.system.home", localHiveDir.getAbsolutePath());
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
        Files.createTempDirectory(System.currentTimeMillis() + "-").toFile().getAbsolutePath());
    conf.set("datanucleus.schema.autoCreateTables", "true");
    conf.set("hive.metastore.schema.verification", "false");
    setSystemProperty("derby.stream.error.file", derbyLogFile.getPath());

    return new HiveConf(conf, this.getClass());
  }

  private boolean waitForServerUp(HiveConf serverConf, String hostname, int port, int timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        new HiveMetaStoreClient(serverConf);
        return true;
      } catch (MetaException e) {
        // ignore as this is expected
        LOG.info("server " + hostname + ":" + port + " not up " + e);
      }

      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  private void setSystemProperty(String name, String value) {
    if (!sysProps.containsKey(name)) {
      String currentValue = System.getProperty(name);
      sysProps.put(name, currentValue);
    }
    if (value != null) {
      System.setProperty(name, value);
    } else {
      System.getProperties().remove(name);
    }
  }

  private void resetSystemProperties() {
    for (Map.Entry<String, String> entry : sysProps.entrySet()) {
      if (entry.getValue() != null) {
        System.setProperty(entry.getKey(), entry.getValue());
      } else {
        System.getProperties().remove(entry.getKey());
      }
    }
    sysProps.clear();
  }

  private static String getHiveLocation(String baseLocation) {
    return baseLocation + Path.SEPARATOR + "hive";
  }

  private HiveServer2 startHiveServer(HiveConf serverConf) {
    HiveServer2 hiveServer = new HiveServer2();
    hiveServer.init(serverConf);
    hiveServer.start();
    return hiveServer;
  }

  // XXX: From org.apache.hadoop.hive.metastore.HiveMetaStore,
  // with changes to support binding to a specified IP address (not only 0.0.0.0)
  private static final class ChainedTTransportFactory extends TTransportFactory {

    private final TTransportFactory parentTransFactory;
    private final TTransportFactory childTransFactory;

    private ChainedTTransportFactory(TTransportFactory parentTransFactory, TTransportFactory childTransFactory) {
      this.parentTransFactory = parentTransFactory;
      this.childTransFactory = childTransFactory;
    }

    @Override
    public TTransport getTransport(TTransport trans) {
      return childTransFactory.getTransport(parentTransFactory.getTransport(trans));
    }
  }

  private static final class TServerSocketKeepAlive extends TServerSocket {

    public TServerSocketKeepAlive(int port) throws TTransportException {
      super(port, 0);
    }

    public TServerSocketKeepAlive(InetSocketAddress address) throws TTransportException {
      super(address, 0);
    }

    @Override
    protected TSocket acceptImpl() throws TTransportException {
      TSocket ts = super.acceptImpl();
      try {
        ts.getSocket().setKeepAlive(true);
      } catch (SocketException e) {
        throw new TTransportException(e);
      }
      return ts;
    }
  }

  public TServer startMetaStore(String forceBindIP, int port, HiveConf conf) throws IOException {
    try {
      // Server will create new threads up to max as necessary. After an idle
      // period, it will destory threads to keep the number of threads in the
      // pool to min.
      int minWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMINTHREADS);
      int maxWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMAXTHREADS);
      boolean tcpKeepAlive = conf.getBoolVar(HiveConf.ConfVars.METASTORE_TCP_KEEP_ALIVE);
      boolean useFramedTransport = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);

      // don't support SASL yet
      // boolean useSasl = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);

      TServerTransport serverTransport;
      if (forceBindIP != null) {
        InetSocketAddress address = new InetSocketAddress(forceBindIP, port);
        serverTransport = tcpKeepAlive ? new TServerSocketKeepAlive(address) : new TServerSocket(address);

      } else {
        serverTransport = tcpKeepAlive ? new TServerSocketKeepAlive(port) : new TServerSocket(port);
      }

      TProcessor processor;
      TTransportFactory transFactory;

      HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", conf, false);
      IHMSHandler handler = RetryingHMSHandler.getProxy(conf, baseHandler, true);

      if (conf.getBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI)) {
        transFactory = useFramedTransport
            ? new ChainedTTransportFactory(new TFramedTransport.Factory(), new TUGIContainingTransport.Factory())
            : new TUGIContainingTransport.Factory();

        processor = new TUGIBasedProcessor<>(handler);
        LOG.info("Starting DB backed MetaStore Server with SetUGI enabled");
      } else {
        transFactory = useFramedTransport ? new TFramedTransport.Factory() : new TTransportFactory();
        processor = new TSetIpAddressProcessor<>(handler);
        LOG.info("Starting DB backed MetaStore Server");
      }

      TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport).processor(processor)
          .transportFactory(transFactory).protocolFactory(new TBinaryProtocol.Factory())
          .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads);

      final TServer tServer = new TThreadPoolServer(args);
      executorService.submit(tServer::serve);
      return tServer;
    } catch (Throwable x) {
      throw new IOException(x);
    }
  }
}
