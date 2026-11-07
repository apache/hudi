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

import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.TUGIBasedProcessor;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.thrift.TUGIContainingTransport;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.RetryableThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class HiveTestService {
  private static final int CONNECTION_TIMEOUT_MS = 30000;
  private static final int MAX_START_ATTEMPTS = 5;
  private static final String BIND_HOST = "127.0.0.1";

  private final Configuration hadoopConf;
  private final String workDir;
  private final Map<String, String> sysProps = new HashMap<>();
  private ExecutorService executorService;
  private TServer tServer;
  @Getter
  private HiveServer2 hiveServer;
  @Getter
  private HiveConf hiveConf;

  public HiveTestService(Configuration hadoopConf) throws IOException {
    this.workDir = Files.createTempDirectory(System.currentTimeMillis() + "-").toFile().getAbsolutePath();
    this.hadoopConf = hadoopConf;
  }

  public HiveServer2 start() throws IOException {
    Objects.requireNonNull(workDir, "The work dir must be set before starting cluster.");

    String localHiveLocation = getHiveLocation(workDir);
    log.info("Cleaning Hive cluster data at: {} and starting fresh.", localHiveLocation);
    File file = new File(localHiveLocation);
    FileIOUtils.deleteDirectory(file);

    for (int attempt = 1; attempt <= MAX_START_ATTEMPTS; attempt++) {
      try {
        int hs2ThriftPort = NetworkTestUtils.nextFreePort();
        int metastoreServerPort = NetworkTestUtils.nextFreePort();
        hiveConf = configureHive(hadoopConf, localHiveLocation, hs2ThriftPort, metastoreServerPort);

        executorService = Executors.newSingleThreadExecutor();
        tServer = startMetaStore(hiveConf);
        hiveServer = startHiveServer(hiveConf);

        if (!waitForServerUp(hiveConf)) {
          throw new IOException("Waiting for startup of standalone server");
        }

        log.info("Hive Minicluster service started.");
        return hiveServer;
      } catch (IOException | RuntimeException e) {
        boolean bindFailure = hasBindException(e);
        stop();

        if (!bindFailure || attempt == MAX_START_ATTEMPTS) {
          throw e;
        }

        log.warn("Hive Minicluster failed to bind on attempt {}. Retrying with new ports.", attempt, e);
      }
    }
    throw new IllegalStateException("Hive Minicluster failed to start");
  }

  public void stop() {
    resetSystemProperties();
    if (tServer != null) {
      try {
        tServer.stop();
      } catch (Exception e) {
        log.error("Stop meta store failed", e);
      }
    }
    if (hiveServer != null) {
      try {
        hiveServer.stop();
      } catch (Exception e) {
        log.error("Stop hive server failed", e);
      }
    }
    if (executorService != null) {
      executorService.shutdownNow();
    }
    log.info("Hive Minicluster service shut down.");
    tServer = null;
    hiveServer = null;
    executorService = null;
  }

  public int getHiveServerPort() {
    return hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT);
  }

  public String getJdbcHive2Url() {
    return String.format("jdbc:hive2://%s:%s/",
        BIND_HOST, hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT));
  }

  public HiveConf configureHive(Configuration hadoopConf, String localHiveLocation,
                                int hs2ThriftPort, int metastoreServerPort) throws IOException {
    hadoopConf.set("hive.metastore.local", "false");
    hadoopConf.set("datanucleus.schema.autoCreateTables", "true");
    hadoopConf.set("datanucleus.autoCreateSchema", "true");
    hadoopConf.set("datanucleus.fixedDatastore", "false");
    HiveConf conf = new HiveConf(hadoopConf, HiveConf.class);
    conf.setBoolVar(ConfVars.HIVE_IN_TEST, true);
    conf.setBoolVar(ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
    conf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, hs2ThriftPort);
    conf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, BIND_HOST);
    conf.setIntVar(ConfVars.METASTORE_SERVER_PORT, metastoreServerPort);
    conf.setVar(ConfVars.METASTOREURIS, "thrift://" + BIND_HOST + ":" + metastoreServerPort);
    File localHiveDir = new File(localHiveLocation);
    localHiveDir.mkdirs();
    File metastoreDbDir = new File(localHiveDir, "metastore_db");
    conf.setVar(ConfVars.METASTORECONNECTURLKEY, "jdbc:derby:" + metastoreDbDir.getPath() + ";create=true");
    File derbyLogFile = new File(localHiveDir, "derby.log");
    derbyLogFile.createNewFile();
    setSystemProperty("derby.stream.error.file", derbyLogFile.getPath());
    setSystemProperty("derby.system.home", localHiveDir.getAbsolutePath());
    File metastoreWarehouseDir = new File(localHiveDir, "warehouse");
    metastoreWarehouseDir.mkdir();
    conf.setVar(ConfVars.METASTOREWAREHOUSE, metastoreWarehouseDir.getAbsolutePath());

    return conf;
  }

  private boolean waitForServerUp(HiveConf serverConf) {
    log.info("waiting for {}", serverConf.getVar(ConfVars.METASTOREURIS));
    final long start = System.currentTimeMillis();
    while (true) {
      try {
        new HiveMetaStoreClient(serverConf);
        return true;
      } catch (MetaException ignored) {
        // ignore as this is expected
      }

      if (System.currentTimeMillis() > start + CONNECTION_TIMEOUT_MS) {
        break;
      }
      try {
        Thread.sleep(CONNECTION_TIMEOUT_MS / 10);
      } catch (InterruptedException ignored) {
        // no op
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
    return baseLocation + StoragePath.SEPARATOR + "hive";
  }

  private static boolean hasBindException(Throwable throwable) {
    Throwable cause = throwable;
    while (cause != null) {
      if (cause instanceof BindException) {
        return true;
      }
      if (cause == cause.getCause()) {
        break;
      }
      cause = cause.getCause();
    }
    return false;
  }

  private HiveServer2 startHiveServer(HiveConf serverConf) throws IOException {
    RetryableHiveServer2 retryableHiveServer = new RetryableHiveServer2();
    hiveServer = retryableHiveServer;
    hiveServer.init(serverConf);
    hiveServer.start();
    Throwable startupFailure;
    try {
      startupFailure = retryableHiveServer.awaitStartup(CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (TimeoutException e) {
      throw new IOException(e);
    }
    if (startupFailure instanceof RuntimeException) {
      throw (RuntimeException) startupFailure;
    }
    if (startupFailure instanceof Error) {
      throw (Error) startupFailure;
    }
    if (startupFailure != null) {
      throw new IOException(startupFailure);
    }
    return hiveServer;
  }

  private static final class RetryableHiveServer2 extends HiveServer2 {

    private RetryableThriftBinaryCLIService thriftCLIService;

    @Override
    public synchronized void init(HiveConf hiveConf) {
      super.init(hiveConf);
      CLIService cliService = null;
      ThriftCLIService originalThriftCLIService = null;
      for (Service service : getServices()) {
        if (service instanceof CLIService) {
          cliService = (CLIService) service;
        } else if (service instanceof ThriftCLIService) {
          originalThriftCLIService = (ThriftCLIService) service;
        }
      }
      if (cliService == null || originalThriftCLIService == null) {
        throw new IllegalStateException("HiveServer2 services were not initialized");
      }
      removeService(originalThriftCLIService);
      thriftCLIService = new RetryableThriftBinaryCLIService(cliService, this::stop);
      thriftCLIService.init(hiveConf);
      addService(thriftCLIService);
    }

    private Throwable awaitStartup(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
      return thriftCLIService.awaitStartup(timeout, timeUnit);
    }
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
    public TTransport getTransport(TTransport trans) throws TTransportException {
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
    public TSocket accept() throws TTransportException {
      TSocket ts = super.accept();
      try {
        ts.getSocket().setKeepAlive(true);
      } catch (SocketException e) {
        throw new TTransportException(e);
      }
      return ts;
    }
  }

  private TServer startMetaStore(HiveConf conf) throws IOException {
    try {
      // Server will create new threads up to max as necessary. After an idle
      // period, it will destroy threads to keep the number of threads in the
      // pool to min.
      String host = conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
      int port = conf.getIntVar(ConfVars.METASTORE_SERVER_PORT);
      int minWorkerThreads = conf.getIntVar(ConfVars.METASTORESERVERMINTHREADS);
      int maxWorkerThreads = conf.getIntVar(ConfVars.METASTORESERVERMAXTHREADS);
      boolean tcpKeepAlive = conf.getBoolVar(ConfVars.METASTORE_TCP_KEEP_ALIVE);
      boolean useFramedTransport = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);

      InetSocketAddress address = new InetSocketAddress(host, port);
      TServerTransport serverTransport = tcpKeepAlive ? new TServerSocketKeepAlive(address) : new TServerSocket(address);

      TProcessor processor;
      TTransportFactory transFactory;

      HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", conf, false);
      IHMSHandler handler = RetryingHMSHandler.getProxy(conf, baseHandler, true);

      if (conf.getBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI)) {
        transFactory = useFramedTransport
            ? new ChainedTTransportFactory(new TFramedTransport.Factory(), new TUGIContainingTransport.Factory())
            : new TUGIContainingTransport.Factory();

        processor = new TUGIBasedProcessor<>(handler);
        log.info("Starting DB backed MetaStore Server with SetUGI enabled");
      } else {
        transFactory = useFramedTransport ? new TFramedTransport.Factory() : new TTransportFactory();
        processor = new TSetIpAddressProcessor<>(handler);
        log.info("Starting DB backed MetaStore Server");
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
