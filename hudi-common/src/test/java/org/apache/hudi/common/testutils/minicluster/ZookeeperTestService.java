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

package org.apache.hudi.common.testutils.minicluster;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.util.Objects;

import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * A Zookeeper minicluster service implementation.
 * <p/>
 * This class was ripped from MiniZooKeeperCluster from the HBase tests. Changes made include:
 * <p/>
 * 1. It will now only launch 1 zookeeper server.
 * <p/>
 * 2. It will only attempt to bind to the port specified, and will fail if it can't.
 * <p/>
 * 3. The startup method now takes a bindAddress, which allows us to configure which IP the ZK server binds to. This was
 * not configurable in the original class.
 * <p/>
 * 4. The ZK cluster will re-use a data dir on the local filesystem if it already exists instead of blowing it away.
 */
@Slf4j
public class ZookeeperTestService {

  private static final int TICK_TIME = 2000;
  private static final int CONNECTION_TIMEOUT = 30000;

  /**
   * Configuration settings.
   */
  @Getter
  private Configuration hadoopConf;
  private String workDir;
  private Integer clientPort = 2828;
  private String bindIP = "127.0.0.1";
  private Boolean clean = false;
  private int tickTime = 0;

  /**
   * Embedded ZooKeeper cluster.
   */
  private NIOServerCnxnFactory standaloneServerFactory;
  private ZooKeeperServer zooKeeperServer;
  private boolean started = false;

  public ZookeeperTestService(Configuration config) throws IOException {
    this.workDir = Files.createTempDirectory(System.currentTimeMillis() + "-").toFile().getAbsolutePath();
    this.hadoopConf = config;
  }

  public ZooKeeperServer start() throws IOException, InterruptedException {
    Objects.requireNonNull(workDir, "The localBaseFsLocation must be set before starting cluster.");

    setupTestEnv();
    stop();

    File dir = new File(workDir, "zookeeper").getAbsoluteFile();
    recreateDir(dir, clean);
    int tickTimeToUse;
    if (this.tickTime > 0) {
      tickTimeToUse = this.tickTime;
    } else {
      tickTimeToUse = TICK_TIME;
    }
    this.zooKeeperServer = new ZooKeeperServer(dir, dir, tickTimeToUse);
    standaloneServerFactory = new NIOServerCnxnFactory();

    // NOTE: Changed from the original, where InetSocketAddress was
    // originally created to bind to the wildcard IP, we now configure it.
    log.info("Zookeeper force binding to: " + this.bindIP);
    standaloneServerFactory.configure(new InetSocketAddress(bindIP, clientPort), 1000);

    // Start up this ZK server
    standaloneServerFactory.startup(zooKeeperServer);

    String serverHostname;
    if (bindIP.equals("0.0.0.0")) {
      serverHostname = "localhost";
    } else {
      serverHostname = bindIP;
    }
    if (!waitForServerUp(serverHostname, clientPort, CONNECTION_TIMEOUT)) {
      throw new IOException("Waiting for startup of standalone server");
    }

    started = true;
    log.info("Zookeeper Minicluster service started on client port: " + clientPort);
    return zooKeeperServer;
  }

  public void stop() throws RuntimeException {
    if (!started) {
      return;
    }

    standaloneServerFactory.shutdown();
    if (!waitForServerDown(clientPort, CONNECTION_TIMEOUT)) {
      throw new RuntimeException("Waiting for shutdown of standalone server");
    }

    // clear everything
    started = false;
    standaloneServerFactory = null;
    zooKeeperServer = null;

    log.info("Zookeeper Minicluster service shut down.");
  }

  private void recreateDir(File dir, boolean clean) throws IOException {
    if (dir.exists() && clean) {
      FileUtil.fullyDelete(dir);
    } else if (dir.exists() && !clean) {
      // the directory's exist, and we don't want to clean, so exit
      return;
    }
    try {
      dir.mkdirs();
    } catch (SecurityException e) {
      throw new IOException("creating dir: " + dir, e);
    }
  }

  // / XXX: From o.a.zk.t.ClientBase
  private static void setupTestEnv() {
    // during the tests we run with 100K prealloc in the logs.
    // on windows systems prealloc of 64M was seen to take ~15seconds
    // resulting in test failure (client timeout on first session).
    // set env and directly in order to handle static init/gc issues
    System.setProperty("zookeeper.preAllocSize", "100");
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    FileTxnLog.setPreallocSize(100 * 1024);
  }

  // XXX: From o.a.zk.t.ClientBase
  private static boolean waitForServerDown(int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        try (Socket sock = new Socket("localhost", port)) {
          OutputStream outstream = sock.getOutputStream();
          outstream.write(getUTF8Bytes("stat"));
          outstream.flush();
        }
      } catch (IOException e) {
        return true;
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

  // XXX: From o.a.zk.t.ClientBase
  private static boolean waitForServerUp(String hostname, int port, long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        Socket sock = new Socket(hostname, port);
        BufferedReader reader = null;
        try {
          OutputStream outstream = sock.getOutputStream();
          outstream.write(getUTF8Bytes("stat"));
          outstream.flush();

          Reader isr = new InputStreamReader(sock.getInputStream());
          reader = new BufferedReader(isr);
          String line = reader.readLine();
          if (line != null && line.startsWith("Zookeeper version:")) {
            return true;
          }
        } finally {
          sock.close();
          if (reader != null) {
            reader.close();
          }
        }
      } catch (IOException e) {
        // ignore as this is expected
        log.info("server " + hostname + ":" + port + " not up " + e);
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

  public String connectString() {
    return bindIP + ":" + clientPort;
  }
}
