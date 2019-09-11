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

package org.apache.hudi.common.minicluster;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * An HDFS minicluster service implementation.
 */
public class HdfsTestService {

  private static final Logger logger = LogManager.getLogger(HdfsTestService.class);

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private String workDir;
  private String bindIP = "127.0.0.1";
  private int namenodeRpcPort = 8020;
  private int namenodeHttpPort = 50070;
  private int datanodePort = 50010;
  private int datanodeIpcPort = 50020;
  private int datanodeHttpPort = 50075;

  /**
   * Embedded HDFS cluster
   */
  private MiniDFSCluster miniDfsCluster;

  public HdfsTestService() {
    workDir = Files.createTempDir().getAbsolutePath();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public MiniDFSCluster start(boolean format) throws IOException {
    Preconditions.checkState(workDir != null, "The work dir must be set before starting cluster.");
    hadoopConf = HoodieTestUtils.getDefaultHadoopConf();

    // If clean, then remove the work dir so we can start fresh.
    String localDFSLocation = getDFSLocation(workDir);
    if (format) {
      logger.info("Cleaning HDFS cluster data at: " + localDFSLocation + " and starting fresh.");
      File file = new File(localDFSLocation);
      FileIOUtils.deleteDirectory(file);
    }

    // Configure and start the HDFS cluster
    // boolean format = shouldFormatDFSCluster(localDFSLocation, clean);
    hadoopConf = configureDFSCluster(hadoopConf, localDFSLocation, bindIP, namenodeRpcPort, namenodeHttpPort,
        datanodePort, datanodeIpcPort, datanodeHttpPort);
    miniDfsCluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(1).format(format).checkDataNodeAddrConfig(true)
        .checkDataNodeHostConfig(true).build();
    logger.info("HDFS Minicluster service started.");
    return miniDfsCluster;
  }

  public void stop() throws IOException {
    logger.info("HDFS Minicluster service being shut down.");
    miniDfsCluster.shutdown();
    miniDfsCluster = null;
    hadoopConf = null;
  }

  /**
   * Get the location on the local FS where we store the HDFS data.
   *
   * @param baseFsLocation The base location on the local filesystem we have write access to create dirs.
   * @return The location for HDFS data.
   */
  private static String getDFSLocation(String baseFsLocation) {
    return baseFsLocation + Path.SEPARATOR + "dfs";
  }

  /**
   * Returns true if we should format the DFS Cluster. We'll format if clean is true, or if the dfsFsLocation does not
   * exist.
   *
   * @param localDFSLocation The location on the local FS to hold the HDFS metadata and block data
   * @param clean            Specifies if we want to start a clean cluster
   * @return Returns true if we should format a DFSCluster, otherwise false
   */
  private static boolean shouldFormatDFSCluster(String localDFSLocation, boolean clean) {
    boolean format = true;
    File f = new File(localDFSLocation);
    if (f.exists() && f.isDirectory() && !clean) {
      format = false;
    }
    return format;
  }

  /**
   * Configure the DFS Cluster before launching it.
   *
   * @param config           The already created Hadoop configuration we'll further configure for HDFS
   * @param localDFSLocation The location on the local filesystem where cluster data is stored
   * @param bindIP           An IP address we want to force the datanode and namenode to bind to.
   * @return The updated Configuration object.
   */
  private static Configuration configureDFSCluster(Configuration config, String localDFSLocation, String bindIP,
      int namenodeRpcPort, int namenodeHttpPort, int datanodePort, int datanodeIpcPort, int datanodeHttpPort) {

    logger.info("HDFS force binding to ip: " + bindIP);
    config.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + bindIP + ":" + namenodeRpcPort);
    config.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, bindIP + ":" + datanodePort);
    config.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, bindIP + ":" + datanodeIpcPort);
    config.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, bindIP + ":" + datanodeHttpPort);
    // When a datanode registers with the namenode, the Namenode do a hostname
    // check of the datanode which will fail on OpenShift due to reverse DNS
    // issues with the internal IP addresses. This config disables that check,
    // and will allow a datanode to connect regardless.
    config.setBoolean("dfs.namenode.datanode.registration.ip-hostname-check", false);
    config.set("hdfs.minidfs.basedir", localDFSLocation);
    // allow current user to impersonate others
    String user = System.getProperty("user.name");
    config.set("hadoop.proxyuser." + user + ".groups", "*");
    config.set("hadoop.proxyuser." + user + ".hosts", "*");
    return config;
  }

}
