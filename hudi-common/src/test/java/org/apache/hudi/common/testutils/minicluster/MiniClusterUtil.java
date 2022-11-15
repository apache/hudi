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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.IOException;

/**
 * A utility class about mini cluster.
 */
public class MiniClusterUtil {

  private static MiniDFSCluster dfsCluster;
  private static ZooKeeperServer zkServer;
  public static Configuration configuration;
  public static FileSystem fileSystem;

  public static void setUp() throws IOException, InterruptedException {
    if (dfsCluster == null) {
      HdfsTestService service = new HdfsTestService();
      dfsCluster = service.start(true);
      configuration = service.getHadoopConf();
    }
    if (zkServer == null) {
      ZookeeperTestService zkService = new ZookeeperTestService(configuration);
      zkServer = zkService.start();
    }
    fileSystem = FileSystem.get(configuration);
  }

  public static void shutdown() {
    if (dfsCluster != null) {
      dfsCluster.shutdown(true, true);
    }
    if (zkServer != null) {
      zkServer.shutdown(true);
    }
  }
}
