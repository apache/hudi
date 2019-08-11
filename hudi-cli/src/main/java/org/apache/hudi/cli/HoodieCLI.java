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

package org.apache.hudi.cli;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.FSUtils;

public class HoodieCLI {

  public static Configuration conf;
  public static ConsistencyGuardConfig consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().build();
  public static FileSystem fs;
  public static CLIState state = CLIState.INIT;
  public static String basePath;
  public static HoodieTableMetaClient tableMetadata;
  public static HoodieTableMetaClient syncTableMetadata;


  public enum CLIState {
    INIT, DATASET, SYNC
  }

  public static void setConsistencyGuardConfig(ConsistencyGuardConfig config) {
    consistencyGuardConfig = config;
  }

  private static void setTableMetaClient(HoodieTableMetaClient tableMetadata) {
    HoodieCLI.tableMetadata = tableMetadata;
  }

  private static void setBasePath(String basePath) {
    HoodieCLI.basePath = basePath;
  }

  public static boolean initConf() {
    if (HoodieCLI.conf == null) {
      HoodieCLI.conf = FSUtils.prepareHadoopConf(new Configuration());
      return true;
    }
    return false;
  }

  public static void initFS(boolean force) throws IOException {
    if (fs == null || force) {
      fs = (tableMetadata != null) ? tableMetadata.getFs() : FileSystem.get(conf);
    }
  }

  public static void refreshTableMetadata() {
    setTableMetaClient(new HoodieTableMetaClient(HoodieCLI.conf, basePath, false, HoodieCLI.consistencyGuardConfig));
  }

  public static void connectTo(String basePath) {
    setBasePath(basePath);
    refreshTableMetadata();
  }
}
