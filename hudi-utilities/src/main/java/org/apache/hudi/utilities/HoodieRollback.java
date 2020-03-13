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

package org.apache.hudi.utilities;

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utilities.config.AbstractCommandConfig;

import com.beust.jcommander.Parameter;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

public class HoodieRollback {

  private static final Logger LOG = Logger.getLogger(HoodieRollback.class);
  private final Config cfg;

  public HoodieRollback(Config cfg) {
    this.cfg = cfg;
  }

  private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withIndexConfig(
        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build()).build();
    return new HoodieWriteClient(jsc, config);
  }

  public int rollback(JavaSparkContext jsc) throws Exception {
    HoodieWriteClient client = createHoodieClient(jsc, cfg.basePath);
    if (client.rollback(cfg.commitTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", cfg.commitTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", cfg.commitTime));
      return -1;
    }
  }

  public int rollbackToSavepoint(JavaSparkContext jsc) throws Exception {
    HoodieWriteClient client = createHoodieClient(jsc, cfg.basePath);
    if (client.rollbackToSavepoint(cfg.savepointTime)) {
      LOG.info(String.format("The commit \"%s\" rolled back.", cfg.savepointTime));
      return 0;
    } else {
      LOG.info(String.format("The commit \"%s\" failed to roll back.", cfg.savepointTime));
      return -1;
    }
  }

  public static class Config extends AbstractCommandConfig {

    @Parameter(names = {"--commit-time",
        "-ct"}, description = "Commit time for rollback")
    public String commitTime = null;

    @Parameter(names = {"--savepoint-time",
        "-st"}, description = "Savepoint time for rollback")
    public String savepointTime = null;

    @Parameter(names = {"--base-path",
        "-bp"}, description = "Base path for the hoodie dataset", required = true)
    public String basePath = null;
  }
}
