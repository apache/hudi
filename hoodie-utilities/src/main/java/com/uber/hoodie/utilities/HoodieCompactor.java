/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.configs.HoodieCompactorJobConfig;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class HoodieCompactor {

  private static volatile Logger logger = LogManager.getLogger(HoodieCompactor.class);
  private final HoodieCompactorJobConfig cfg;
  private transient FileSystem fs;

  public HoodieCompactor(HoodieCompactorJobConfig cfg) {
    this.cfg = cfg;
  }

  public static void main(String[] args) throws Exception {
    final HoodieCompactorJobConfig cfg = new HoodieCompactorJobConfig();
    cfg.parseJobConfig(args, true);

    HoodieCompactor compactor = new HoodieCompactor(cfg);
    compactor.compact(UtilHelpers.buildSparkContext("compactor-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory),
        cfg.retry);
  }

  public int compact(JavaSparkContext jsc, int retry) {
    this.fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    int ret = -1;
    try {
      do {
        if (cfg.runSchedule) {
          if (null == cfg.strategyClassName) {
            throw new IllegalArgumentException("Missing Strategy class name for running compaction");
          }
          ret = doSchedule(jsc);
        } else {
          ret = doCompact(jsc);
        }
      } while (ret != 0 && retry-- > 0);
    } catch (Throwable t) {
      logger.error(t);
    }
    return ret;
  }

  private int doCompact(JavaSparkContext jsc) throws Exception {
    //Get schema.
    String schemaStr = UtilHelpers.parseSchema(fs, cfg.schemaFile);
    HoodieWriteClient client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism,
        Optional.empty());
    JavaRDD<WriteStatus> writeResponse = client.compact(cfg.compactionInstantTime);
    return UtilHelpers.handleErrors(jsc, cfg.compactionInstantTime, writeResponse);
  }

  private int doSchedule(JavaSparkContext jsc) throws Exception {
    //Get schema.
    HoodieWriteClient client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, "", cfg.parallelism,
        Optional.of(cfg.strategyClassName));
    client.scheduleCompactionAtInstant(cfg.compactionInstantTime, Optional.empty());
    return 0;
  }
}
