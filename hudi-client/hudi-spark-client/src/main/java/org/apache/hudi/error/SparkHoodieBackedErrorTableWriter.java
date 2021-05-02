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

package org.apache.hudi.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

public class SparkHoodieBackedErrorTableWriter<T extends HoodieRecordPayload> extends
    HoodieBackedErrorTableWriter<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBackedErrorTableWriter.class);

  public static HoodieBackedErrorTableWriter create(Configuration conf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    return new SparkHoodieBackedErrorTableWriter(conf, writeConfig, engineContext);
  }

  SparkHoodieBackedErrorTableWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super(hadoopConf, writeConfig, engineContext);
  }

  @Override
  public void commit(JavaRDD<WriteStatus> writeStatuses,
               HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {

    try {
      String schema = hoodieTable.getConfig().getSchema();
      String tableName = hoodieTable.getConfig().getTableName();
      JavaRDD<HoodieRecord> errorRecordJavaRDD = writeStatuses.flatMap(writeStatus -> createErrorRecord(writeStatus, schema, tableName).iterator());
      SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, errorTableWriteConfig);
      String instantTime = writeClient.startCommit();
      writeClient.insertError(errorRecordJavaRDD, instantTime);
    } catch (Exception e) {
      LOG.error("commit error message Fail", e);
    }
  }
}
