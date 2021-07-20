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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.spark.api.java.JavaRDD;

public class SparkErrorTableWriteStatusWriter extends HoodieBackedErrorTableWriter<JavaRDD<WriteStatus>> {

  public static HoodieBackedErrorTableWriter create(Configuration conf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    return new SparkErrorTableWriteStatusWriter(conf, writeConfig, engineContext);
  }

  SparkErrorTableWriteStatusWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super(hadoopConf, writeConfig, engineContext);
  }

  @Override
  public void commit(JavaRDD<WriteStatus> writeStatuses, String schema, String tableName) {

    try {
      JavaRDD<HoodieRecord> errorRecordJavaRDD = writeStatuses.flatMap(writeStatus -> createErrorRecord(
          writeStatus.getFailedRecords(), writeStatus.getErrors(), schema, tableName).iterator());
      SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, errorTableWriteConfig);
      String instantTime = writeClient.startCommit();
      writeClient.insertError(errorRecordJavaRDD, instantTime);
    } catch (Exception e) {
      throw new HoodieException("commit error message Fail.", e);
    }
  }
}