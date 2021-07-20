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
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.stream.Collectors;

public class FlinkHoodieBackedErrorTableWriter<T extends HoodieRecordPayload> extends
    HoodieBackedErrorTableWriter<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> {

  public static HoodieBackedErrorTableWriter create(Configuration conf, HoodieWriteConfig writeConfig, HoodieEngineContext context) {
    return new FlinkHoodieBackedErrorTableWriter(conf, writeConfig, context);
  }

  FlinkHoodieBackedErrorTableWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    super(hadoopConf, writeConfig, engineContext);
  }

  @Override
  public void commit(List<WriteStatus> writeStatuses,
                     HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {

    try {
      String schema = hoodieTable.getConfig().getSchema();
      String tableName = hoodieTable.getConfig().getTableName();
      List<HoodieRecord> errorRecordJavaRDD = writeStatuses.stream().flatMap(writeStatus -> createErrorRecord(writeStatus, schema, tableName).stream()).collect(Collectors.toList());
      HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient(engineContext, errorTableWriteConfig);
      String instantTime = writeClient.startCommit();
      writeClient.insertError(errorRecordJavaRDD, instantTime);
    } catch (Exception e) {
      throw new HoodieException("commit error message Fail.", e);
    }
  }
}