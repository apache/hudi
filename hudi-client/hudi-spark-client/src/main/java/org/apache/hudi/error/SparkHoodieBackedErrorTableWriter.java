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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieErrorTableConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_TS;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_UUID;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_SCHEMA;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_RECORD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_MESSAGE;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_CONTEXT;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_FILE_ID_FIELD;
import static org.apache.hudi.common.config.HoodieErrorTableConfig.ERROR_TABLE_NAME;

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
  protected void initialize(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) {

    try {
      bootstrapErrorTable(datasetMetaClient);
    } catch (IOException e) {
      LOG.error("init error table fail", e);
    }
  }

  @Override
  public void commit(JavaRDD<WriteStatus> writeStatuses,
               HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {

    JavaRDD<HoodieRecord> errorRecordJavaRDD = writeStatuses.flatMap(writeStatus -> {

      HashMap<HoodieKey, Throwable> errorsMap = writeStatus.getErrors();
      List<HoodieRecord> errorHoodieRecords = new ArrayList<>();
      for (HoodieRecord hoodieRecord : writeStatus.getFailedRecords()) {

        String uuid = UUID.randomUUID().toString();

        long timeMillis = System.currentTimeMillis();
        String ts = String.valueOf(timeMillis);
        DateTimeZone dateTimeZone = null;
        String partitionPath = new DateTime(timeMillis, dateTimeZone).toString("yyyy/MM/dd");

        HoodieKey hoodieKey = hoodieRecord.getKey();

        HoodieRecordLocation hoodieRecordLocation = null;
        if (hoodieRecord.getNewLocation().isPresent()) {
          hoodieRecordLocation = (HoodieRecordLocation) hoodieRecord.getNewLocation().get();
        }

        String instancTime = hoodieRecordLocation == null ? "" : hoodieRecordLocation.getInstantTime();
        String fileId = hoodieRecordLocation == null ? "" : hoodieRecordLocation.getFileId();
        String message = errorsMap.get(hoodieKey).toString();

        OverwriteWithLatestAvroPayload data = (OverwriteWithLatestAvroPayload)hoodieRecord.getData();

        Map<String, String> context = new HashMap<>();
        context.put(ERROR_COMMIT_TIME_METADATA_FIELD, instancTime);
        context.put(ERROR_RECORD_KEY_METADATA_FIELD, hoodieKey.getRecordKey());
        context.put(ERROR_PARTITION_PATH_METADATA_FIELD, hoodieRecord.getPartitionPath());
        context.put(ERROR_FILE_ID_FIELD, fileId);
        context.put(ERROR_TABLE_NAME, hoodieTable.getConfig().getTableName());

        GenericRecord errorGenericRecord = new GenericData.Record(new Schema.Parser().parse(HoodieErrorTableConfig.ERROR_TABLE_SCHEMA));

        errorGenericRecord.put(ERROR_RECORD_UUID, uuid);
        errorGenericRecord.put(ERROR_RECORD_TS, ts);
        errorGenericRecord.put(ERROR_RECORD_SCHEMA, hoodieTable.getConfig().getSchema());
        errorGenericRecord.put(ERROR_RECORD_RECORD, data.getGenericRecord().toString());
        errorGenericRecord.put(ERROR_RECORD_MESSAGE, message);
        errorGenericRecord.put(ERROR_RECORD_CONTEXT, context);

        HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(Option.of(errorGenericRecord));

        HoodieKey errorHoodieKey = new HoodieKey(uuid, partitionPath);
        errorHoodieRecords.add(new HoodieRecord(errorHoodieKey, hoodieAvroPayload));
      }
      return errorHoodieRecords.iterator();
    });

    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, errorTableWriteConfig);
    String instantTime = writeClient.startCommit();
    writeClient.insertError(errorRecordJavaRDD, instantTime);
  }
}
