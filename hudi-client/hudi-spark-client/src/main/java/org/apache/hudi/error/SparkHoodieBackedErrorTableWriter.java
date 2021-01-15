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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
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

public class SparkHoodieBackedErrorTableWriter<T extends HoodieRecordPayload> extends
    HoodieBackedErrorTableWriter<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkHoodieBackedErrorTableWriter.class);

  public static HoodieBackedErrorTableWriter create(Configuration conf, HoodieWriteConfig writeConfig, HoodieEngineContext context) {
    return new SparkHoodieBackedErrorTableWriter(conf, writeConfig, context);
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
        HoodieRecordLocation hoodieRecordLocation = (HoodieRecordLocation) hoodieRecord.getNewLocation().get();

        String instancTime = hoodieRecordLocation.getInstantTime();
        String fileId = hoodieRecordLocation.getFileId();
        String message = errorsMap.get(hoodieKey).toString();

        OverwriteWithLatestAvroPayload data = (OverwriteWithLatestAvroPayload)hoodieRecord.getData();
        String record = HoodieAvroUtils.bytesToAvro(data.recordBytes, new Schema.Parser().parse(hoodieTable.getConfig().getSchema())).toString();

        Map<String, String> context = new HashMap<>();
        context.put("_error" + HoodieRecord.COMMIT_TIME_METADATA_FIELD, instancTime);
        context.put("_error" + HoodieRecord.RECORD_KEY_METADATA_FIELD, hoodieKey.getRecordKey());
        context.put("_error" + HoodieRecord.PARTITION_PATH_METADATA_FIELD, hoodieRecord.getPartitionPath());
        context.put("_error" + "_hoodie_file_id", fileId);
        context.put("_error" + "_table_name", hoodieTable.getConfig().getTableName());

        GenericRecord errorGenericRecord = new GenericData.Record(new Schema.Parser().parse(HoodieTableMetaClient.ERRORS_SCHEMA));

        errorGenericRecord.put("uuid", uuid);
        errorGenericRecord.put("ts", ts);
        errorGenericRecord.put("schema", hoodieTable.getConfig().getSchema());
        errorGenericRecord.put("record", record);
        errorGenericRecord.put("message", message);
        errorGenericRecord.put("context", context);

        HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(Option.of(errorGenericRecord));

        HoodieKey errorHoodieKey = new HoodieKey(uuid, partitionPath);
        errorHoodieRecords.add(new HoodieRecord(errorHoodieKey, hoodieAvroPayload));
      }
      return errorHoodieRecords.iterator();
    });

    SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, errorTableWriteConfig, true);
    String instantTime = writeClient.startCommit();
    writeClient.insertError(errorRecordJavaRDD, instantTime);
  }
}
