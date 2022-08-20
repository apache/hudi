/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.delta.sync;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hudi.sync.common.HoodieSyncTool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class DeltaLakeSyncTool extends HoodieSyncTool {

  private static final Logger LOG = LogManager.getLogger(DeltaLakeSyncTool.class);

  private static final String DELTA_LOG_DIR = "_delta_log";
  private final String basePath;
  //private final 20d.
  private final HoodieTableMetaClient metaClient;
  private final FileSystem fs;

  public DeltaLakeSyncTool(Properties props, FileSystem fileSystem) {
    super(props, fileSystem);
    this.fs = fileSystem;
    basePath = props.getProperty(HoodieSyncConfig.META_SYNC_BASE_PATH.key());
    this.metaClient = HoodieTableMetaClient.builder().setConf(fileSystem.getConf()).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
  }

  @Override
  public void syncHoodieTable() {
    try {
      if (metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().isPresent()) {
        HoodieInstant instant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get();
        LOG.warn("Looking to sync " + instant.toString());
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
            .fromBytes(metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);

        Path deltaLogPath = new Path(basePath + "/" + DELTA_LOG_DIR);
        if (!fs.exists(deltaLogPath)) {
          fs.mkdirs(deltaLogPath);
        }
        FileStatus[] files = fs.listStatus(deltaLogPath);
        int totalFiles = files.length;
        String nextFileName = String.format("%020d", totalFiles) + ".json";

        if (totalFiles == 0) {
          String newId = UUID.randomUUID().toString();
          CommitInfo commitInfo = new CommitInfo();
          commitInfo.timestamp = System.currentTimeMillis();
          commitInfo.operation = "WRITE";
          commitInfo.isBlindAppend = true;
          OperationParameters operationParameters = new OperationParameters();
          operationParameters.mode = "ErrorIfExists";
          operationParameters.partitionBy = metaClient.getTableConfig().getPartitionFields().get();
          commitInfo.operationParameters = operationParameters;
          OperationMetrics operationMetrics = new OperationMetrics();
          operationMetrics.numFiles = String.valueOf(commitMetadata.getWriteStats().size());
          operationMetrics.numOutputBytes = String.valueOf(commitMetadata.fetchTotalBytesWritten());
          operationMetrics.numOutputRows = String.valueOf(commitMetadata.fetchTotalRecordsWritten());
          commitInfo.operationMetrics = operationMetrics;

          Protocol protocol = new Protocol();

          Metadata metadata = new Metadata();
          metadata.id = newId;
          Format format = new Format();
          format.provider = "parquet";
          metadata.format = format;
          String avroSchema = commitMetadata.getMetadata("schema");
          Schema schema = new Schema.Parser().parse(avroSchema);
          Schema hudiSchema = getHudiSchema(schema);
          String structTypeStr = AvroConversionUtils.convertAvroSchemaToStructType(hudiSchema).json();
          metadata.schemaString = structTypeStr;
          metadata.partitionColumns = metaClient.getTableConfig().getPartitionFields().get();
          metadata.createdTime = commitInfo.timestamp;

          List<AddRecord> addList = new ArrayList<>();
          commitMetadata.getPartitionToWriteStats().values().forEach(entry -> {
                entry.forEach(writeStat -> {
                  Add add = new Add();
                  add.dataChange = true;
                  add.path = writeStat.getPath();
                  Map<String, String> partitionValueMap = new HashMap<>();
                  partitionValueMap.put(metaClient.getTableConfig().getPartitionFieldProp(), writeStat.getPartitionPath().substring(writeStat.getPartitionPath().indexOf('=') + 1));
                  add.partitionValues = partitionValueMap;
                  add.size = writeStat.getFileSizeInBytes();
                  add.modificationTime = commitInfo.timestamp;
                  AddRecord addRecord = new AddRecord();
                  addRecord.add = add;
                  addList.add(addRecord);
                });
              }
          );

          ObjectMapper mapper = new ObjectMapper();
          mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
          CommitInfoWrapper commitInfoWrapper = new CommitInfoWrapper();
          commitInfoWrapper.commitInfo = commitInfo;
          String commitInfoStr = mapper.writeValueAsString(commitInfoWrapper);
          ProtocolWrapper protocolWrapper = new ProtocolWrapper();
          protocolWrapper.protocol = protocol;
          String protocolStr = mapper.writeValueAsString(protocolWrapper);
          MetadataWrapper metadataWrapper = new MetadataWrapper();
          metadataWrapper.metaData = metadata;
          String metadataStr = mapper.writeValueAsString(metadataWrapper);

          // String addListStr = mapper.writeValueAsString(addList);
          String outStr = commitInfoStr + "\n" + protocolStr + "\n" + metadataStr + "\n";
          for (AddRecord addRecord : addList) {
            outStr += mapper.writeValueAsString(addRecord) + "\n";
          }

          LOG.warn("Final output string " + outStr);

          LOG.warn(" File to be written " + new Path(deltaLogPath.toString() + "/" + nextFileName).toString());
          FSDataOutputStream stream = null;
          stream = fs.create(new Path(deltaLogPath.toString() + "/" + nextFileName));
          stream.write(outStr.getBytes(StandardCharsets.UTF_8));
          stream.flush();
          // stream.sync();
          stream.close();
        } else {
          String lastFileName = String.format("%020d", totalFiles - 1) + ".json";
          Path lastFilePath = new Path(deltaLogPath.toString() + "/" + lastFileName);
          FSDataInputStream inputStream = fs.open(lastFilePath);
          String commitInfoStrOld = inputStream.readLine();
          String protocolStrOld = inputStream.readLine();
          String metadataStrOld = inputStream.readLine();

          String str = inputStream.readLine();
          List<String> addRecordStr = new ArrayList<>();
          while (str != null) {
            addRecordStr.add(str);
            str = inputStream.readLine();
          }

          inputStream.close();

          ObjectMapper mapper = new ObjectMapper();
          CommitInfoWrapper commitInfoWrapperOld = mapper.readValue(commitInfoStrOld, CommitInfoWrapper.class);
          ProtocolWrapper protocolWrapperOld = mapper.readValue(protocolStrOld, ProtocolWrapper.class);
          MetadataWrapper metadataWrapperOld = mapper.readValue(metadataStrOld, MetadataWrapper.class);
          List<AddRecord> addRecordsOld = new ArrayList<>();
          addRecordStr.forEach(entry -> {
            try {
              addRecordsOld.add(mapper.readValue(entry, AddRecord.class));
            } catch (IOException e) {
              e.printStackTrace();
            }
          });

          LOG.warn(" Commit str read " + commitInfoStrOld);
          LOG.warn(" Protocol str read " + protocolStrOld);
          LOG.warn(" Metadata str read " + metadataStrOld);
          addRecordsOld.forEach(entry -> LOG.warn("Add rec : " + entry));

          CommitInfo commitInfo = new CommitInfo();
          commitInfo.timestamp = System.currentTimeMillis();
          commitInfo.operation = "MERGE";
          OperationParameters operationParameters = new OperationParameters();
          operationParameters.predicate = "(table1.`uuid` = table2.`uuid)";
          Map<String, String> matchedPredicates = new HashMap<>();
          matchedPredicates.put("actionType", "update");
          Map<String, String> nonMatchedPredicates = new HashMap<>();
          nonMatchedPredicates.put("actionType", "insert");
          operationParameters.matchedPredicates = matchedPredicates;
          operationParameters.nonMatchedPredicates = nonMatchedPredicates;
          commitInfo.operationParameters = operationParameters;
          commitInfo.readVersion = 0;
          commitInfo.isBlindAppend = false;
          OperationMetrics operationMetrics = new OperationMetrics();
          operationMetrics.numTargetFilesAdded = Long.valueOf(commitMetadata.getWriteStats().size());
          operationMetrics.numOutputBytes = String.valueOf(commitMetadata.fetchTotalBytesWritten());
          operationMetrics.numOutputRows = String.valueOf(commitMetadata.fetchTotalRecordsWritten());
          operationMetrics.numTargetRowsCopied = commitMetadata.fetchTotalRecordsWritten() - commitMetadata.fetchTotalInsertRecordsWritten()
              - commitMetadata.fetchTotalUpdateRecordsWritten();
          operationMetrics.numSourceRows = commitMetadata.fetchTotalInsertRecordsWritten() + commitMetadata.fetchTotalUpdateRecordsWritten();
          operationMetrics.numTargetRowsDeleted = Long.valueOf(0);
          operationMetrics.executionTimeMs = commitMetadata.getTotalCreateTime();
          operationMetrics.scanTimeMs = commitMetadata.getTotalScanTime();
          operationMetrics.rewriteTimeMs = commitMetadata.getTotalUpsertTime();

          operationMetrics.numTargetRowsInserted = commitMetadata.getWriteStats().stream().mapToLong(entry -> entry.getNumInserts()).sum();
          operationMetrics.numTargetRowsUpdated = commitMetadata.getWriteStats().stream().mapToLong(entry -> entry.getNumUpdateWrites()).sum();
          operationMetrics.numTargetFilesRemoved = commitMetadata.getWriteStats().stream().filter(entry -> entry.getPrevFilePathOverwritten() != null).count();

          commitInfo.operationMetrics = operationMetrics;

          List<AddRecord> addList = new ArrayList<>();
          commitMetadata.getPartitionToWriteStats().values().forEach(entry -> {
                entry.forEach(writeStat -> {
                  Add add = new Add();
                  add.dataChange = true;
                  add.path = writeStat.getPath();
                  Map<String, String> partitionValueMap = new HashMap<>();
                  partitionValueMap.put(metaClient.getTableConfig().getPartitionFieldProp(), writeStat.getPartitionPath().substring(writeStat.getPartitionPath().indexOf('=') + 1));
                  add.partitionValues = partitionValueMap;
                  add.size = writeStat.getFileSizeInBytes();
                  add.modificationTime = commitInfo.timestamp;
                  AddRecord addRecord = new AddRecord();
                  addRecord.add = add;
                  addList.add(addRecord);
                });
              }
          );

          List<RemoveRecord> removeList = new ArrayList<>();
          commitMetadata.getWriteStats().stream().filter(entry -> entry.getPrevFilePathOverwritten() != null).forEach(entry -> {
            Add add = new Add();
            add.dataChange = true;
            add.path = entry.getPrevFilePathOverwritten();
            add.deletionTimestamp = commitInfo.timestamp;
            add.dataChange = true;
            add.extendedFileMetadata = true;
            Map<String, String> partitionValueMap = new HashMap<>();
            partitionValueMap.put(metaClient.getTableConfig().getPartitionFieldProp(), entry.getPartitionPath().substring(entry.getPartitionPath().indexOf('=') + 1));
            add.partitionValues = partitionValueMap;
            RemoveRecord addRecord = new RemoveRecord();
            addRecord.remove = add;
            removeList.add(addRecord);
          });


          mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

          CommitInfoWrapper commitInfoWrapper = new CommitInfoWrapper();
          commitInfoWrapper.commitInfo = commitInfo;
          String commitInfoStr = mapper.writeValueAsString(commitInfoWrapper);

          // String addListStr = mapper.writeValueAsString(addList);
          String outStr = commitInfoStr + "\n";
          for (RemoveRecord removeRecord : removeList) {
            outStr += mapper.writeValueAsString(removeRecord) + "\n";
          }
          for (AddRecord addRecord : addList) {
            outStr += mapper.writeValueAsString(addRecord) + "\n";
          }

          LOG.warn("Final output string " + outStr);

          LOG.warn(" File to be written " + new Path(deltaLogPath.toString() + "/" + nextFileName).toString());
          FSDataOutputStream stream = null;
          stream = fs.create(new Path(deltaLogPath.toString() + "/" + nextFileName));
          stream.write(outStr.getBytes(StandardCharsets.UTF_8));
          stream.flush();
          // stream.sync();
          stream.close();

        }
      }
    } catch (Exception e) {
      LOG.error("Exception thrown " + e.getMessage());
      throw new HoodieException(" exceptin thrown ", e);
    }
  }

  Schema getHudiSchema(Schema schema) {
    List<Schema.Field> schemaFields = new ArrayList<>();
    Schema.Field schemafield = new Schema.Field(HoodieRecord.COMMIT_TIME_METADATA_FIELD, Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE);
    schemaFields.add(schemafield);
    schemaFields.add(new Schema.Field(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE));
    schemaFields.add(new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE));
    schemaFields.add(new Schema.Field(HoodieRecord.PARTITION_PATH_METADATA_FIELD, Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE));
    schemaFields.add(new Schema.Field(HoodieRecord.FILENAME_METADATA_FIELD, Schema.create(Schema.Type.STRING), "doc", JsonProperties.NULL_VALUE));
    schema.getFields().forEach(entry -> {
          schemaFields.add(new Schema.Field(entry.name(), entry.schema(), entry.doc(), entry.defaultVal()));
        }
    );
    return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), schemaFields);
  }

  String getDeltaSchema(String avroSchemaStr) throws JsonProcessingException {
    Schema schema = new Schema.Parser().parse(avroSchemaStr);
    List<DeltaSchemaField> fields = new ArrayList<>();

    schema.getFields().forEach(field -> {
          DeltaSchemaField deltaSchemaField = new DeltaSchemaField();
          deltaSchemaField.name = field.name();
          //List<Schema.Field> innerFields = field.schema().getFields();
          deltaSchemaField.type = field.schema().getType() == Schema.Type.UNION
              ? ((field.schema().getTypes().get(0).getType() == Schema.Type.NULL) ? field.schema().getTypes().get(1).getType().getName()
              : field.schema().getTypes().get(0).getType().getName())
              : field.schema().getType().getName();
          deltaSchemaField.nullable = true;
          fields.add(deltaSchemaField);
        }
    );

    ObjectMapper mapper = new ObjectMapper();
    DeltaSchema deltaSchema = new DeltaSchema();
    deltaSchema.fields = fields.toArray(new DeltaSchemaField[0]);
    System.out.println(" schema str " + mapper.writeValueAsString(deltaSchema));
    return mapper.writeValueAsString(deltaSchema);
  }
}
