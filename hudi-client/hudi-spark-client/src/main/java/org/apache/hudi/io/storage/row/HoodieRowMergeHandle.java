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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.client.HoodieRowWriteStatus;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.client.model.HoodieRowRecord;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieBaseTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import scala.Tuple2;

import static org.apache.hudi.client.utils.ScalaConversions.toList;
import static org.apache.hudi.client.utils.ScalaConversions.toSeq;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_POS;
import static org.apache.hudi.common.model.HoodieRecord.COMMIT_TIME_POS;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_POS;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_POS;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_POS;

public class HoodieRowMergeHandle implements Serializable, AutoCloseable {

  public static final Column[] HOODIE_META_COLS = HOODIE_META_COLUMNS.stream().map(Column::new).toArray(Column[]::new);
  private final HoodieBaseTable table;
  private final StructType schema;
  private final int partitionId;
  private final long taskId;
  private final long taskEpochId;
  private final String instantTime;
  private final HoodieWriteConfig writeConfig;

  private final Configuration hadoopConf;
  private final String writeToken;
  private final String basePath;
  private int numFilesWritten;
  private int sequence;
  /* <{partitionPath, existingFileName}, <key, incoming record>> */
  private Map<Tuple2<String, String>, Map<String, HoodieRowRecord>> existingFileAndUpdateRecords;
  private Tuple2<String, List<HoodieRowRecord>> newFileAndInsertRecords;
  private final HoodieRowWriteStatus writeStatus;
  private final HoodieTimer timer;

  public HoodieRowMergeHandle(HoodieBaseTable table, StructType schema,
      int partitionId, long taskId, long taskEpochId,
      String instantTime, HoodieWriteConfig writeConfig) {
    this.table = table;
    this.schema = schema;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;

    this.hadoopConf = table.getHadoopConf();
    this.writeToken = FSUtils.makeWriteToken(partitionId, (int) taskId, taskEpochId);
    this.basePath = writeConfig.getBasePath();
    this.existingFileAndUpdateRecords = new HashMap<>();
    this.writeStatus = new HoodieRowWriteStatus(true, writeConfig.getWriteStatusFailureFraction());
    // TODO(rxu) set more properties
    this.writeStatus.setStat(new HoodieWriteStat());
    this.timer = new HoodieTimer().startTimer();
  }

  public void handle(HoodieRowRecord rr) {
    String partitionPath = rr.getPartitionPath();
    if (isUpdate(rr)) {
      String existingFileName = rr.getFileName();
      if (!existingFileAndUpdateRecords.containsKey(Tuple2.apply(partitionPath, existingFileName))) {
        existingFileAndUpdateRecords.put(Tuple2.apply(partitionPath, existingFileName), new HashMap<>());
      }
      String key = rr.getRecordKey();
      String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, sequence++);
      String fileGroupId = fileGroupId(existingFileName);
      String newFileName = FSUtils.makeDataFileName(instantTime, writeToken,
          fileGroupId + "-" + numFilesWritten++, ".parquet");
      HoodieRowRecord recordWithMetaCols = updateMetaCols(rr, instantTime, seqId, key, partitionPath, newFileName);
      existingFileAndUpdateRecords.get(Tuple2.apply(partitionPath, existingFileName)).put(key, recordWithMetaCols);
    } else {
      if (newFileAndInsertRecords == null) {
        String newFileName = getFileName(UUID.randomUUID().toString());
        newFileAndInsertRecords = Tuple2.apply(newFileName, new ArrayList<>());
      }
      String key = rr.getRecordKey();
      String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, sequence++);
      HoodieRowRecord recordWithMetaCols = updateMetaCols(rr, instantTime, seqId, key, partitionPath,
          newFileAndInsertRecords._1);
      newFileAndInsertRecords._2.add(recordWithMetaCols);
    }
  }

  public HoodieRowWriteStatus flush() throws IOException {
    try {
      flushRecordsToFiles();
    } catch (Throwable t) {
      writeStatus.setGlobalError(t.toString());
      throw t;
    }
    HoodieWriteStat stat = writeStatus.getStat();
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    return writeStatus;
  }

  private void flushRecordsToFiles() throws IOException {
    // write new files for inserts
    if (newFileAndInsertRecords != null) {
      String newFileName = newFileAndInsertRecords._1;
      String partitionPath = newFileAndInsertRecords._2.get(0).getPartitionPath();
      try (HoodieInternalRowFileWriter writer = newWriter(partitionPath, newFileName)) {
        for (HoodieRowRecord rr : newFileAndInsertRecords._2) {
          writeRow(writer, rr, writeStatus);
        }
      }
      writeStatus.getStat().setNumInserts(newFileAndInsertRecords._2.size());
    } else {
      writeStatus.getStat().setNumInserts(0);
    }
    // rewrite files for updates
    for (Tuple2<String, String> partitionAndFileName : existingFileAndUpdateRecords.keySet()) {
      String partitionPath = partitionAndFileName._1;
      String existingFileName = partitionAndFileName._2;
      try (ParquetReader<InternalRow> r = newReader(partitionPath, existingFileName)) {
        List<HoodieRowRecord> mergedRecords = new ArrayList<>();
        Map<String, InternalRow> existingRecords = readRows(r);
        for (Map.Entry<String, HoodieRowRecord> e : existingFileAndUpdateRecords.get(partitionAndFileName).entrySet()) {
          String key = e.getKey();
          HoodieRowRecord incomingRecord = e.getValue();
          HoodieRowRecord existingRecord = HoodieRowRecord.fromHoodieInternalRow(existingRecords.get(key), schema);
          mergedRecords.add(merge(incomingRecord, existingRecord, schema, writeConfig));
          existingRecords.remove(key);
        }

        // records from the same existing file are written to the same new file
        String newFileName = mergedRecords.get(0).getFileName();
        try (HoodieInternalRowFileWriter w = newWriter(partitionPath, newFileName)) {
          for (HoodieRowRecord rr : mergedRecords) {
            writeRow(w, rr, writeStatus);
          }
          long currentUpdates = writeStatus.getStat().getNumUpdateWrites();
          writeStatus.getStat().setNumUpdateWrites(currentUpdates + mergedRecords.size());
          // rewrite non-updating records with new meta info
          for (InternalRow ir : existingRecords.values()) {
            HoodieRowRecord rr = HoodieRowRecord.fromHoodieInternalRow(ir, schema);
            writeRow(w, updateMetaCols(rr, instantTime, HoodieRecord
                .generateSequenceId(instantTime, partitionId, sequence++), newFileName), writeStatus);
          }
        }
      }
    }

    // after writing data, write a partition metadata file
    writePartitionMetadata();
  }

  private void writePartitionMetadata() {
    String partitionPath;
    if (newFileAndInsertRecords != null) {
      partitionPath = newFileAndInsertRecords._2.get(0).getPartitionPath();
    } else if (!existingFileAndUpdateRecords.isEmpty()) {
      partitionPath = existingFileAndUpdateRecords.keySet().iterator().next()._1;
    } else {
      return;
    }

    HoodiePartitionMetadata partitionMetadata =
        new HoodiePartitionMetadata(table.getMetaClient().getFs(), instantTime,
            new Path(basePath), FSUtils.getPartitionPath(basePath, partitionPath));
    partitionMetadata.trySave(partitionId);
  }

  public void close() throws IOException {
  }

  private String getFileName(String fileId) {
    return FSUtils.makeDataFileName(instantTime, writeToken,
        fileId + "-" + numFilesWritten++, ".parquet");
  }

  private static HoodieRowRecord merge(HoodieRowRecord incoming, HoodieRowRecord existing, StructType schema, HoodieWriteConfig writeConfig) {
    // TODO(rxu) allow custom merge behavior
    return incoming;
  }

  private static Map<String, InternalRow> readRows(ParquetReader<InternalRow> reader) throws IOException {
    Map<String, InternalRow> keyToExistingRows = new HashMap<>();
    while (true) {
      InternalRow r = reader.read();
      if (r == null) {
        break;
      }
      String key = r.getUTF8String(RECORD_KEY_POS).toString();
      keyToExistingRows.put(key, r);
    }
    return keyToExistingRows;
  }

  private ParquetReader<InternalRow> newReader(String partitionPath, String fileName) throws IOException {
    Path file = new Path(new Path(basePath, partitionPath), fileName);
    Configuration conf = new Configuration(hadoopConf);
    conf.set("org.apache.spark.sql.parquet.row.requested_schema", schema.json());
    conf.set("spark.sql.parquet.binaryAsString", "true");
    conf.set("spark.sql.parquet.int96AsTimestamp", "true");
    ParquetReadSupport readSupport = new ParquetReadSupport();
    try (ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      MessageType schema = parquetFileReader.getFooter().getFileMetaData().getSchema();
      InitContext ctx = new InitContext(conf, null, schema);
      ReadSupport.ReadContext readCtx = readSupport.init(ctx);
      readSupport.prepareForRead(conf, null, schema, readCtx);
    }
    return new ParquetReader(conf, file, readSupport);
  }

  private HoodieInternalRowFileWriter newWriter(String partitionPath, String fileName) throws IOException {
    Path file = new Path(new Path(basePath, partitionPath), fileName);
    return HoodieInternalRowFileWriterFactory.getInternalRowFileWriter(file, table, writeConfig, schema);
  }

  private static void writeRow(HoodieInternalRowFileWriter writer, HoodieRowRecord rr, HoodieRowWriteStatus writeStatus) {
    String k = rr.getRecordKey();
    try {
      writer.writeRow(k, rr.toInternalRow());
      writeStatus.markSuccess(k);
    } catch (Throwable t) {
      writeStatus.markFailure(k, t);
    }
  }

  private static boolean isUpdate(HoodieRowRecord record) {
    return record.getFileName() != null;
  }

  private static String fileGroupId(String fileName) {
    String fileGroupIdAndNumWrites = fileName.substring(0, fileName.indexOf('_'));
    return fileGroupIdAndNumWrites.substring(0, fileGroupIdAndNumWrites.lastIndexOf('-'));
  }

  private static HoodieInternalRow getHoodieInternalRow(InternalRow row, StructType schema) {
    List<Object> allColsValues = toList(row.toSeq(schema));
    String[] metaColsValues = allColsValues.subList(0, HOODIE_META_COLS.length).stream()
        .map(v -> v == null ? null : v.toString()).toArray(String[]::new);
    List<Object> lessMetaColsValues = allColsValues.subList(HOODIE_META_COLS.length, allColsValues.size());
    InternalRow originalInternalRow = GenericInternalRow.fromSeq(toSeq(lessMetaColsValues));
    return new HoodieInternalRow(
        metaColsValues[COMMIT_TIME_POS],
        metaColsValues[COMMIT_SEQNO_POS],
        metaColsValues[RECORD_KEY_POS],
        metaColsValues[PARTITION_PATH_POS],
        metaColsValues[FILENAME_POS],
        originalInternalRow
    );
  }

  private static HoodieRowRecord updateMetaCols(HoodieRowRecord rr, String commitTime, String seqId, String fileName) {
    rr.setCommitTime(commitTime);
    rr.setCommitSeqNumber(seqId);
    rr.setFileName(fileName);
    return rr;
  }

  private static HoodieRowRecord updateMetaCols(HoodieRowRecord rr, String commitTime, String seqId, String key, String partitionPath, String fileName) {
    rr.setCommitTime(commitTime);
    rr.setCommitSeqNumber(seqId);
    rr.setRecordKey(key);
    rr.setPartitionPath(partitionPath);
    rr.setFileName(fileName);
    return rr;
  }
}
