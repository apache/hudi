/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.clearspring.analytics.util.Lists;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieDeltaWriteStat;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.log.HoodieLogAppendConfig;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import com.uber.hoodie.common.table.log.avro.RollingAvroLogAppender;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieAppendException;
import com.uber.hoodie.exception.HoodieUpsertException;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class HoodieAppendHandle<T extends HoodieRecordPayload> extends HoodieIOHandle<T> {
    private static Logger logger = LogManager.getLogger(HoodieUpdateHandle.class);
    private static AtomicLong recordIndex = new AtomicLong(1);

    private final WriteStatus writeStatus;
    private final String fileId;
    private String partitionPath;
    private RollingAvroLogAppender logAppender;
    private List<HoodieRecord<T>> records;
    private long recordsWritten = 0;
    private long recordsDeleted = 0;
    private HoodieLogFile currentLogFile;

    public HoodieAppendHandle(HoodieWriteConfig config, String commitTime,
        HoodieTable<T> hoodieTable, String fileId, Iterator<HoodieRecord<T>> recordItr) {
        super(config, commitTime, hoodieTable);
        WriteStatus writeStatus = new WriteStatus();
        writeStatus.setStat(new HoodieDeltaWriteStat());
        this.writeStatus = writeStatus;
        this.fileId = fileId;
        init(recordItr);
    }

    private void init(Iterator<HoodieRecord<T>> recordItr) {
        List<HoodieRecord<T>> records = Lists.newArrayList();
        recordItr.forEachRemaining(record -> {
            records.add(record);
            // extract some information from the first record
            if (partitionPath == null) {
                partitionPath = record.getPartitionPath();
                String latestValidFilePath =
                    fileSystemView.getLatestDataFilesForFileId(record.getPartitionPath(), fileId)
                        .findFirst().get().getFileName();
                String baseCommitTime = FSUtils.getCommitTime(latestValidFilePath);
                writeStatus.getStat().setPrevCommit(baseCommitTime);
                writeStatus.setFileId(fileId);
                writeStatus.setPartitionPath(record.getPartitionPath());
                writeStatus.getStat().setFileId(fileId);

                try {
                    HoodieLogAppendConfig logConfig = HoodieLogAppendConfig.newBuilder()
                        .onPartitionPath(
                            new Path(hoodieTable.getMetaClient().getBasePath(), partitionPath))
                        .withFileId(fileId).withBaseCommitTime(baseCommitTime).withSchema(schema)
                        .withFs(fs).withLogFileExtension(HoodieLogFile.DELTA_EXTENSION).build();
                    this.logAppender = new RollingAvroLogAppender(logConfig);
                    this.currentLogFile = logAppender.getConfig().getLogFile();
                    ((HoodieDeltaWriteStat) writeStatus.getStat())
                        .setLogVersion(currentLogFile.getLogVersion());
                    ((HoodieDeltaWriteStat) writeStatus.getStat())
                        .setLogOffset(logAppender.getCurrentSize());
                } catch (Exception e) {
                    logger.error("Error in update task at commit " + commitTime, e);
                    writeStatus.setGlobalError(e);
                    throw new HoodieUpsertException(
                        "Failed to initialize HoodieUpdateHandle for FileId: " + fileId
                            + " on commit " + commitTime + " on HDFS path " + hoodieTable
                            .getMetaClient().getBasePath() + partitionPath, e);
                }
                writeStatus.getStat().setFullPath(currentLogFile.getPath().toString());
            }
            // update the new location of the record, so we know where to find it next
            record.setNewLocation(new HoodieRecordLocation(commitTime, fileId));
        });
        this.records = records;
    }

    private Optional<IndexedRecord> getIndexedRecord(HoodieRecord<T> hoodieRecord) {
        try {
            Optional<IndexedRecord> avroRecord = hoodieRecord.getData().getInsertValue(schema);

            if(avroRecord.isPresent()) {
                String seqId = HoodieRecord.generateSequenceId(commitTime, TaskContext.getPartitionId(),
                        recordIndex.getAndIncrement());
                HoodieAvroUtils
                        .addHoodieKeyToRecord((GenericRecord) avroRecord.get(), hoodieRecord.getRecordKey(),
                                hoodieRecord.getPartitionPath(), fileId);
                HoodieAvroUtils
                        .addCommitMetadataToRecord((GenericRecord) avroRecord.get(), commitTime, seqId);
                recordsWritten++;
            } else {
                recordsDeleted++;
            }

            hoodieRecord.deflate();
            writeStatus.markSuccess(hoodieRecord);
            return avroRecord;
        } catch (Exception e) {
            logger.error("Error writing record  " + hoodieRecord, e);
            writeStatus.markFailure(hoodieRecord, e);
        }
        return Optional.empty();
    }

    public void doAppend() {
        Iterator<IndexedRecord> recordItr =
            records.stream().map(this::getIndexedRecord).filter(Optional::isPresent)
                .map(Optional::get).iterator();
        try {
            logAppender.append(recordItr);
        } catch (Exception e) {
            throw new HoodieAppendException(
                "Failed while appeding records to " + currentLogFile.getPath(), e);
        }
    }

    public void close() {
        try {
            if (logAppender != null) {
                logAppender.close();
            }
            writeStatus.getStat().setNumWrites(recordsWritten);
            writeStatus.getStat().setNumDeletes(recordsDeleted);
            writeStatus.getStat().setTotalWriteErrors(writeStatus.getFailedRecords().size());
        } catch (IOException e) {
            throw new HoodieUpsertException("Failed to close UpdateHandle", e);
        }
    }

    public WriteStatus getWriteStatus() {
        return writeStatus;
    }


}
