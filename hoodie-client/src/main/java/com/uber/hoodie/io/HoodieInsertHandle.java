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

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieInsertException;
import com.uber.hoodie.io.storage.HoodieStorageWriter;
import com.uber.hoodie.io.storage.HoodieStorageWriterFactory;
import com.uber.hoodie.table.HoodieTable;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class HoodieInsertHandle<T extends HoodieRecordPayload> extends HoodieIOHandle<T> {
    private static Logger logger = LogManager.getLogger(HoodieInsertHandle.class);

    private final WriteStatus status;
    private final HoodieStorageWriter<IndexedRecord> storageWriter;
    private final Path path;
    private long recordsWritten = 0;
    private long recordsDeleted = 0;

    public HoodieInsertHandle(HoodieWriteConfig config, String commitTime,
                              HoodieTable<T> hoodieTable, String partitionPath) {
        super(config, commitTime, hoodieTable);
        this.status = new WriteStatus();
        status.setFileId(UUID.randomUUID().toString());
        status.setPartitionPath(partitionPath);

        this.path = makeNewPath(partitionPath, TaskContext.getPartitionId(), status.getFileId());
        try {
            HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs,
                                       commitTime,
                                       new Path(config.getBasePath()),
                                       new Path(config.getBasePath(), partitionPath));
            partitionMetadata.trySave(TaskContext.getPartitionId());
            this.storageWriter =
                HoodieStorageWriterFactory.getStorageWriter(commitTime, path, hoodieTable, config, schema);
        } catch (IOException e) {
            throw new HoodieInsertException(
                "Failed to initialize HoodieStorageWriter for path " + path, e);
        }
        logger.info("New InsertHandle for partition :" + partitionPath);
    }

    /**
     * Determines whether we can accept the incoming records, into the current file, depending on
     *
     * - Whether it belongs to the same partitionPath as existing records
     * - Whether the current file written bytes lt max file size
     *
     * @return
     */
    public boolean canWrite(HoodieRecord record) {
        return storageWriter.canWrite() && record.getPartitionPath()
            .equals(status.getPartitionPath());
    }

    /**
     * Perform the actual writing of the given record into the backing file.
     *
     * @param record
     */
    public void write(HoodieRecord record) {
        try {
            Optional<IndexedRecord> avroRecord = record.getData().getInsertValue(schema);

            if(avroRecord.isPresent()) {
                storageWriter.writeAvroWithMetadata(avroRecord.get(), record);
                // update the new location of record, so we know where to find it next
                record.setNewLocation(new HoodieRecordLocation(commitTime, status.getFileId()));
                recordsWritten++;
            } else {
                recordsDeleted++;
            }

            record.deflate();
            status.markSuccess(record);
        } catch (Throwable t) {
            // Not throwing exception from here, since we don't want to fail the entire job
            // for a single record
            status.markFailure(record, t);
            logger.error("Error writing record " + record, t);
        }
    }

    /**
     * Performs actions to durably, persist the current changes and returns a WriteStatus object
     *
     * @return
     */
    public WriteStatus close() {
        logger.info(
            "Closing the file " + status.getFileId() + " as we are done with all the records "
                + recordsWritten);
        try {
            storageWriter.close();

            HoodieWriteStat stat = new HoodieWriteStat();
            stat.setNumWrites(recordsWritten);
            stat.setNumDeletes(recordsDeleted);
            stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
            stat.setFileId(status.getFileId());
            stat.setFullPath(path.toString());
            stat.setTotalWriteBytes(FSUtils.getFileSize(fs, path));
            stat.setTotalWriteErrors(status.getFailedRecords().size());
            status.setStat(stat);

            return status;
        } catch (IOException e) {
            throw new HoodieInsertException("Failed to close the Insert Handle for path " + path,
                e);
        }
    }
}
