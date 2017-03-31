/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.log.avro;

import com.uber.hoodie.common.table.log.HoodieLogAppendConfig;
import com.uber.hoodie.common.table.log.HoodieLogAppender;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * AvroLogAppender appends a bunch of IndexedRecord to a Avro data file.
 * If auto-flush is set, every call to append writes out a block.
 * A avro block corresponds to records appended in a single commit.
 *
 * @see org.apache.avro.file.DataFileReader
 */
public class AvroLogAppender implements HoodieLogAppender<IndexedRecord> {
    private final static Logger log = LogManager.getLogger(AvroLogAppender.class);
    private final HoodieLogAppendConfig config;
    private FSDataOutputStream output;
    private DataFileWriter<IndexedRecord> writer;
    private boolean autoFlush;

    public AvroLogAppender(HoodieLogAppendConfig config) throws IOException, InterruptedException {
        FileSystem fs = config.getFs();
        this.config = config;
        this.autoFlush = config.isAutoFlush();
        GenericDatumWriter<IndexedRecord> datumWriter =
            new GenericDatumWriter<>(config.getSchema());
        this.writer = new DataFileWriter<>(datumWriter);
        Path path = config.getLogFile().getPath();

        if (fs.exists(path)) {
            //TODO - check for log corruption and roll over if needed
            log.info(config.getLogFile() + " exists. Appending to existing file");
            // this log path exists, we will append to it
            // fs = FileSystem.get(fs.getConf());
            try {
                this.output = fs.append(path, config.getBufferSize());
            } catch (RemoteException e) {
                // this happens when either another task executor writing to this file died or data node is going down
                if (e.getClassName().equals(AlreadyBeingCreatedException.class.getName())
                    && fs instanceof DistributedFileSystem) {
                    log.warn("Trying to recover log on path " + path);
                    if (FSUtils.recoverDFSFileLease((DistributedFileSystem) fs, path)) {
                        log.warn("Recovered lease on path " + path);
                        // try again
                        this.output = fs.append(path, config.getBufferSize());
                    } else {
                        log.warn("Failed to recover lease on path " + path);
                        throw new HoodieException(e);
                    }
                }
            }

            this.writer
                .appendTo(new FsInput(path, fs.getConf()), output);
            // we always want to flush to disk everytime a avro block is written
            this.writer.setFlushOnEveryBlock(true);
        } else {
            log.info(config.getLogFile() + " does not exist. Create a new file");
            this.output = fs.create(path, false, config.getBufferSize(), config.getReplication(),
                config.getBlockSize(), null);
            this.writer.create(config.getSchema(), output);
            this.writer.setFlushOnEveryBlock(true);
            // We need to close the writer to be able to tell the name node that we created this file
            // this.writer.close();
        }
    }

    public void append(Iterator<IndexedRecord> records) throws IOException {
        records.forEachRemaining(r -> {
            try {
                writer.append(r);
            } catch (IOException e) {
                throw new HoodieIOException(
                    "Could not append record " + r + " to " + config.getLogFile());
            }
        });
        if (autoFlush) {
            sync();
        }
    }

    public void sync() throws IOException {
        if (output == null || writer == null)
            return; // Presume closed
        writer.flush();
        output.flush();
        output.hflush();
    }

    public void close() throws IOException {
        sync();
        writer.close();
        writer = null;
        output.close();
        output = null;
    }

    public long getCurrentSize() throws IOException {
        if (writer == null) {
            throw new IllegalStateException(
                "LogWriter " + config.getLogFile() + " has been closed. Cannot getCurrentSize");
        }
        // writer.sync() returns only the offset for this block and not the global offset
        return output.getPos();
    }
}
