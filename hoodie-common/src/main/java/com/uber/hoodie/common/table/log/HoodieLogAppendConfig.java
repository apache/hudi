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

package com.uber.hoodie.common.table.log;

import com.uber.hoodie.common.util.FSUtils;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Configuration for a HoodieLog
 */
public class HoodieLogAppendConfig {
    private final static Logger log = LogManager.getLogger(HoodieLogAppendConfig.class);
    private static final long DEFAULT_SIZE_THRESHOLD = 32 * 1024 * 1024L;

    private final int bufferSize;
    private final short replication;
    private final long blockSize;
    private final HoodieLogFile logFile;
    private boolean isAutoFlush;
    private final Schema schema;
    private final FileSystem fs;
    private final long sizeThreshold;

    private HoodieLogAppendConfig(FileSystem fs, HoodieLogFile logFile, Schema schema, Integer bufferSize,
        Short replication, Long blockSize, boolean isAutoFlush, Long sizeThreshold) {
        this.fs = fs;
        this.logFile = logFile;
        this.schema = schema;
        this.bufferSize = bufferSize;
        this.replication = replication;
        this.blockSize = blockSize;
        this.isAutoFlush = isAutoFlush;
        this.sizeThreshold = sizeThreshold;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public short getReplication() {
        return replication;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public Schema getSchema() {
        return schema;
    }

    public FileSystem getFs() {
        return fs;
    }

    public HoodieLogFile getLogFile() {
        return logFile;
    }

    public long getSizeThreshold() {
        return sizeThreshold;
    }

    public boolean isAutoFlush() {
        return isAutoFlush;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public HoodieLogAppendConfig withLogFile(HoodieLogFile newFile) {
        return new HoodieLogAppendConfig(fs, newFile, schema, bufferSize, replication, blockSize,
            isAutoFlush, sizeThreshold);
    }

    public static class Builder {
        // Auto-flush. if set to true - then after every append, the avro block will be flushed
        private boolean isAutoFlush = true;
        // Buffer size in the Avro writer
        private Integer bufferSize;
        // Replication for the log file
        private Short replication;
        // Blocksize for the avro log file (useful if auto-flush is set to false)
        private Long blockSize;
        // Schema for the log file
        private Schema schema;
        // FileSystem
        private FileSystem fs;
        // Size threshold for the log file. Useful when used with a rolling log appender
        private Long sizeThreshold;
        // Log File extension. Could be .avro.delta or .avro.commits etc
        private String logFileExtension;
        // File ID
        private String fileId;
        // version number for this log file. If not specified, then the current version will be computed
        private Integer fileVersion;
        // Partition path for the log file
        private Path partitionPath;

        public Builder withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder withReplication(short replication) {
            this.replication = replication;
            return this;
        }

        public Builder withBlockSize(long blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withFs(FileSystem fs) {
            this.fs = fs;
            return this;
        }

        public Builder withAutoFlush(boolean autoFlush) {
            this.isAutoFlush = autoFlush;
            return this;
        }

        public Builder withSizeThreshold(long sizeThreshold) {
            this.sizeThreshold = sizeThreshold;
            return this;
        }

        public Builder withLogFileExtension(String logFileExtension) {
            this.logFileExtension = logFileExtension;
            return this;
        }

        public Builder withFileId(String fileId) {
            this.fileId = fileId;
            return this;
        }

        public Builder withFileVersion(int version) {
            this.fileVersion = version;
            return this;
        }

        public Builder onPartitionPath(Path path) {
            this.partitionPath = path;
            return this;
        }

        public HoodieLogAppendConfig build() throws IOException {
            log.info("Building HoodieLogAppendConfig");
            if (schema == null) {
                throw new IllegalArgumentException("Schema for log is not specified");
            }
            if (fs == null) {
                fs = FSUtils.getFs();
            }

            if (fileId == null) {
                throw new IllegalArgumentException("FileID is not specified");
            }
            if (logFileExtension == null) {
                throw new IllegalArgumentException("File extension is not specified");
            }
            if (partitionPath == null) {
                throw new IllegalArgumentException("Partition path is not specified");
            }
            if (fileVersion == null) {
                log.info("Computing the next log version for " + fileId + " in " + partitionPath);
                fileVersion =
                    FSUtils.getCurrentLogVersion(fs, partitionPath, fileId, logFileExtension);
                log.info(
                    "Computed the next log version for " + fileId + " in " + partitionPath + " as "
                        + fileVersion);
            }

            Path logPath = new Path(partitionPath,
                FSUtils.makeLogFileName(fileId, logFileExtension, fileVersion));
            log.info("LogConfig created on path " + logPath);
            HoodieLogFile logFile = new HoodieLogFile(logPath);

            if (bufferSize == null) {
                bufferSize = FSUtils.getDefaultBufferSize(fs);
            }
            if (replication == null) {
                replication = FSUtils.getDefaultReplication(fs, partitionPath);
            }
            if (blockSize == null) {
                blockSize = FSUtils.getDefaultBlockSize(fs, partitionPath);
            }
            if (sizeThreshold == null) {
                sizeThreshold = DEFAULT_SIZE_THRESHOLD;
            }

            return new HoodieLogAppendConfig(fs, logFile, schema, bufferSize, replication, blockSize,
                isAutoFlush, sizeThreshold);

        }


    }
}
