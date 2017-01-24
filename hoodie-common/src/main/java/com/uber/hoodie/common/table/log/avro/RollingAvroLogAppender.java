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

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.log.HoodieLogAppendConfig;
import com.uber.hoodie.common.table.log.HoodieLogAppender;
import com.uber.hoodie.common.table.log.HoodieLogFile;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of {@link HoodieLogAppender} to roll over the log file when the sizeThreshold is reached.
 */
public class RollingAvroLogAppender implements HoodieLogAppender<IndexedRecord> {
    private static final Log LOG = LogFactory.getLog(RollingAvroLogAppender.class);
    private AvroLogAppender logWriter;
    private HoodieLogAppendConfig config;

    public RollingAvroLogAppender(HoodieLogAppendConfig config)
        throws IOException, InterruptedException {
        // initialize
        this.logWriter = new AvroLogAppender(config);
        this.config = config;
        rollOverIfNeeded();
    }

    private void rollOverIfNeeded() throws IOException, InterruptedException {
        HoodieLogFile logFile = config.getLogFile();
        boolean shouldRollOver = logFile.shouldRollOver(this, config);
        if (shouldRollOver) {
            if (logWriter != null) {
                // Close the old writer and open a new one
                logWriter.close();
            }
            // Current logWriter is not initialized, set the current file name
            HoodieLogFile nextRollLogPath = logFile.rollOver(config.getFs());
            LOG.info("Rolling over log from " + logFile + " to " + nextRollLogPath);
            this.config = config.withLogFile(nextRollLogPath);
            this.logWriter = new AvroLogAppender(this.config);
        }
    }

    public long getCurrentSize() throws IOException {
        Preconditions.checkArgument(logWriter != null);
        return logWriter.getCurrentSize();
    }

    public void append(List<IndexedRecord> records) throws IOException, InterruptedException {
        LOG.info("Appending " + records.size() + " records to " + config.getLogFile());
        rollOverIfNeeded();
        Preconditions.checkArgument(logWriter != null);
        logWriter.append(records);
    }

    public void sync() throws IOException {
        Preconditions.checkArgument(logWriter != null);
        logWriter.sync();
    }

    public void close() throws IOException {
        Preconditions.checkArgument(logWriter != null);
        logWriter.close();
    }

    public HoodieLogAppendConfig getConfig() {
        return config;
    }
}
