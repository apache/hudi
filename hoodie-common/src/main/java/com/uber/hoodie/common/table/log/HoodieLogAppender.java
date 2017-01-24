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

import com.uber.hoodie.common.table.log.avro.AvroLogAppender;
import com.uber.hoodie.common.table.log.avro.RollingAvroLogAppender;

import java.io.IOException;
import java.util.List;

/**
 * Interface for implementations supporting appending data to a log file
 *
 * @param <R>
 * @see AvroLogAppender
 * @see RollingAvroLogAppender
 */
public interface HoodieLogAppender<R> {
    /**
     * Append a stream of records in a batch (this will be written as a block/unit to the underlying log)
     *
     * @param records
     * @throws IOException
     */
    void append(List<R> records) throws IOException, InterruptedException;

    /**
     * Syncs the log manually if auto-flush is not set in HoodieLogAppendConfig. If auto-flush is set
     * Then the LogAppender will automatically flush after the append call.
     *
     * @throws IOException
     */
    void sync() throws IOException;

    /**
     * Close the appended and release any resources holding on to
     *
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Gets the current offset in the log. This is usually used to mark the start of the block in
     * meta-data and passed to the HoodieLogReader
     *
     * @return
     * @throws IOException
     */
    long getCurrentSize() throws IOException;
}
