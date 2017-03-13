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

package com.uber.hoodie.func;

import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;

import com.uber.hoodie.io.HoodieIOHandle;
import com.uber.hoodie.io.HoodieInsertHandle;
import org.apache.spark.TaskContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath,
 * into new files.
 */
public class LazyInsertIterable<T extends HoodieRecordPayload> extends LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {

    private final HoodieWriteConfig hoodieConfig;
    private final String commitTime;
    private final HoodieTableMetaClient metaClient;
    private Set<String> partitionsCleaned;
    private HoodieInsertHandle handle;

    public LazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
        String commitTime, HoodieTableMetaClient metaClient) {
        super(sortedRecordItr);
        this.partitionsCleaned = new HashSet<>();
        this.hoodieConfig = config;
        this.commitTime = commitTime;
        this.metaClient = metaClient;
    }

    @Override protected void start() {
    }


    @Override protected List<WriteStatus> computeNext()  {
        List<WriteStatus> statuses = new ArrayList<>();

        while (inputItr.hasNext()) {
            HoodieRecord record = inputItr.next();

            // clean up any partial failures
            if (!partitionsCleaned.contains(record.getPartitionPath())) {
                // This insert task could fail multiple times, but Spark will faithfully retry with
                // the same data again. Thus, before we open any files under a given partition, we
                // first delete any files in the same partitionPath written by same Spark partition
                HoodieIOHandle.cleanupTmpFilesFromCurrentCommit(hoodieConfig,
                                                                commitTime,
                                                                record.getPartitionPath(),
                                                                TaskContext.getPartitionId());
                partitionsCleaned.add(record.getPartitionPath());
            }

            // lazily initialize the handle, for the first time
            if (handle == null) {
                handle =
                    new HoodieInsertHandle(hoodieConfig, commitTime, metaClient,
                        record.getPartitionPath());
            }

            if (handle.canWrite(record)) {
                // write the record, if the handle has capacity
                handle.write(record);
            } else {
                // handle is full.
                statuses.add(handle.close());
                // Need to handle the rejected record & open new handle
                handle =
                    new HoodieInsertHandle(hoodieConfig, commitTime, metaClient,
                        record.getPartitionPath());
                handle.write(record); // we should be able to write 1 record.
                break;
            }
        }

        // If we exited out, because we ran out of records, just close the pending handle.
        if (!inputItr.hasNext()) {
            if (handle != null) {
                statuses.add(handle.close());
            }
        }

        assert statuses.size() > 0; // should never return empty statuses
        return statuses;
    }

    @Override protected void end() {

    }
}
