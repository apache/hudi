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

package com.uber.hoodie.table;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.HoodieAppendHandle;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of a more real-time read-optimized Hoodie Table where
 *
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or)
 *           Merge with the smallest existing file, to expand it
 *
 * UPDATES - Appends the changes to a rolling log file maintained per file Id.
 * Compaction merges the log file into the base file.
 *
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends HoodieCopyOnWriteTable<T> {
    private static Logger logger = LogManager.getLogger(HoodieMergeOnReadTable.class);

    public HoodieMergeOnReadTable(HoodieWriteConfig config,
        HoodieTableMetaClient metaClient) {
        super(config, metaClient);
    }

    @Override
    public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId,
        Iterator<HoodieRecord<T>> recordItr) throws IOException {
        logger.info("Merging updates for commit " + commitTime + " for file " + fileId);
        HoodieAppendHandle<T> appendHandle =
            new HoodieAppendHandle<>(config, commitTime, this, fileId, recordItr);
        appendHandle.doAppend();
        appendHandle.close();
        return Collections.singletonList(Collections.singletonList(appendHandle.getWriteStatus()))
            .iterator();
    }
}
