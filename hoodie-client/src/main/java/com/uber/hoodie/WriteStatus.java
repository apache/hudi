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

package com.uber.hoodie;

import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieWriteStat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Status of a write operation.
 */
public class WriteStatus implements Serializable {

    private final HashMap<HoodieKey, Throwable> errors = new HashMap<>();

    private final List<HoodieRecord> writtenRecords = new ArrayList<>();

    private final List<HoodieRecord> failedRecords  = new ArrayList<>();

    private Throwable globalError = null;

    private String fileId = null;

    private String partitionPath = null;

    private HoodieWriteStat stat = null;

    private long totalRecords = 0;
    private long totalErrorRecords = 0;

    public void markSuccess(HoodieRecord record) {
        writtenRecords.add(record);
        totalRecords++;
    }

    public void markFailure(HoodieRecord record, Throwable t) {
        failedRecords.add(record);
        errors.put(record.getKey(), t);
        totalRecords++;
        totalErrorRecords++;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public boolean hasErrors() {
        return totalErrorRecords > 0;
    }

    public boolean isErrored(HoodieKey key) {
        return errors.containsKey(key);
    }

    public HashMap<HoodieKey, Throwable> getErrors() {
        return errors;
    }

    public boolean hasGlobalError() {
        return globalError != null;
    }

    public void setGlobalError(Throwable t) {
        this.globalError = t;
    }

    public Throwable getGlobalError() {
        return this.globalError;
    }

    public List<HoodieRecord> getWrittenRecords() {
        return writtenRecords;
    }

    public List<HoodieRecord> getFailedRecords() {
        return failedRecords;
    }

    public HoodieWriteStat getStat() {
        return stat;
    }

    public void setStat(HoodieWriteStat stat) {
        this.stat = stat;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    public void setPartitionPath(String partitionPath) {
        this.partitionPath = partitionPath;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WriteStatus {");
        sb.append("fileId=").append(fileId);
        sb.append(", globalError='").append(globalError).append('\'');
        sb.append(", hasErrors='").append(hasErrors()).append('\'');
        sb.append(", errorCount='").append(totalErrorRecords).append('\'');
        sb.append(", errorPct='").append((100.0 * totalErrorRecords) / totalRecords).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
