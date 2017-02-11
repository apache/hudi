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

import com.google.common.collect.Sets;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.view.HoodieTableFileSystemView;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Abstract implementation of a HoodieTable
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {
    protected final HoodieWriteConfig config;
    protected final HoodieTableMetaClient metaClient;

    protected HoodieTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
        this.config = config;
        this.metaClient = metaClient;
    }

    /**
     * Provides a partitioner to perform the upsert operation, based on the
     * workload profile
     *
     * @return
     */
    public abstract Partitioner getUpsertPartitioner(WorkloadProfile profile);


    /**
     * Provides a partitioner to perform the insert operation, based on the workload profile
     *
     * @return
     */
    public abstract Partitioner getInsertPartitioner(WorkloadProfile profile);


    /**
     * Return whether this HoodieTable implementation can benefit from workload
     * profiling
     *
     * @return
     */
    public abstract boolean isWorkloadProfileNeeded();

    public HoodieWriteConfig getConfig() {
        return config;
    }

    public HoodieTableMetaClient getMetaClient() {
        return metaClient;
    }

    public FileSystem getFs() {
        return metaClient.getFs();
    }

    /**
     * Get the view of the file system for this table
     *
     * @return
     */
    public TableFileSystemView getFileSystemView() {
        return new HoodieTableFileSystemView(metaClient, getCompletedCommitTimeline());
    }

    /**
     * Get the view of the file system for this table
     *
     * @return
     */
    public TableFileSystemView getCompactedFileSystemView() {
        return new HoodieTableFileSystemView(metaClient, getCompletedCompactionCommitTimeline());
    }

    /**
     * Get only the completed (no-inflights) commit timeline
     * @return
     */
    public HoodieTimeline getCompletedCommitTimeline() {
        return getCommitTimeline().filterCompletedInstants();
    }

    public HoodieActiveTimeline getActiveTimeline() {
        return metaClient.getActiveTimeline();
    }

    /**
     * Get the commit timeline visible for this table
     *
     * @return
     */
    public HoodieTimeline getCommitTimeline() {
        switch (metaClient.getTableType()) {
            case COPY_ON_WRITE:
                return getActiveTimeline().getCommitTimeline();
            case MERGE_ON_READ:
                // We need to include the parquet files written out in delta commits
                return getActiveTimeline().getTimelineOfActions(
                    Sets.newHashSet(HoodieActiveTimeline.COMPACTION_ACTION,
                        HoodieActiveTimeline.DELTA_COMMIT_ACTION));
            default:
                throw new HoodieException("Unsupported table type :"+ metaClient.getTableType());
        }
    }

    /**
     * Get only the completed (no-inflights) compaction commit timeline
     * @return
     */
    public HoodieTimeline getCompletedCompactionCommitTimeline() {
        return getCompactionCommitTimeline().filterCompletedInstants();
    }


    /**
     * Get the compacted commit timeline visible for this table
     *
     * @return
     */
    public HoodieTimeline getCompactionCommitTimeline() {
        switch (metaClient.getTableType()) {
            case COPY_ON_WRITE:
                return getActiveTimeline().getCommitTimeline();
            case MERGE_ON_READ:
                // We need to include the parquet files written out in delta commits in tagging
                return getActiveTimeline().getTimelineOfActions(
                    Sets.newHashSet(HoodieActiveTimeline.COMPACTION_ACTION));
            default:
                throw new HoodieException("Unsupported table type :"+ metaClient.getTableType());
        }
    }

    /**
     * Gets the commit action type
     * @return
     */
    public String getCommitActionType() {
        switch (metaClient.getTableType()) {
            case COPY_ON_WRITE:
                return HoodieActiveTimeline.COMMIT_ACTION;
            case MERGE_ON_READ:
                return HoodieActiveTimeline.DELTA_COMMIT_ACTION;
        }
        throw new HoodieCommitException(
            "Could not commit on unknown storage type " + metaClient.getTableType());
    }

    /**
     * Gets the action type for a compaction commit
     * @return
     */
    public String getCompactedCommitActionType() {
        switch (metaClient.getTableType()) {
            case COPY_ON_WRITE:
                return HoodieTimeline.COMMIT_ACTION;
            case MERGE_ON_READ:
                return HoodieTimeline.COMPACTION_ACTION;
        }
        throw new HoodieException("Unsupported table type :"+ metaClient.getTableType());
    }



    /**
     * Perform the ultimate IO for a given upserted (RDD) partition
     *
     * @param partition
     * @param recordIterator
     * @param partitioner
     */
    public abstract Iterator<List<WriteStatus>> handleUpsertPartition(String commitTime,
        Integer partition, Iterator<HoodieRecord<T>> recordIterator, Partitioner partitioner);


    public static <T extends HoodieRecordPayload> HoodieTable<T> getHoodieTable(
        HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
        switch (metaClient.getTableType()) {
            case COPY_ON_WRITE:
                return new HoodieCopyOnWriteTable<>(config, metaClient);
            case MERGE_ON_READ:
                return new HoodieMergeOnReadTable<>(config, metaClient);
            default:
                throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
        }
    }
}
