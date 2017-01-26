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

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.exception.HoodieException;

import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Abstract implementation of a HoodieTable
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {

    protected final String commitTime;

    protected final HoodieWriteConfig config;

    protected final HoodieTableMetadata metadata;

    protected HoodieTable(String commitTime, HoodieWriteConfig config, HoodieTableMetadata metadata) {
        this.commitTime = commitTime;
        this.config = config;
        this.metadata = metadata;
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


    /**
     * Perform the ultimate IO for a given upserted (RDD) partition
     *
     * @param partition
     * @param recordIterator
     * @param partitioner
     */
    public abstract Iterator<List<WriteStatus>> handleUpsertPartition(Integer partition,
                                                                      Iterator<HoodieRecord<T>> recordIterator,
                                                                      Partitioner partitioner);

    /**
     * Perform the ultimate IO for a given inserted (RDD) partition
     *
     * @param partition
     * @param recordIterator
     * @param partitioner
     */
    public abstract Iterator<List<WriteStatus>> handleInsertPartition(Integer partition,
                                                                      Iterator<HoodieRecord<T>> recordIterator,
                                                                      Partitioner partitioner);


    public static HoodieTable getHoodieTable(HoodieTableType type,
                                             String commitTime,
                                             HoodieWriteConfig config,
                                             HoodieTableMetadata metadata) {
        if (type == HoodieTableType.COPY_ON_WRITE) {
            return new HoodieCopyOnWriteTable(commitTime, config, metadata);
        } else {
            throw new HoodieException("Unsupported table type :"+ type);
        }
    }
}
