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

package com.uber.hoodie.index;

import com.google.common.base.Optional;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieRecord;

import com.uber.hoodie.exception.HoodieIndexException;
import com.uber.hoodie.table.HoodieTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Base class for different types of indexes to determine the mapping from uuid
 *
 */
public abstract class HoodieIndex<T extends HoodieRecordPayload> implements Serializable {
    protected transient JavaSparkContext jsc = null;

    public enum IndexType {
        HBASE,
        INMEMORY,
        BLOOM,
        BUCKETED
    }

    protected final HoodieWriteConfig config;

    protected HoodieIndex(HoodieWriteConfig config, JavaSparkContext jsc) {
        this.config = config;
        this.jsc = jsc;
    }

    /**
     * Checks if the given [Keys] exists in the hoodie table and returns [Key, Optional[FullFilePath]]
     * If the optional FullFilePath value is not present, then the key is not found. If the FullFilePath
     * value is present, it is the path component (without scheme) of the URI underlying file
     *
     * @param hoodieKeys
     * @param table
     * @return
     */
    public abstract JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(
        JavaRDD<HoodieKey> hoodieKeys, final HoodieTable<T> table);

    /**
     * Looks up the index and tags each incoming record with a location of a file that contains the
     * row (if it is actually present)
     */
    public abstract JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
        HoodieTable<T> hoodieTable) throws HoodieIndexException;

    /**
     * Extracts the location of written records, and updates the index.
     *
     * TODO(vc): We may need to propagate the record as well in a WriteStatus class
     */
    public abstract JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
        HoodieTable<T> hoodieTable) throws HoodieIndexException;

    /**
     * Rollback the efffects of the commit made at commitTime.
     */
    public abstract boolean rollbackCommit(String commitTime);

    public static <T extends HoodieRecordPayload> HoodieIndex<T> createIndex(
            HoodieWriteConfig config, JavaSparkContext jsc) throws HoodieIndexException {
        switch (config.getIndexType()) {
            case HBASE:
                return new HBaseIndex<>(config, jsc);
            case INMEMORY:
                return new InMemoryHashIndex<>(config, jsc);
            case BLOOM:
                return new HoodieBloomIndex<>(config, jsc);
            case BUCKETED:
                return new BucketedIndex<>(config, jsc);
        }
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
}
