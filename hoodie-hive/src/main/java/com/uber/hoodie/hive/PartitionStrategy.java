/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.hive;

import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Abstraction to define HDFS partition strategies.
 * Strategy provides hookups to map partitions on to physical layout
 *
 * @see SchemaStrategy
 */
public interface PartitionStrategy {
    /**
     * Scans the file system for all partitions and returns FileStatus[] which are the available partitions
     *
     * @param basePath
     * @param fsClient
     * @return
     */
    FileStatus[] scanAllPartitions(HoodieDatasetReference basePath, HoodieFSClient fsClient);

    /**
     * Get the list of hive field names the dataset will be partitioned on.
     * The field name should be present in the storage schema.
     *
     * @return List of partitions field names
     */
    String[] getHivePartitionFieldNames();

    /**
     * Convert a Partition path (returned in scanAllPartitions) to values for column names returned in getHivePartitionFieldNames
     * e.g. /data/topic/2016/12/12/ will return [2016, 12, 12]
     *
     * @param partition storage path
     * @return List of partitions field values
     */
    String[] convertPartitionToValues(HoodieDatasetReference metadata, Path partition);
}
