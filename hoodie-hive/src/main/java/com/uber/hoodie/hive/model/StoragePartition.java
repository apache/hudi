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

package com.uber.hoodie.hive.model;

import com.google.common.base.Objects;
import com.uber.hoodie.hive.PartitionStrategy;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePartition {
    private static Logger LOG = LoggerFactory.getLogger(StoragePartition.class);
    private final PartitionStrategy partitionStrategy;
    private final Path partitionPath;
    private final HoodieDatasetReference metadata;

    public StoragePartition(HoodieDatasetReference metadata, PartitionStrategy partitionStrategy,
        FileStatus input) {
        this.metadata = metadata;
        this.partitionPath = Path.getPathWithoutSchemeAndAuthority(input.getPath());
        this.partitionStrategy = partitionStrategy;
    }

    public String[] getPartitionFieldValues() {
        return partitionStrategy.convertPartitionToValues(metadata, partitionPath);
    }

    public Path getPartitionPath() {
        return partitionPath;
    }

    @Override public String toString() {
        return Objects.toStringHelper(this).add("partitionPath", partitionPath)
            .add("metadata", metadata).toString();
    }
}
