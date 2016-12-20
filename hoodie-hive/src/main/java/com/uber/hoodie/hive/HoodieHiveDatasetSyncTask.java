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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.client.HoodieHiveClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import com.uber.hoodie.hive.model.StoragePartition;
import com.uber.hoodie.hive.model.TablePartition;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Represents a Hive External Dataset.
 * Contains metadata for storage and table partitions.
 */
public class HoodieHiveDatasetSyncTask {
    private static Logger LOG = LoggerFactory.getLogger(HoodieHiveDatasetSyncTask.class);
    private final HoodieHiveSchemaSyncTask schemaSyncTask;
    private final List<StoragePartition> newPartitions;
    private final List<StoragePartition> changedPartitions;

    public HoodieHiveDatasetSyncTask(HoodieHiveSchemaSyncTask schemaSyncTask,
        List<StoragePartition> newPartitions, List<StoragePartition> changedPartitions) {
        this.schemaSyncTask = schemaSyncTask;
        this.newPartitions = ImmutableList.copyOf(newPartitions);
        this.changedPartitions = ImmutableList.copyOf(changedPartitions);
    }

    public HoodieHiveSchemaSyncTask getSchemaSyncTask() {
        return schemaSyncTask;
    }

    public List<StoragePartition> getNewPartitions() {
        return newPartitions;
    }

    public List<StoragePartition> getChangedPartitions() {
        return changedPartitions;
    }

    /**
     * Sync this dataset
     * 1. If any schema difference is found, then sync the table schema
     * 2. If any new partitions are found, adds partitions to the table (which uses the table schema by default)
     * 3. If any partition path has changed, modify the partition to the new path (which does not change the partition schema)
     */
    public void sync() {
        LOG.info("Starting Sync for " + schemaSyncTask.getReference());
        try {
            // First sync the table schema
            schemaSyncTask.sync();

            // Add all the new partitions
            schemaSyncTask.getHiveClient()
                .addPartitionsToTable(schemaSyncTask.getReference(), newPartitions,
                    schemaSyncTask.getPartitionStrategy());
            // Update all the changed partitions
            schemaSyncTask.getHiveClient()
                .updatePartitionsToTable(schemaSyncTask.getReference(), changedPartitions,
                    schemaSyncTask.getPartitionStrategy());
        } catch (Exception e) {
            throw new HoodieHiveDatasetException(
                "Failed to sync dataset " + schemaSyncTask.getReference(), e);
        }
        LOG.info("Sync for " + schemaSyncTask.getReference() + " complete.");
    }

    public static Builder newBuilder(HoodieHiveDatasetSyncTask dataset) {
        return newBuilder().withConfiguration(dataset.schemaSyncTask.getConf())
            .withReference(dataset.schemaSyncTask.getReference())
            .withFSClient(dataset.schemaSyncTask.getFsClient())
            .withHiveClient(dataset.schemaSyncTask.getHiveClient())
            .schemaStrategy(dataset.schemaSyncTask.getSchemaStrategy())
            .partitionStrategy(dataset.schemaSyncTask.getPartitionStrategy());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private static Logger LOG = LoggerFactory.getLogger(Builder.class);
        private HoodieHiveConfiguration configuration;
        private HoodieDatasetReference datasetReference;
        private SchemaStrategy schemaStrategy;
        private PartitionStrategy partitionStrategy;
        private HoodieHiveClient hiveClient;
        private HoodieFSClient fsClient;

        public Builder withReference(HoodieDatasetReference reference) {
            this.datasetReference = reference;
            return this;
        }

        public Builder withConfiguration(HoodieHiveConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder schemaStrategy(SchemaStrategy schemaStrategy) {
            this.schemaStrategy = schemaStrategy;
            return this;
        }

        public Builder partitionStrategy(PartitionStrategy partitionStrategy) {
            if(partitionStrategy != null) {
                LOG.info("Partitioning the dataset with keys " + ArrayUtils
                    .toString(partitionStrategy.getHivePartitionFieldNames()));
            }
            this.partitionStrategy = partitionStrategy;
            return this;
        }

        public Builder withHiveClient(HoodieHiveClient hiveClient) {
            this.hiveClient = hiveClient;
            return this;
        }

        public Builder withFSClient(HoodieFSClient fsClient) {
            this.fsClient = fsClient;
            return this;
        }

        public HoodieHiveDatasetSyncTask build() {
            LOG.info("Building dataset for " + datasetReference);
            HoodieHiveSchemaSyncTask schemaSyncTask =
                HoodieHiveSchemaSyncTask.newBuilder().withReference(datasetReference)
                    .withConfiguration(configuration).schemaStrategy(schemaStrategy)
                    .partitionStrategy(partitionStrategy).withHiveClient(hiveClient)
                    .withFSClient(fsClient).build();

            List<StoragePartition> storagePartitions = Lists.newArrayList();
            FileStatus[] storagePartitionPaths = schemaSyncTask.getPartitionStrategy()
                .scanAllPartitions(schemaSyncTask.getReference(), schemaSyncTask.getFsClient());
            for (FileStatus fileStatus : storagePartitionPaths) {
                storagePartitions.add(new StoragePartition(schemaSyncTask.getReference(),
                    schemaSyncTask.getPartitionStrategy(), fileStatus));
            }
            LOG.info("Storage partitions scan complete. Found " + storagePartitions.size());

            List<StoragePartition> newPartitions;
            List<StoragePartition> changedPartitions;

            // Check if table exists
            if (schemaSyncTask.getHiveClient().checkTableExists(schemaSyncTask.getReference())) {
                List<TablePartition> partitions =
                    schemaSyncTask.getHiveClient().scanPartitions(schemaSyncTask.getReference());
                LOG.info("Table partition scan complete. Found " + partitions.size());
                newPartitions = schemaSyncTask.getFsClient()
                    .getUnregisteredStoragePartitions(partitions, storagePartitions);
                changedPartitions = schemaSyncTask.getFsClient()
                    .getChangedStoragePartitions(partitions, storagePartitions);
            } else {
                newPartitions = storagePartitions;
                changedPartitions = Lists.newArrayList();
            }
            return new HoodieHiveDatasetSyncTask(schemaSyncTask, newPartitions, changedPartitions);
        }
    }
}
