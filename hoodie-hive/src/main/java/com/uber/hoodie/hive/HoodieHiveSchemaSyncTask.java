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


import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hive.impl.DayBasedPartitionStrategy;
import com.uber.hoodie.hive.client.HoodieFSClient;
import com.uber.hoodie.hive.client.HoodieHiveClient;
import com.uber.hoodie.hive.impl.ParseSchemaFromDataStrategy;
import com.uber.hoodie.hive.client.SchemaUtil;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import com.uber.hoodie.hive.model.SchemaDifference;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.schema.MessageType;

import java.util.Map;

/**
 * Represents the Schema sync task for the dataset.
 * Execute sync() on this task to sync up the HDFS dataset schema and hive table schema
 */
public class HoodieHiveSchemaSyncTask {
    private static Logger LOG = LoggerFactory.getLogger(HoodieHiveSchemaSyncTask.class);

    private static final String DEFAULT_INPUTFORMAT = HoodieInputFormat.class.getName();
    private static final String DEFAULT_OUTPUTFORMAT = MapredParquetOutputFormat.class.getName();

    private final HoodieDatasetReference reference;
    private final MessageType storageSchema;
    private final Map<String, String> tableSchema;
    private final PartitionStrategy partitionStrategy;
    private final SchemaStrategy schemaStrategy;
    private final HoodieHiveClient hiveClient;
    private final HoodieHiveConfiguration conf;
    private final HoodieFSClient fsClient;

    public HoodieHiveSchemaSyncTask(HoodieDatasetReference datasetReference,
        MessageType schemaInferred, Map<String, String> fieldsSchema,
        PartitionStrategy partitionStrategy, SchemaStrategy schemaStrategy,
        HoodieHiveConfiguration configuration, HoodieHiveClient hiveClient,
        HoodieFSClient fsClient) {
        this.reference = datasetReference;
        this.storageSchema = schemaInferred;
        this.tableSchema = fieldsSchema;
        this.partitionStrategy = partitionStrategy;
        this.schemaStrategy = schemaStrategy;
        this.hiveClient = hiveClient;
        this.conf = configuration;
        this.fsClient = fsClient;
    }

    public SchemaDifference getSchemaDifference() {
        return SchemaUtil.getSchemaDifference(storageSchema, tableSchema,
            partitionStrategy.getHivePartitionFieldNames());
    }

    /**
     * Checks if the table schema is present. If not, creates one.
     * If already exists, computes the schema difference and if there is any difference
     * it generates a alter table and syncs up the schema to hive metastore.
     */
    public void sync() {
        try {
            // Check if the table needs to be created
            if (tableSchema.isEmpty()) {
                // create the database
                LOG.info("Schema not found. Creating for " + reference);
                hiveClient.createTable(storageSchema, reference,
                    partitionStrategy.getHivePartitionFieldNames(), DEFAULT_INPUTFORMAT,
                    DEFAULT_OUTPUTFORMAT);
            } else {
                if (!getSchemaDifference().isEmpty()) {
                    LOG.info("Schema sync required for " + reference);
                    hiveClient.updateTableDefinition(reference,
                        partitionStrategy.getHivePartitionFieldNames(), storageSchema);
                } else {
                    LOG.info("Schema sync not required for " + reference);
                }
            }
        } catch (Exception e) {
            throw new HoodieHiveDatasetException("Failed to sync dataset " + reference,
                e);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public MessageType getStorageSchema() {
        return storageSchema;
    }

    public Map<String, String> getTableSchema() {
        return tableSchema;
    }

    public PartitionStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public SchemaStrategy getSchemaStrategy() {
        return schemaStrategy;
    }

    public HoodieHiveClient getHiveClient() {
        return hiveClient;
    }

    public HoodieHiveConfiguration getConf() {
        return conf;
    }

    public HoodieDatasetReference getReference() {
        return reference;
    }

    public HoodieFSClient getFsClient() {
        return fsClient;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        HoodieHiveSchemaSyncTask that = (HoodieHiveSchemaSyncTask) o;
        return Objects.equal(storageSchema, that.storageSchema) && Objects
            .equal(tableSchema, that.tableSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(storageSchema, tableSchema);
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

        public HoodieHiveSchemaSyncTask build() {
            LOG.info("Building dataset schema for " + datasetReference);
            createDefaults();

            MessageType schemaInferred =
                schemaStrategy.getDatasetSchema(datasetReference, fsClient);
            LOG.info("Storage Schema inferred for dataset " + datasetReference);
            LOG.debug("Inferred Storage Schema " + schemaInferred);

            Map<String, String> fieldsSchema;
            if (!hiveClient.checkTableExists(datasetReference)) {
                fieldsSchema = Maps.newHashMap();
            } else {
                fieldsSchema = hiveClient.getTableSchema(datasetReference);
            }
            LOG.info("Table Schema inferred for dataset " + datasetReference);
            LOG.debug("Inferred Table Schema " + fieldsSchema);

            return new HoodieHiveSchemaSyncTask(datasetReference, schemaInferred, fieldsSchema,
                partitionStrategy, schemaStrategy, configuration, hiveClient, fsClient);
        }

        private void createDefaults() {
            if (partitionStrategy == null) {
                LOG.info("Partition strategy is not set. Selecting the default strategy");
                partitionStrategy = new DayBasedPartitionStrategy();
            }
            if (schemaStrategy == null) {
                LOG.info(
                    "Schema strategy not specified. Selecting the default based on the dataset type");
                schemaStrategy = new ParseSchemaFromDataStrategy();
            }
            if (fsClient == null) {
                LOG.info("Creating a new FS Client as none has been passed in");
                fsClient = new HoodieFSClient(configuration);
            }
            if (hiveClient == null) {
                LOG.info("Creating a new Hive Client as none has been passed in");
                hiveClient = new HoodieHiveClient(configuration);
            }
        }
    }
}
