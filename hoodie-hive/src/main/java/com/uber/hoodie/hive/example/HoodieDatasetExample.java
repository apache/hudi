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

package com.uber.hoodie.hive.example;

import com.uber.hoodie.hive.HoodieHiveConfiguration;
import com.uber.hoodie.hive.HoodieHiveDatasetSyncTask;
import com.uber.hoodie.hive.PartitionStrategy;
import com.uber.hoodie.hive.SchemaStrategy;
import com.uber.hoodie.hive.impl.DayBasedPartitionStrategy;
import com.uber.hoodie.hive.impl.ParseSchemaFromDataStrategy;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import org.apache.hadoop.conf.Configuration;

/**
 * Example showing basic usage of Hoodie Hive API
 */
public class HoodieDatasetExample {
    public static void main(String[] args) {
        // Configure to point to which metastore and database to connect to
        HoodieHiveConfiguration apiConfig =
            HoodieHiveConfiguration.newBuilder().hadoopConfiguration(new Configuration())
                .hivedb("tmp").hiveJdbcUrl("jdbc:hive2://localhost:10010/").jdbcUsername("hive")
                .jdbcPassword("hive").build();

        HoodieDatasetReference datasetReference =
            new HoodieDatasetReference("clickstream", "hdfs:///data/tables/user.clickstream",
                "raw");

        // initialize the strategies
        PartitionStrategy partitionStrategy = new DayBasedPartitionStrategy();
        SchemaStrategy schemaStrategy = new ParseSchemaFromDataStrategy();

        // Creates a new dataset which reflects the state at the time of creation
        HoodieHiveDatasetSyncTask datasetSyncTask =
            HoodieHiveDatasetSyncTask.newBuilder().withReference(datasetReference)
                .withConfiguration(apiConfig).partitionStrategy(partitionStrategy)
                .schemaStrategy(schemaStrategy).build();

        // Sync dataset
        datasetSyncTask.sync();
    }
}
