/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hive;

import com.beust.jcommander.JCommander;
import com.uber.hoodie.hive.impl.DayBasedPartitionStrategy;
import com.uber.hoodie.hive.impl.ParseSchemaFromDataStrategy;
import com.uber.hoodie.hive.model.HoodieDatasetReference;

import org.apache.hadoop.conf.Configuration;

/**
 * Tool to sync new data from commits, into Hive in terms of
 *
 *  - New table/partitions
 *  - Updated schema for table/partitions
 */
public class HiveSyncTool {


    /**
     * Sync to Hive, based on day based partitioning
     *
     * @param cfg
     */
    public static void sync(HiveSyncConfig cfg) {
        // Configure to point to which metastore and database to connect to
        HoodieHiveConfiguration apiConfig =
                HoodieHiveConfiguration.newBuilder().hadoopConfiguration(new Configuration())
                        .hivedb(cfg.databaseName)
                        .hiveJdbcUrl(cfg.jdbcUrl)
                        .jdbcUsername(cfg.hiveUser)
                        .jdbcPassword(cfg.hivePass)
                        .build();

        HoodieDatasetReference datasetReference =
                new HoodieDatasetReference(cfg.tableName, cfg.basePath, cfg.databaseName);

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


    public static void main(String[] args) throws Exception {

        // parse the params
        final HiveSyncConfig cfg = new HiveSyncConfig();
        JCommander cmd = new JCommander(cfg, args);
        if (cfg.help || args.length == 0) {
            cmd.usage();
            System.exit(1);
        }

        sync(cfg);
    }
}
