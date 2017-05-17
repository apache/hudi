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

import com.uber.hoodie.hive.client.HoodieHiveClient;
import com.uber.hoodie.hive.model.HoodieDatasetReference;
import com.uber.hoodie.hive.util.TestUtil;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;
import parquet.schema.MessageType;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HDroneDatasetTest {
    private HoodieHiveClient hiveClient;

    @Before
    public void setUp() throws IOException, InterruptedException {
        TestUtil.setUp();
        hiveClient = new HoodieHiveClient(TestUtil.hDroneConfiguration);
    }

    @Test
    public void testDatasetCreation() throws IOException, InitializationError {
        HoodieDatasetReference metadata = TestUtil
            .createDataset("test1", "/tmp/hdfs/DatasetSchemaTest/testSchema/", 5, "/nation.schema");
        HoodieHiveDatasetSyncTask dataset =
            HoodieHiveDatasetSyncTask.newBuilder().withReference(metadata)
                .withConfiguration(TestUtil.hDroneConfiguration).build();
        assertEquals("There should be 5 new partitions", 5, dataset.getNewPartitions().size());
        assertEquals("There should not be any changed partitions", 0,
            dataset.getChangedPartitions().size());
        assertFalse("Table should not exist", hiveClient.checkTableExists(metadata));
        dataset.sync();

        dataset = HoodieHiveDatasetSyncTask.newBuilder().withReference(metadata)
            .withConfiguration(TestUtil.hDroneConfiguration).build();
        assertTrue("Table should exist after flush", hiveClient.checkTableExists(metadata));
        assertEquals("After flush, There should not be any new partitions to flush", 0,
            dataset.getNewPartitions().size());
        assertEquals("After flush, There should not be any modified partitions to flush", 0,
            dataset.getChangedPartitions().size());

        assertEquals("Table Schema should have 5 fields", 5,
            hiveClient.getTableSchema(metadata).size());
    }

    @Test
    public void testDatasetEvolution() throws IOException, InitializationError {
        int initialPartitionsCount = 5;
        HoodieDatasetReference metadata = TestUtil
            .createDataset("test1", "/tmp/hdfs/DatasetSchemaTest/testSchema/",
                initialPartitionsCount, "/nation.schema");
        HoodieHiveDatasetSyncTask dataset =
            HoodieHiveDatasetSyncTask.newBuilder().withReference(metadata)
                .withConfiguration(TestUtil.hDroneConfiguration).build();
        dataset.sync();

        dataset = HoodieHiveDatasetSyncTask.newBuilder(dataset).build();
        int newSchemaversion = 2;
        int newPartitionsCount = 2;
        TestUtil.evolveDataset(metadata, newPartitionsCount, "/nation_evolved.schema",
            DateTime.now().getMillis(), newSchemaversion);
        dataset = HoodieHiveDatasetSyncTask.newBuilder(dataset).build();
        assertEquals("There should be " + newPartitionsCount + " partitions to be added",
            newPartitionsCount, dataset.getNewPartitions().size());
        dataset.sync();

        dataset = HoodieHiveDatasetSyncTask.newBuilder(dataset).build();
        MessageType newDatasetSchema = dataset.getSchemaSyncTask().getStorageSchema();
        MessageType expectedSchema = TestUtil.readSchema("/nation_evolved.schema");
        assertEquals("Table schema should be evolved schema", expectedSchema, newDatasetSchema);
        assertEquals("Table schema should have 6 fields", 6,
            hiveClient.getTableSchema(metadata).size());
        assertEquals("Valid Evolution should be reflected", "BIGINT",
            hiveClient.getTableSchema(metadata).get("region_key"));
    }

}
