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

package com.uber.hoodie.func;

import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieTableMetadata;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.table.HoodieCopyOnWriteTable;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.fail;

public class TestUpdateMapFunction {
    private String basePath = null;

    @Before
    public void init() throws Exception {
        // Create a temp folder as the base path
        TemporaryFolder folder = new TemporaryFolder();
        folder.create();
        this.basePath = folder.getRoot().getAbsolutePath();
        HoodieTestUtils.initializeHoodieDirectory(basePath);
    }

    @Test
    public void testSchemaEvolutionOnUpdate() throws Exception {
        // Create a bunch of records with a old version of schema
        HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.txt");
        HoodieTableMetadata metadata = new HoodieTableMetadata(FSUtils.getFs(), basePath);
        HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable("100", config, metadata);

        String recordStr1 =
            "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
        String recordStr2 =
            "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
        String recordStr3 =
            "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
        List<HoodieRecord> records = new ArrayList<>();
        TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
        records.add(
            new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()),
                rowChange1));
        TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
        records.add(
            new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()),
                rowChange2));
        TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
        records.add(
            new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()),
                rowChange3));
        Iterator<List<WriteStatus>> insertResult = table.handleInsert(records.iterator());
        Path commitFile =
            new Path(config.getBasePath() + "/.hoodie/" + FSUtils.makeCommitFileName("100"));
        FSUtils.getFs().create(commitFile);

        // Now try an update with an evolved schema
        // Evolved schema does not have guarantee on preserving the original field ordering
        config = makeHoodieClientConfig("/exampleEvolvedSchema.txt");
        metadata = new HoodieTableMetadata(FSUtils.getFs(), basePath);
        String fileId = insertResult.next().get(0).getFileId();
        System.out.println(fileId);


        table = new HoodieCopyOnWriteTable("101", config, metadata);
        // New content with values for the newly added field
        recordStr1 =
            "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\",\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
        records = new ArrayList<>();
        rowChange1 = new TestRawTripPayload(recordStr1);
        HoodieRecord record1 =
            new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()),
                rowChange1);
        record1.setCurrentLocation(new HoodieRecordLocation("100", fileId));
        records.add(record1);

        try {
            table.handleUpdate(fileId, records.iterator());
        } catch (ClassCastException e) {
            fail(
                "UpdateFunction could not read records written with exampleSchema.txt using the exampleEvolvedSchema.txt");
        }
    }

    private HoodieWriteConfig makeHoodieClientConfig(String schema) throws Exception {
        // Prepare the AvroParquetIO
        String schemaStr = IOUtils.toString(getClass().getResourceAsStream(schema), "UTF-8");
        return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr).build();
    }

}
