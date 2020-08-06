/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieCreateHandle;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUpdateSchemaEvolution extends HoodieClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath);
    initSparkContexts("TestUpdateSchemaEvolution");
    initFileSystem();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  @Test
  public void testSchemaEvolutionOnUpdate() throws Exception {
    // Create a bunch of records with a old version of schema
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.txt");
    final HoodieTable<?> table = HoodieTable.create(config, hadoopConf);

    final List<WriteStatus> statuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
      String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
      String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
      List<HoodieRecord> insertRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
      insertRecords
          .add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
      RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
      insertRecords
          .add(new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
      RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
      insertRecords
          .add(new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

      HoodieCreateHandle createHandle =
          new HoodieCreateHandle(config, "100", table, rowChange1.getPartitionPath(), "f1-0", insertRecords.iterator(), supplier);
      createHandle.write();
      return createHandle.close();
    }).collect();

    final Path commitFile = new Path(config.getBasePath() + "/.hoodie/" + HoodieTimeline.makeCommitFileName("100"));
    FSUtils.getFs(basePath, HoodieTestUtils.getDefaultHadoopConf()).create(commitFile);

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchema.txt");
    final WriteStatus insertResult = statuses.get(0);
    String fileId = insertResult.getFileId();

    final HoodieTable table2 = HoodieTable.create(config2, hadoopConf);
    assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
      HoodieRecord record1 =
          new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1);
      record1.unseal();
      record1.setCurrentLocation(new HoodieRecordLocation("100", fileId));
      record1.seal();
      updateRecords.add(record1);

      assertDoesNotThrow(() -> {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2,
            updateRecords.iterator(), record1.getPartitionPath(), fileId, supplier);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchemaWithMetafields());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      }, "UpdateFunction could not read records written with exampleSchema.txt using the "
          + "exampleEvolvedSchema.txt");

      return 1;
    }).collect().size());
  }

  private HoodieWriteConfig makeHoodieClientConfig(String name) {
    Schema schema = getSchemaFromResource(getClass(), name);
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schema.toString()).build();
  }
}
