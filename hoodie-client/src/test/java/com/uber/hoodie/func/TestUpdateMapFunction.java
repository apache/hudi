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

package com.uber.hoodie.func;

import static org.junit.Assert.fail;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.HoodieCreateHandle;
import com.uber.hoodie.io.HoodieMergeHandle;
import com.uber.hoodie.table.HoodieCopyOnWriteTable;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestUpdateMapFunction implements Serializable {

  private String basePath = null;
  private transient JavaSparkContext jsc = null;

  @Before
  public void init() throws Exception {
    // Create a temp folder as the base path
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    this.basePath = folder.getRoot().getAbsolutePath();
    HoodieTestUtils.init(HoodieTestUtils.getDefaultHadoopConf(), basePath);
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestUpdateMapFunction"));
  }

  @After
  public void clean() {
    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }

  @Test
  public void testSchemaEvolutionOnUpdate() throws Exception {
    // Create a bunch of records with a old version of schema
    final HoodieWriteConfig config = makeHoodieClientConfig("/exampleSchema.txt");
    final HoodieCopyOnWriteTable table = new HoodieCopyOnWriteTable(config, jsc);

    final List<WriteStatus> statuses = jsc.parallelize(Arrays.asList(1)).map(x -> {
      String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
      String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
      String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
      List<HoodieRecord> insertRecords = new ArrayList<>();
      TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
      insertRecords
          .add(new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
      TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
      insertRecords
          .add(new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
      TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
      insertRecords
          .add(new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));

      HoodieCreateHandle createHandle = new HoodieCreateHandle(config, "100", table, rowChange1.getPartitionPath(),
          "f1-0", insertRecords.iterator());
      createHandle.write();
      WriteStatus insertResult = createHandle.close();
      return insertResult;
    }).collect();

    final Path commitFile = new Path(config.getBasePath() + "/.hoodie/" + HoodieTimeline.makeCommitFileName("100"));
    FSUtils.getFs(basePath, HoodieTestUtils.getDefaultHadoopConf()).create(commitFile);

    // Now try an update with an evolved schema
    // Evolved schema does not have guarantee on preserving the original field ordering
    final HoodieWriteConfig config2 = makeHoodieClientConfig("/exampleEvolvedSchema.txt");
    final Schema schema = Schema.parse(config2.getSchema());
    final WriteStatus insertResult = statuses.get(0);
    String fileId = insertResult.getFileId();

    final HoodieCopyOnWriteTable table2 = new HoodieCopyOnWriteTable(config2, jsc);
    Assert.assertEquals(1, jsc.parallelize(Arrays.asList(1)).map(x -> {
      // New content with values for the newly added field
      String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
          + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12,\"added_field\":1}";
      List<HoodieRecord> updateRecords = new ArrayList<>();
      TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
      HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()),
          rowChange1);
      record1.setCurrentLocation(new HoodieRecordLocation("100", fileId));
      updateRecords.add(record1);

      try {
        HoodieMergeHandle mergeHandle = new HoodieMergeHandle(config2, "101", table2, updateRecords.iterator(), fileId);
        Configuration conf = new Configuration();
        AvroReadSupport.setAvroReadSchema(conf, mergeHandle.getWriterSchema());
        List<GenericRecord> oldRecords = ParquetUtils.readAvroRecords(conf,
            new Path(config2.getBasePath() + "/" + insertResult.getStat().getPath()));
        for (GenericRecord rec : oldRecords) {
          mergeHandle.write(rec);
        }
        mergeHandle.close();
      } catch (ClassCastException e) {
        fail("UpdateFunction could not read records written with exampleSchema.txt using the "
            + "exampleEvolvedSchema.txt");
      }
      return 1;
    }).collect().size());
  }

  private HoodieWriteConfig makeHoodieClientConfig(String schema) throws Exception {
    // Prepare the AvroParquetIO
    String schemaStr = IOUtils.toString(getClass().getResourceAsStream(schema), "UTF-8");
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr).build();
  }
}
