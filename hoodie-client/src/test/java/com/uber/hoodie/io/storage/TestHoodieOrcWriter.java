/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.io.storage;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.hoodie.common.BloomFilter;
import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHoodieOrcWriter {

  private JavaSparkContext jsc = null;
  private String basePath = null;
  private transient FileSystem fs;
  private String schemaStr;
  private Schema schema;

  @Before
  public void init() throws IOException {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieOrcWriter"));
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, jsc.hadoopConfiguration());
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath);
    schemaStr = IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
  }

  @Test
  public void testWriteOrcFile() throws Exception {
    // Create some records to use
    String recordStr1 = "{\"_row_key\":\"1eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"2eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"3eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"4eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":32}";
    TestRawTripPayload rowChange1 = new TestRawTripPayload(recordStr1);
    HoodieRecord record1 = new HoodieRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()),
        rowChange1);
    TestRawTripPayload rowChange2 = new TestRawTripPayload(recordStr2);
    HoodieRecord record2 = new HoodieRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()),
        rowChange2);
    TestRawTripPayload rowChange3 = new TestRawTripPayload(recordStr3);
    HoodieRecord record3 = new HoodieRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()),
        rowChange3);
    TestRawTripPayload rowChange4 = new TestRawTripPayload(recordStr4);
    HoodieRecord record4 = new HoodieRecord(new HoodieKey(rowChange4.getRowKey(), rowChange4.getPartitionPath()),
        rowChange4);

    // We write record1, record2 to a orc file, but the bloom filter contains (record1,
    // record2, record3).
    BloomFilter filter = new BloomFilter(10000, 0.0000001);
    filter.add(record3.getRecordKey());

    String commitTime = HoodieTestUtils.makeNewCommitTime();

    Path path = new Path(basePath + "/orc_dc.orc");

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .orcBloomFilterColumns("_hoodie_record_key")
        .orcColumns("_row_key,time,number,_hoodie_commit_time,"
            + "_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path")
        .orcColumnsTypes("string,string,int,string,string,string,string")
        .build();

    HoodieOrcWriter writer = new HoodieOrcWriter(commitTime,
        path,
        config,
        new Configuration()
    );

    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> beforeList = new LinkedList<>();
    List<HoodieRecord> records = Arrays.asList(record1, record2, record3, record4);
    int seqId = 1;
    for (HoodieRecord record : records) {
      GenericRecord avroRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
      HoodieAvroUtils.addCommitMetadataToRecord(avroRecord, commitTime, "" + seqId++);
      HoodieAvroUtils.addHoodieKeyToRecord(avroRecord, record.getRecordKey(), record.getPartitionPath(), "orc");
      writer.writeAvro(record.getRecordKey(), avroRecord);
      filter.add(record.getRecordKey());

      Map<String, Object> recordMap = mapper.readValue(avroRecord.toString(), Map.class);
      beforeList.add(recordMap);
    }
    writer.close();

    // value has been written to orc, read it out and verify.
    OrcSerde serde = new OrcSerde();
    Properties p = new Properties();
    p.setProperty("columns", "_row_key,time,number,_hoodie_commit_time,"
        + "_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path");
    p.setProperty("columns.types", "string,string,int,string,string,string,string");
    serde.initialize(HoodieTestUtils.getDefaultHadoopConf(), p);
    StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(HoodieTestUtils.getDefaultHadoopConf()));

    Object row = null;
    RecordReader rows = reader.rows();
    List<? extends StructField> fields = inspector.getAllStructFieldRefs();
    List<Map<String, Object>> afterList = new LinkedList<>();
    while (rows.hasNext()) {
      row = rows.next(row);

      StringBuilder sb = new StringBuilder();
      sb.append("{")
          .append("\"").append("_row_key").append("\"").append(":").append("\"")
          .append(inspector.getStructFieldData(row, fields.get(0))).append("\",")
          .append("\"").append("time").append("\"").append(":").append("\"")
          .append(inspector.getStructFieldData(row, fields.get(1))).append("\",")
          .append("\"").append("number").append("\"").append(":")
          .append(inspector.getStructFieldData(row, fields.get(2))).append(",")
          .append("\"").append("_hoodie_commit_time").append("\"").append(":").append("\"")
          .append(inspector.getStructFieldData(row, fields.get(3))).append("\",")
          .append("\"").append("_hoodie_commit_seqno").append("\"").append(":").append("\"")
          .append(inspector.getStructFieldData(row, fields.get(4))).append("\",")
          .append("\"").append("_hoodie_record_key").append("\"").append(":").append("\"")
          .append(inspector.getStructFieldData(row, fields.get(5))).append("\",")
          .append("\"").append("_hoodie_partition_path").append("\"").append(":").append("\"")
          .append(inspector.getStructFieldData(row, fields.get(6))).append("\"")
          .append("}");

      Map<String, Object> recordMap = mapper.readValue(sb.toString(), Map.class);
      afterList.add(recordMap);
    }

    assertEquals(beforeList.size(), afterList.size());
    for (int i = 0; i < beforeList.size(); i++) {
      Map<String, Object> before = beforeList.get(i);
      Map<String, Object> after = afterList.get(i);

      assertEquals(before.get("_row_key"), after.get("_row_key"));
      assertEquals(before.get("time"), after.get("time"));
      assertEquals(before.get("number"), after.get("number"));
      assertEquals(before.get("_hoodie_commit_time"), after.get("_hoodie_commit_time"));
      assertEquals(before.get("_hoodie_commit_seqno"), after.get("_hoodie_commit_seqno"));
      assertEquals(before.get("_hoodie_record_key"), after.get("_hoodie_record_key"));
      assertEquals(before.get("_hoodie_partition_path"), after.get("_hoodie_partition_path"));
    }

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
}
