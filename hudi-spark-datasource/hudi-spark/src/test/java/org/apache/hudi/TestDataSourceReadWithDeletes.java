/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestDataSourceReadWithDeletes extends SparkClientFunctionalTestHarness {

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_operation\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"partition_path\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  private Schema schema;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void test() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    String[] dataset1 = new String[] {"I,id1,Danny,23,1,par1", "I,id2,Tony,20,1,par1"};
    SparkRDDWriteClient client = getHoodieWriteClient(config);
    String insertTime1 = client.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client, insertTime1, dataset1);
    client.commit(insertTime1, jsc().parallelize(writeStatuses1));

    String[] dataset2 = new String[] {
        "I,id1,Danny,30,2,par1",
        "D,id2,Tony,20,2,par1",
        "I,id3,Julian,40,2,par1",
        "D,id4,Stephan,35,2,par1"};
    String insertTime2 = client.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client, insertTime2, dataset2);
    client.commit(insertTime2, jsc().parallelize(writeStatuses2));

    List<Row> rows = spark().read().format("org.apache.hudi")
        .option("hoodie.datasource.query.type", "snapshot")
        .load(config.getBasePath())
        .select("id", "name", "age", "ts", "partition_path")
        .collectAsList();
    assertEquals(2, rows.size());
    String[] expected = new String[] {
        "[id1,Danny,30,2,par1]",
        "[id3,Julian,40,2,par1]"};
    assertArrayEquals(expected, rows.stream().map(Row::toString).sorted().toArray(String[]::new));
    client.close();
  }

  private HoodieWriteConfig createHoodieWriteConfig() {
    Properties props = getPropertiesForKeyGen(true);
    props.put(TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    String basePath = basePath();
    return HoodieWriteConfig.newBuilder()
        .forTable("test")
        .withPath(basePath)
        .withSchema(jsonSchema)
        .withParallelism(2, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .parquetMaxFileSize(1024).build())
        .withLayoutConfig(HoodieLayoutConfig.newBuilder()
            .withLayoutType(HoodieStorageLayout.LayoutType.BUCKET.name())
            .withLayoutPartitioner(SparkBucketIndexPartitioner.class.getName()).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .fromProperties(props)
            .withIndexType(HoodieIndex.IndexType.BUCKET)
            .withBucketNum("1")
            .build())
        .withPopulateMetaFields(true)
        .withAllowOperationMetadataField(true)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .build();
  }

  private List<WriteStatus> writeData(
      SparkRDDWriteClient client,
      String instant,
      String[] records) {
    List<HoodieRecord> recordList = str2HoodieRecord(records);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(recordList, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> writeStatuses = client.upsert(writeRecords, instant).collect();
    assertNoWriteErrors(writeStatuses);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }

  private List<HoodieRecord> str2HoodieRecord(String[] records) {
    return Stream.of(records).map(rawRecordStr -> {
      String[] parts = rawRecordStr.split(",");
      boolean isDelete = parts[0].equalsIgnoreCase("D");
      GenericRecord record = new GenericData.Record(schema);
      record.put("id", parts[1]);
      record.put("name", parts[2]);
      record.put("age", Integer.parseInt(parts[3]));
      record.put("ts", Long.parseLong(parts[4]));
      record.put("partition_path", parts[5]);
      OverwriteWithLatestAvroPayload payload = new OverwriteWithLatestAvroPayload(record, (Long) record.get("ts"));
      return new HoodieAvroRecord<>(
          new HoodieKey((String) record.get("id"), (String) record.get("partition_path")),
          payload,
          isDelete ? HoodieOperation.DELETE : HoodieOperation.INSERT);
    }).collect(Collectors.toList());
  }
}
