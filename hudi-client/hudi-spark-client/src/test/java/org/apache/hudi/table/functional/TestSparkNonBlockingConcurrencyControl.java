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

package org.apache.hudi.table.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.BucketIndexConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLayoutConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;
import org.apache.hudi.table.storage.HoodieStorageLayout;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TYPE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("functional")
public class TestSparkNonBlockingConcurrencyControl extends SparkClientFunctionalTestHarness {

  String jsonSchema = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_commit_seqno\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_record_key\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_partition_path\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_hoodie_file_name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"age\", \"type\": [\"null\", \"int\"]},\n"
      + "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"part\", \"type\": [\"null\", \"string\"]}\n"
      + "  ]\n"
      + "}";

  private Schema schema;
  private HoodieTableMetaClient metaClient;

  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void testNonBlockingConcurrencyControlWithPartialUpdatePayload() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    String insertTime1 = client1.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = client2.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        context().parallelize(writeStatuses1, 1),
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 2nd txn
    client2.commitStats(
        insertTime2,
        context().parallelize(writeStatuses2, 1),
        writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // There is no base file in partition dir because there is no compaction yet.
    assertFalse(fileExists(), "No base data files should have been created");

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    client1.compact(compactionTime);

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
  }

  @Test
  public void testNonBlockingConcurrencyControlWithInflightInstant() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = client1.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = client2.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, false, WriteOperationType.INSERT);

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        context().parallelize(writeStatuses1, 1),
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // schedule compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();

    // step to commit the 3rd txn, insert record: [id3,Julian,53,4,par1] and commit 3rd txn
    List<String> dataset3 = Collections.singletonList("id3,Julian,53,4,par1");
    String insertTime3 = client1.createNewInstantTime();
    List<WriteStatus> writeStatuses3 = writeData(client1, insertTime3, dataset3, false, WriteOperationType.INSERT);
    client1.commitStats(
        insertTime3,
        context().parallelize(writeStatuses3, 1),
        writeStatuses3.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // do compaction
    client1.compact(compactionTime);

    // read optimized result is [(id1,Danny,23,1,par1)]
    // because 2nd commit is in inflight state and
    // the data files belongs 3rd commit is not included in the last compaction.
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,null,1,par1]");
    checkWrittenData(result, 1);
  }

  // case1: txn1 is upsert writer, txn2 is bulk_insert writer.
  //      |----------- txn1 -----------|
  //                       |----- txn2 ------|
  // the txn2 would fail to commit caused by conflict
  @Test
  public void testBulkInsertInMultiWriter() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);

    // start the 1st txn and insert record: [id1,Danny,null,1,par1], suspend the tx commit
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = client1.createNewInstantTime();
    List<WriteStatus> writeStatuses1 = writeData(client1, insertTime1, dataset1, false, WriteOperationType.INSERT);

    // start the 2nd txn and bulk insert record: [id1,null,23,2,par1], suspend the tx commit
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = client2.createNewInstantTime();
    List<WriteStatus> writeStatuses2 = writeData(client2, insertTime2, dataset2, false, WriteOperationType.BULK_INSERT);

    // step to commit the 1st txn
    client1.commitStats(
        insertTime1,
        context().parallelize(writeStatuses1, 1),
        writeStatuses1.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(),
        metaClient.getCommitActionType());

    // step to commit the 2nd txn
    assertThrows(HoodieWriteConflictException.class, () -> {
      client2.commitStats(
          insertTime2,
          context().parallelize(writeStatuses2, 1),
          writeStatuses2.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(),
          metaClient.getCommitActionType());
    });
  }

  // case1: txn1 is upsert writer, txn2 is bulk_insert writer.
  //                       |----- txn1 ------|
  //      |--- txn2 ----|
  // both two txn would success to commit
  @Test
  public void testBulkInsertInSequence() throws Exception {
    HoodieWriteConfig config = createHoodieWriteConfig();
    metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, config.getProps());

    // start the 1st txn and bulk insert record: [id1,Danny,null,1,par1], commit the 1st txn
    SparkRDDWriteClient client1 = getHoodieWriteClient(config);
    List<String> dataset1 = Collections.singletonList("id1,Danny,,1,par1");
    String insertTime1 = client1.createNewInstantTime();
    writeData(client1, insertTime1, dataset1, true, WriteOperationType.BULK_INSERT);

    // start the 1st txn and insert record: [id1,null,23,2,par1], commit the 2nd txn
    SparkRDDWriteClient client2 = getHoodieWriteClient(config);
    List<String> dataset2 = Collections.singletonList("id1,,23,2,par1");
    String insertTime2 = client2.createNewInstantTime();
    writeData(client2, insertTime2, dataset2, true, WriteOperationType.INSERT);

    // do compaction
    String compactionTime = (String) client1.scheduleCompaction(Option.empty()).get();
    client1.compact(compactionTime);

    // result is [(id1,Danny,23,2,par1)]
    Map<String, String> result = Collections.singletonMap("par1", "[id1,par1,id1,Danny,23,2,par1]");
    checkWrittenData(result, 1);
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
        .withAutoCommit(false)
        .withPayloadConfig(
            HoodiePayloadConfig.newBuilder()
                .withPayloadClass(PartialUpdateAvroPayload.class.getName())
                .withPayloadOrderingField("ts")
                .build())
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
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL)
        // Timeline-server-based markers are not used for multi-writer tests
        .withMarkersType(MarkerType.DIRECT.name())
        .withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(InProcessLockProvider.class)
            .withConflictResolutionStrategy(new BucketIndexConcurrentFileWritesConflictResolutionStrategy())
            .build())
        .build();
  }

  private void checkWrittenData(
      Map<String, String> expected,
      int partitions) throws IOException {
    File baseFile = tempDir.toFile();
    assert baseFile.isDirectory();
    FileFilter filter = file -> !file.getName().startsWith(".");
    File[] partitionDirs = baseFile.listFiles(filter);
    assertNotNull(partitionDirs);
    assertThat(partitionDirs.length, is(partitions));
    for (File partitionDir : partitionDirs) {
      File[] dataFiles = partitionDir.listFiles(filter);
      assertNotNull(dataFiles);
      File latestDataFile = Arrays.stream(dataFiles)
          .max(Comparator.comparing(f -> FSUtils.getCommitTime(f.getName())))
          .orElse(dataFiles[0]);
      ParquetReader<GenericRecord> reader = AvroParquetReader
          .<GenericRecord>builder(new Path(latestDataFile.getAbsolutePath())).build();
      List<String> readBuffer = new ArrayList<>();
      GenericRecord nextRecord = reader.read();
      while (nextRecord != null) {
        readBuffer.add(filterOutVariables(nextRecord));
        nextRecord = reader.read();
      }
      readBuffer.sort(Comparator.naturalOrder());
      assertThat(readBuffer.toString(), is(expected.get(partitionDir.getName())));
    }
  }

  private static String filterOutVariables(GenericRecord genericRecord) {
    List<String> fields = new ArrayList<>();
    fields.add(getFieldValue(genericRecord, "_hoodie_record_key"));
    fields.add(getFieldValue(genericRecord, "_hoodie_partition_path"));
    fields.add(getFieldValue(genericRecord, "id"));
    fields.add(getFieldValue(genericRecord, "name"));
    fields.add(getFieldValue(genericRecord, "age"));
    fields.add(genericRecord.get("ts").toString());
    fields.add(genericRecord.get("part").toString());
    return String.join(",", fields);
  }

  private static String getFieldValue(GenericRecord genericRecord, String fieldName) {
    if (genericRecord.get(fieldName) != null) {
      return genericRecord.get(fieldName).toString();
    } else {
      return null;
    }
  }

  private boolean fileExists() {
    List<File> dirsToCheck = new ArrayList<>();
    dirsToCheck.add(tempDir.toFile());
    while (!dirsToCheck.isEmpty()) {
      File dir = dirsToCheck.remove(0);
      for (File file : Objects.requireNonNull(dir.listFiles())) {
        if (!file.getName().startsWith(".")) {
          if (file.isDirectory()) {
            dirsToCheck.add(file);
          } else {
            return true;
          }
        }
      }
    }
    return false;
  }

  private GenericRecord str2GenericRecord(String str) {
    GenericRecord record = new GenericData.Record(schema);
    String[] fieldValues = str.split(",");
    ValidationUtils.checkArgument(fieldValues.length == 5, "Valid record must have 5 fields");
    record.put("id", StringUtils.isNullOrEmpty(fieldValues[0]) ? null : fieldValues[0]);
    record.put("name", StringUtils.isNullOrEmpty(fieldValues[1]) ? null : fieldValues[1]);
    record.put("age", StringUtils.isNullOrEmpty(fieldValues[2]) ? null : Integer.parseInt(fieldValues[2]));
    record.put("ts", StringUtils.isNullOrEmpty(fieldValues[3]) ? null : Long.parseLong(fieldValues[3]));
    record.put("part", StringUtils.isNullOrEmpty(fieldValues[4]) ? null : fieldValues[4]);
    return record;
  }

  private List<HoodieRecord> str2HoodieRecord(List<String> records) {
    return records.stream().map(recordStr -> {
      GenericRecord record = str2GenericRecord(recordStr);
      PartialUpdateAvroPayload payload = new PartialUpdateAvroPayload(record, (Long) record.get("ts"));
      return new HoodieAvroRecord<>(new HoodieKey((String) record.get("id"), (String) record.get("part")), payload);
    }).collect(Collectors.toList());
  }

  private List<WriteStatus> writeData(
      SparkRDDWriteClient client,
      String instant,
      List<String> records,
      boolean doCommit,
      WriteOperationType operationType) {
    List<HoodieRecord> recordList = str2HoodieRecord(records);
    JavaRDD<HoodieRecord> writeRecords = jsc().parallelize(recordList, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    client.startCommitWithTime(instant);
    List<WriteStatus> writeStatuses;
    switch (operationType) {
      case INSERT:
        writeStatuses = client.insert(writeRecords, instant).collect();
        break;
      case UPSERT:
        writeStatuses = client.upsert(writeRecords, instant).collect();
        break;
      case BULK_INSERT:
        writeStatuses = client.bulkInsert(writeRecords, instant).collect();
        break;
      default:
        throw new UnsupportedOperationException(operationType + " is not supported yet in this test!");
    }
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatuses);
    if (doCommit) {
      List<HoodieWriteStat> writeStats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      boolean committed = client.commitStats(instant, context().parallelize(writeStatuses, 1), writeStats, Option.empty(), metaClient.getCommitActionType());
      Assertions.assertTrue(committed);
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatuses;
  }
}
