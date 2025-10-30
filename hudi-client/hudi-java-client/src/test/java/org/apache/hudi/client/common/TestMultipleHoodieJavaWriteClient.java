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

package org.apache.hudi.client.common;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;

/**
 * Tests concurrent Java client writers.
 */
public class TestMultipleHoodieJavaWriteClient {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestMultipleHoodieJavaWriteClient.class.getName());

  @TempDir
  protected java.nio.file.Path tablePath;

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testOccWithMultipleWriters(HoodieTableType tableType) throws IOException {
    StorageConfiguration<Configuration> storageConf = getDefaultStorageConf();
    final String tableTypeName = tableType.name();
    final String tableName = "hudiTestTable";
    final String basePath = tablePath.toAbsolutePath().toString() + "/" + tableTypeName;

    HoodieTableMetaClient.newTableBuilder()
        .setTableType(tableTypeName)
        .setTableName(tableName)
        .setPayloadClassName(HoodieAvroPayload.class.getName())
        .setRecordKeyFields("ph")
        .setPartitionFields("id,name")
        .setKeyGeneratorType(KeyGeneratorType.COMPLEX.name())
        .initTable(storageConf, basePath);

    final Schema schema =
        SchemaBuilder.record("user")
            .doc("user")
            .namespace("example.avro")
            .fields()
            .name("id")
            .doc("Unique identifier")
            .type()
            .intType()
            .noDefault()
            .name("name")
            .doc("Nome")
            .type()
            .nullable()
            .stringType()
            .stringDefault("Unknown")
            .name("favorite_number")
            .doc("number")
            .type()
            .nullable()
            .intType()
            .intDefault(0)
            .name("favorite_color")
            .doc("color")
            .type()
            .nullable()
            .stringType()
            .stringDefault("Unknown")
            .name("ph")
            .doc("mobile")
            .type()
            .nullable()
            .intType()
            .noDefault()
            .name("entryTs")
            .doc("tiebreaker on duplicates")
            .type()
            .nullable()
            .longType()
            .noDefault()
            .endRecord();

    // The below means we create 20 records with keys ranging from integer values 0-10 to
    // mimic contention. i.e: Each key will be repeated 2 times.
    int recordCountToProduce = 20;
    int range = 10;

    ConcurrentLinkedQueue<HoodieAvroRecord<HoodieAvroPayload>> hoodieAvroRecords = new ConcurrentLinkedQueue<>();
    IntStream intStream = IntStream.range(0, recordCountToProduce);
    intStream
        .parallel()
        .forEach(
            i -> {
              try {
                final GenericRecord user = new GenericData.Record(schema);
                user.put("id", i % range);
                user.put("name", "test");
                HoodieAvroPayload record = new HoodieAvroPayload(user, 0);
                final HoodieAvroRecord<HoodieAvroPayload> hoodieAvroRecord =
                    new HoodieAvroRecord<>(
                        new HoodieKey(user.get("id").toString(), createPartitionPath(user)),
                        record);
                hoodieAvroRecords.add(hoodieAvroRecord);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });


    HoodieWriteConfig cfg =
        HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.JAVA)
            .withPath(basePath)
            .withSchema(schema.toString())
            .forTable(tableName)
            .withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.MEMORY).build())
            .withLockConfig(
                HoodieLockConfig.newBuilder()
                    .withLockProvider(FileSystemBasedLockProvider.class)
                    .withClientNumRetries(240)
                    .withNumRetries(240)
                    .withClientRetryWaitTimeInMillis(1000L)
                    .build())
            .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                .build())
            .build();

    BlockingQueue<HoodieJavaWriteClient> writerQueue = new LinkedBlockingQueue<>();

    // Number of writers to mock a distributed setup
    int numHudiWriteClients = 3;
    IntStream.range(0, numHudiWriteClients).forEach(i -> {
      HoodieJavaWriteClient writer =
          new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(storageConf), cfg, true);
      try {
        writerQueue.put(writer);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    hoodieAvroRecords.stream()
        .parallel()
        .forEach(
            record -> {
              LOGGER.info("Inserting record = {}", record);
              HoodieJavaWriteClient writer;
              LOGGER.info("Entering while loop");
              try {
                writer = writerQueue.take();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              String startCommitTime = writer.startCommit();
              List<WriteStatus> s = writer.upsert(Collections.singletonList(record), startCommitTime);

              writer.commit(startCommitTime, s);
              LOGGER.info("Completed commit");
              synchronized (writerQueue) {
                try {
                  writerQueue.put(writer);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            }
        );
  }

  private String createPartitionPath(final GenericRecord user1) {
    return user1.get("id").toString() + "/" + user1.get("name");
  }
}
