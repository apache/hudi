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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.avro.AvroSchemaUtils.isSchemaCompatible;
import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.convertMetadataToByteArray;
import static org.apache.hudi.common.testutils.HoodieTestUtils.COMMIT_METADATA_SER_DE;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie commit metadata {@link HoodieCommitMetadata}.
 */
public class TestHoodieCommitMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHoodieCommitMetadata.class);

  private static final List<String> EXPECTED_FIELD_NAMES = Arrays.asList(
      "partitionToWriteStats", "compacted", "extraMetadata", "operationType");

  public static void verifyMetadataFieldNames(
      HoodieCommitMetadata commitMetadata, List<String> expectedFieldNameList)
      throws IOException {
    String serializedCommitMetadata = commitMetadata.toJsonString();
    List<String> actualFieldNameList = CollectionUtils.toStream(
            JsonUtils.getObjectMapper().readTree(serializedCommitMetadata).fieldNames())
        .collect(Collectors.toList());
    assertEquals(
        expectedFieldNameList.stream().sorted().collect(Collectors.toList()),
        actualFieldNameList.stream().sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void verifyFieldNamesInCommitMetadata() throws IOException {
    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(10);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    verifyMetadataFieldNames(commitMetadata, EXPECTED_FIELD_NAMES);
  }

  @Test
  public void testPerfStatPresenceInHoodieMetadata() throws Exception {

    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(100);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    assertTrue(commitMetadata.getTotalCreateTime() > 0);
    assertTrue(commitMetadata.getTotalUpsertTime() > 0);
    assertTrue(commitMetadata.getTotalScanTime() > 0);
    assertTrue(commitMetadata.getTotalLogFilesCompacted() > 0);

    String serializedCommitMetadata = commitMetadata.toJsonString();
    HoodieCommitMetadata metadata =
        HoodieCommitMetadata.fromJsonString(serializedCommitMetadata, HoodieCommitMetadata.class);
    assertTrue(commitMetadata.getTotalCreateTime() > 0);
    assertTrue(commitMetadata.getTotalUpsertTime() > 0);
    assertTrue(commitMetadata.getTotalScanTime() > 0);
    assertTrue(metadata.getTotalLogFilesCompacted() > 0);
  }

  @Test
  public void testCompatibilityWithoutOperationType() throws Exception {
    // test compatibility of old version file
    String serializedCommitMetadata =
        FileIOUtils.readAsUTFString(TestHoodieCommitMetadata.class.getResourceAsStream("/old-version.commit"));
    HoodieCommitMetadata metadata =
        HoodieCommitMetadata.fromJsonString(serializedCommitMetadata, HoodieCommitMetadata.class);
    assertSame(metadata.getOperationType(), WriteOperationType.UNKNOWN);

    // test operate type
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    assertSame(commitMetadata.getOperationType(), WriteOperationType.INSERT);

    // test serialized
    serializedCommitMetadata = commitMetadata.toJsonString();
    metadata =
        HoodieCommitMetadata.fromJsonString(serializedCommitMetadata, HoodieCommitMetadata.class);
    assertSame(metadata.getOperationType(), WriteOperationType.INSERT);
  }

  @Test
  public void testGetFileSliceForFileGroupFromDeltaCommit() throws IOException {
    org.apache.hudi.avro.model.HoodieCommitMetadata commitMetadata = new org.apache.hudi.avro.model.HoodieCommitMetadata();
    org.apache.hudi.avro.model.HoodieWriteStat writeStat1 = createWriteStat("111", "111base", Arrays.asList("1.log", "2.log"));
    org.apache.hudi.avro.model.HoodieWriteStat writeStat2 = createWriteStat("111", "111base", Arrays.asList("3.log", "4.log"));
    org.apache.hudi.avro.model.HoodieWriteStat writeStat3 = createWriteStat("222", null, Collections.singletonList("5.log"));
    Map<String, List<org.apache.hudi.avro.model.HoodieWriteStat>> partitionToWriteStatsMap = new HashMap<>();
    partitionToWriteStatsMap.put("partition1", Arrays.asList(writeStat2, writeStat3));
    partitionToWriteStatsMap.put("partition2", Collections.singletonList(writeStat1));
    commitMetadata.setPartitionToWriteStats(partitionToWriteStatsMap);
    byte[] serializedCommitMetadata = TimelineMetadataUtils.serializeAvroMetadata(
        commitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class).get();

    Option<Pair<String, List<String>>> result = HoodieCommitMetadata.getFileSliceForFileGroupFromDeltaCommit(
        new ByteArrayInputStream(serializedCommitMetadata), new HoodieFileGroupId("partition1", "111"));

    assertTrue(result.isPresent());
    assertEquals("111base", result.get().getKey());
    assertEquals(2, result.get().getValue().size());
    assertEquals("3.log", result.get().getValue().get(0));
    assertEquals("4.log", result.get().getValue().get(1));

    result = HoodieCommitMetadata.getFileSliceForFileGroupFromDeltaCommit(
        new ByteArrayInputStream(serializedCommitMetadata), new HoodieFileGroupId("partition1", "222"));
    assertTrue(result.isPresent());
    assertTrue(result.get().getKey().isEmpty());
    assertEquals(1, result.get().getValue().size());
    assertEquals("5.log", result.get().getValue().get(0));
  }

  @Test
  public void testCommitMetadataSerde() throws Exception {
    org.apache.hudi.avro.model.HoodieCommitMetadata commitMetadata = new org.apache.hudi.avro.model.HoodieCommitMetadata();
    org.apache.hudi.avro.model.HoodieWriteStat writeStat1 = createWriteStat("111", "111base", Arrays.asList("1.log", "2.log"));
    org.apache.hudi.avro.model.HoodieWriteStat writeStat2 = createWriteStat("222", "222base", Arrays.asList("3.log", "4.log"));
    org.apache.hudi.avro.model.HoodieWriteStat writeStat3 = createWriteStat("333", null, Collections.singletonList("5.log"));
    Map<String, List<org.apache.hudi.avro.model.HoodieWriteStat>> partitionToWriteStatsMap = new HashMap<>();
    partitionToWriteStatsMap.put("partition1", Arrays.asList(writeStat1, writeStat2));
    partitionToWriteStatsMap.put("partition2", Collections.singletonList(writeStat3));
    commitMetadata.setPartitionToWriteStats(partitionToWriteStatsMap);
    byte[] serializedCommitMetadata = TimelineMetadataUtils.serializeAvroMetadata(
        commitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class).get();
    // Case: Reading 1.x written commit metadata
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "1");
    org.apache.hudi.common.model.HoodieCommitMetadata commitMetadata1 =
        COMMIT_METADATA_SER_DE.deserialize(instant,
            new ByteArrayInputStream(serializedCommitMetadata), () -> false, org.apache.hudi.common.model.HoodieCommitMetadata.class);
    assertEquals(2, commitMetadata1.partitionToWriteStats.size());
    assertEquals(2, commitMetadata1.partitionToWriteStats.get("partition1").size());
    assertEquals(2, commitMetadata1.partitionToWriteStats.get("partition1").size());
    assertEquals("111", commitMetadata1.partitionToWriteStats.get("partition1").get(0).getFileId());
    assertEquals("222", commitMetadata1.partitionToWriteStats.get("partition1").get(1).getFileId());
    assertEquals("333", commitMetadata1.partitionToWriteStats.get("partition2").get(0).getFileId());

    // Case: Reading 0.x written commit metadata
    HoodieInstant legacyInstant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", "1", "1", true);
    CommitMetadataSerDe v1SerDe = new CommitMetadataSerDeV1();
    byte[] v1Bytes = convertMetadataToByteArray(commitMetadata1, v1SerDe);
    System.out.println(new String(v1Bytes));
    org.apache.hudi.common.model.HoodieCommitMetadata commitMetadata2 =
        COMMIT_METADATA_SER_DE.deserialize(legacyInstant, new ByteArrayInputStream(v1Bytes), () -> false, org.apache.hudi.common.model.HoodieCommitMetadata.class);
    assertEquals(2, commitMetadata2.partitionToWriteStats.size());
    assertEquals(2, commitMetadata2.partitionToWriteStats.get("partition1").size());
    assertEquals(2, commitMetadata2.partitionToWriteStats.get("partition1").size());
    System.out.println(commitMetadata2.partitionToWriteStats.get("partition1").get(0));
    assertEquals("111", commitMetadata2.partitionToWriteStats.get("partition1").get(0).getFileId());
    assertEquals("222", commitMetadata2.partitionToWriteStats.get("partition1").get(1).getFileId());
    assertEquals("333", commitMetadata2.partitionToWriteStats.get("partition2").get(0).getFileId());
  }

  private org.apache.hudi.avro.model.HoodieWriteStat createWriteStat(String fileId, String baseFile, List<String> logFiles) {
    org.apache.hudi.avro.model.HoodieWriteStat writeStat = new org.apache.hudi.avro.model.HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setBaseFile(baseFile);
    writeStat.setLogFiles(logFiles);
    return writeStat;
  }

  @Test
  public void testSchemaEqualityForHoodieCommitMetaData() {
    // Step 1: Get the schema from the Avro auto-generated class
    Schema avroSchema = org.apache.hudi.avro.model.HoodieCommitMetadata.SCHEMA$;

    // Step 2: Convert the POJO class to an Avro schema
    Schema pojoSchema = ReflectData.get().getSchema(org.apache.hudi.common.model.HoodieCommitMetadata.class);

    // Step 3: Validate schemas
    // We need to replace ENUM with STRING to workaround inherit type mismatch of java ENUM when coverted to avro object.
    assertTrue(isSchemaCompatible(replaceEnumWithString(pojoSchema), avroSchema, false, false));
  }

  @Test
  public void testSchemaEqualityForHoodieReplaceCommitMetaData() {
    // Step 1: Get the schema from the Avro auto-generated class
    Schema avroSchema = org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.SCHEMA$;

    // Step 2: Convert the POJO class to an Avro schema
    Schema pojoSchema = ReflectData.get().getSchema(org.apache.hudi.common.model.HoodieReplaceCommitMetadata.class);

    // Step 3: Validate schemas
    // We need to replace ENUM with STRING to workaround inherit type mismatch of java ENUM when coverted to avro object.
    assertTrue(isSchemaCompatible(replaceEnumWithString(pojoSchema), avroSchema, false, false));
  }

  // Utility method that search for all ENUM fields and replace it with STRING.
  private Schema replaceEnumWithString(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      return Schema.create(Schema.Type.STRING);
    } else if (schema.getType() == Schema.Type.RECORD) {
      List<Schema.Field> newFields = new ArrayList<>();
      for (Schema.Field field : schema.getFields()) {
        Schema newFieldSchema = replaceEnumWithString(field.schema());
        newFields.add(new Schema.Field(field.name(), newFieldSchema, field.doc(), field.defaultVal()));
      }
      return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, newFields);
    } else if (schema.getType() == Schema.Type.UNION) {
      List<Schema> types = new ArrayList<>();
      for (Schema type : schema.getTypes()) {
        types.add(replaceEnumWithString(type));
      }
      return Schema.createUnion(types);
    } else if (schema.getType() == Schema.Type.ARRAY) {
      return Schema.createArray(replaceEnumWithString(schema.getElementType()));
    } else if (schema.getType() == Schema.Type.MAP) {
      return Schema.createMap(replaceEnumWithString(schema.getValueType()));
    }
    return schema;
  }
}
