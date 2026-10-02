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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.internal.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link InternalSchemaHistory}.
 */
class TestInternalSchemaHistory extends HoodieCommonTestHarness {

  // 50 predates the active timeline, 150 and 250 are not commits, 500 is not a valid commit
  private static final List<Long> VERSION_IDS = Arrays.asList(50L, 100L, 150L, 200L, 250L, 300L, 400L, 450L, 500L);

  private static final InternalSchema SCHEMA_BEFORE_EVOLUTION = schema(-1,
      Types.Field.get(0, false, "id", Types.IntType.get()),
      Types.Field.get(1, true, "name", Types.StringType.get()));
  private static final InternalSchema RENAMED_SCHEMA = schema(200,
      Types.Field.get(0, false, "id", Types.IntType.get()),
      Types.Field.get(1, true, "fullname", Types.StringType.get()));
  private static final InternalSchema ADDED_COLUMN_SCHEMA = schema(400,
      Types.Field.get(0, false, "id", Types.IntType.get()),
      Types.Field.get(1, true, "fullname", Types.StringType.get()),
      Types.Field.get(2, true, "qty", Types.LongType.get()));

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testResolvesSchemasLikeTimelineLookup(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);
    InternalSchemaHistory history = InternalSchemaHistory.load(metaClient, validCommits);

    for (long versionId : VERSION_IDS) {
      assertEquals(timelineLookup(versionId, validCommits), history.getSchemaByVersionId(versionId), "version " + versionId);
    }
    assertTrue(history.getSchemaByVersionId(50L).isEmptySchema());
    assertEquals(Arrays.asList("id", "name"), dataFieldNames(history.getSchemaByVersionId(100L)));
    assertEquals(RENAMED_SCHEMA, history.getSchemaByVersionId(300L));
    assertEquals(ADDED_COLUMN_SCHEMA, history.getSchemaByVersionId(450L));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFallbackLookupFindsCommitFileWithTableTimelineLayout(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);

    // the file of commit 100 predates the schema history, so only its commit file knows its schema
    InternalSchema fileSchema = InternalSchemaCache.getInternalSchemaByVersionId(
        100L, basePath, metaClient.getStorage(), validCommits);
    assertEquals(Arrays.asList("id", "name"), dataFieldNames(fileSchema));
    assertEquals(timelineLookup(100L, validCommits), fileSchema);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testShippedHistoryResolvesWithoutTableMetadata(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);
    InternalSchemaHistory history = InternalSchemaHistory.load(metaClient, validCommits);
    Map<String, InternalSchema> expected = resolveAll(history);

    Map<String, String> configs = history.toConfigs();
    assertTrue(configs.keySet().stream().allMatch(InternalSchemaHistory::isConfigKey));
    assertTrue(InternalSchemaHistory.isPresentIn(configs::get));
    assertFalse(InternalSchemaHistory.isPresentIn(key -> null));
    InternalSchemaHistory deserialized = javaRoundTrip(history);
    metaClient.getStorage().deleteDirectory(new StoragePath(basePath, ".hoodie"));

    assertEquals(expected, VERSION_IDS.stream().collect(
        Collectors.toMap(String::valueOf, versionId -> InternalSchemaHistory.resolve(configs::get, versionId))));
    assertEquals(expected, resolveAll(deserialized));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testResolveReadsOnlyTheNeededSchema(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);
    Map<String, String> configs = InternalSchemaHistory.load(metaClient, validCommits).toConfigs();

    List<String> readKeys = new ArrayList<>();
    InternalSchema fileSchema = InternalSchemaHistory.resolve(key -> {
      readKeys.add(key);
      return configs.get(key);
    }, 300L);

    assertEquals(RENAMED_SCHEMA, fileSchema);
    assertEquals(1, readKeys.stream().filter(key -> configs.get(key).startsWith("{")).count(), "schemas read: " + readKeys);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMissingCommitFileResolvesLikeTimelineLookup(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);
    // the commit file of 100 is archived after the query loaded the timeline
    String commitFileName = validCommits.split(",")[0];
    assertTrue(commitFileName.startsWith("100"));
    assertTrue(metaClient.getStorage().deleteFile(new StoragePath(metaClient.getTimelinePath(), commitFileName)));

    InternalSchemaHistory history = InternalSchemaHistory.load(metaClient, validCommits);

    for (long versionId : VERSION_IDS) {
      assertEquals(timelineLookup(versionId, validCommits), history.getSchemaByVersionId(versionId), "version " + versionId);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUnparsableCommitFileIsReadAgain(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);
    StoragePath commitFile = new StoragePath(metaClient.getTimelinePath(), validCommits.split(",")[0]);
    byte[] content = InternalSchemaCache.readCommitFile(metaClient.getStorage(), commitFile);

    writeFile(commitFile, "not commit metadata".getBytes(StandardCharsets.UTF_8));
    assertTrue(InternalSchemaHistory.load(metaClient, validCommits).getSchemaByVersionId(100L).isEmptySchema());
    writeFile(commitFile, content);

    assertEquals(Arrays.asList("id", "name"),
        dataFieldNames(InternalSchemaHistory.load(metaClient, validCommits).getSchemaByVersionId(100L)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCommitSchemasAreShippedOncePerDistinctSchema(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8);
    String avroSchema = InternalSchemaConverter.convert(SCHEMA_BEFORE_EVOLUTION, "record").toString();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    for (String commitTime : Arrays.asList("100", "110", "120")) {
      testTable.addCommit(commitTime, Option.of(commitMetadata(avroSchema, null)));
    }
    new FileBasedInternalSchemaStorageManager(metaClient).persistHistorySchemaStr("200", SerDeHelper.inheritSchemas(RENAMED_SCHEMA, ""));
    testTable.addCommit("200", Option.of(commitMetadata(avroSchema, RENAMED_SCHEMA)));
    metaClient.reloadActiveTimeline();
    InstantFileNameGenerator fileNameGenerator = metaClient.getInstantFileNameGenerator();
    String validCommits = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(fileNameGenerator::getFileName).collect(Collectors.joining(","));

    InternalSchemaHistory history = InternalSchemaHistory.load(metaClient, validCommits);

    // one entry for the shared pre-history schema and one for the history version
    assertEquals(2, history.toConfigs().values().stream().filter(value -> value.startsWith("{")).count());
    for (long versionId : Arrays.asList(100L, 110L, 120L, 200L)) {
      assertEquals(timelineLookup(versionId, validCommits), history.getSchemaByVersionId(versionId), "version " + versionId);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCommitFilesAreReadOncePerTable(boolean preTableVersion8) throws Exception {
    String validCommits = prepareEvolvedTable(preTableVersion8);
    Map<String, String> configs = InternalSchemaHistory.load(metaClient, validCommits).toConfigs();

    for (String commitFileName : validCommits.split(",")) {
      assertTrue(metaClient.getStorage().deleteFile(new StoragePath(metaClient.getTimelinePath(), commitFileName)));
    }

    assertEquals(configs, InternalSchemaHistory.load(metaClient, validCommits).toConfigs());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCommitCompletedBeforeEarlierSchemaChangeKeepsItsOwnSchema(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8);
    InternalSchema initialSchema = schema(50,
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "name", Types.StringType.get()));
    InternalSchema renamedSchema = schema(100,
        Types.Field.get(0, false, "id", Types.IntType.get()),
        Types.Field.get(1, true, "fullname", Types.StringType.get()));
    String avroSchema = InternalSchemaConverter.convert(initialSchema, "record").toString();
    String initialHistory = SerDeHelper.inheritSchemas(initialSchema, "");
    String renamedHistory = SerDeHelper.inheritSchemas(renamedSchema, initialHistory);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    FileBasedInternalSchemaStorageManager schemaManager = new FileBasedInternalSchemaStorageManager(metaClient);
    schemaManager.persistHistorySchemaStr("50", initialHistory);
    testTable.addCommit("50", Option.of(commitMetadata(avroSchema, initialSchema)));
    // the rename requested at 100 is still running when the writer of 101 commits with the initial schema
    schemaManager.persistHistorySchemaStr("101", initialHistory);
    testTable.addCommit("101", Option.of(commitMetadata(avroSchema, initialSchema)));
    schemaManager.persistHistorySchemaStr("100", renamedHistory);
    testTable.addCommit("100", Option.of(commitMetadata(avroSchema, renamedSchema)));
    schemaManager.persistHistorySchemaStr("102", renamedHistory);
    testTable.addCommit("102", Option.of(commitMetadata(avroSchema, renamedSchema)));
    metaClient.reloadActiveTimeline();
    InstantFileNameGenerator fileNameGenerator = metaClient.getInstantFileNameGenerator();
    String validCommits = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(fileNameGenerator::getFileName).collect(Collectors.joining(","));

    InternalSchemaHistory history = InternalSchemaHistory.load(metaClient, validCommits);

    assertEquals(initialSchema, history.getSchemaByVersionId(101L));
    for (long versionId : Arrays.asList(50L, 100L, 101L, 102L, 103L)) {
      assertEquals(timelineLookup(versionId, validCommits), history.getSchemaByVersionId(versionId), "version " + versionId);
    }
  }

  /**
   * Commit 100 predates schema evolution; 200 renames a column and starts the schema history, 300 keeps the
   * schema and 400 adds a column; 500 is inflight. Returns the completed commits as instant file names.
   */
  private String prepareEvolvedTable(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8);
    String avroSchema = InternalSchemaConverter.convert(SCHEMA_BEFORE_EVOLUTION, "record").toString();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    testTable.addCommit("100", Option.of(commitMetadata(avroSchema, null)));

    FileBasedInternalSchemaStorageManager schemaManager = new FileBasedInternalSchemaStorageManager(metaClient);
    String history = SerDeHelper.inheritSchemas(RENAMED_SCHEMA, "");
    schemaManager.persistHistorySchemaStr("200", history);
    testTable.addCommit("200", Option.of(commitMetadata(avroSchema, RENAMED_SCHEMA)));
    schemaManager.persistHistorySchemaStr("300", history);
    testTable.addCommit("300", Option.of(commitMetadata(avroSchema, RENAMED_SCHEMA)));
    schemaManager.persistHistorySchemaStr("400", SerDeHelper.inheritSchemas(ADDED_COLUMN_SCHEMA, history));
    testTable.addCommit("400", Option.of(commitMetadata(avroSchema, ADDED_COLUMN_SCHEMA)));
    testTable.addInflightCommit("500");

    metaClient.reloadActiveTimeline();
    InstantFileNameGenerator fileNameGenerator = metaClient.getInstantFileNameGenerator();
    return metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants().getInstantsAsStream()
        .map(fileNameGenerator::getFileName).collect(Collectors.joining(","));
  }

  private void writeFile(StoragePath path, byte[] content) throws Exception {
    try (OutputStream out = metaClient.getStorage().create(path, true)) {
      out.write(content);
    }
  }

  private InternalSchema timelineLookup(long versionId, String validCommits) {
    return InternalSchemaCache.getInternalSchemaByVersionId(versionId, basePath, metaClient.getStorage(), validCommits,
        metaClient.getTimelineLayout(), metaClient.getTableConfig());
  }

  private static InternalSchemaHistory javaRoundTrip(InternalSchemaHistory history) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(history);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (InternalSchemaHistory) in.readObject();
    }
  }

  private static Map<String, InternalSchema> resolveAll(InternalSchemaHistory history) {
    return VERSION_IDS.stream().collect(Collectors.toMap(String::valueOf, history::getSchemaByVersionId));
  }

  private static HoodieCommitMetadata commitMetadata(String avroSchema, InternalSchema latestSchema) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, avroSchema);
    if (latestSchema != null) {
      metadata.addMetadata(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(latestSchema));
    }
    return metadata;
  }

  private static List<String> dataFieldNames(InternalSchema schema) {
    return schema.getRecord().fields().stream().map(Types.Field::name)
        .filter(name -> !HoodieRecord.HOODIE_META_COLUMNS.contains(name)).collect(Collectors.toList());
  }

  private static InternalSchema schema(long versionId, Types.Field... fields) {
    InternalSchema schema = new InternalSchema(Types.RecordType.get(fields));
    schema.setSchemaId(versionId);
    return schema;
  }
}
