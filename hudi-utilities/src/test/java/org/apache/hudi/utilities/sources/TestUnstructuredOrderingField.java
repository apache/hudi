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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.concat_ws;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * An ordering field on the unstructured tables, as on any structured stream: {@code hoodie.table.ordering.fields} with
 * {@code EVENT_TIME_ORDERING}. Both record keys hold {@code source_modification_time}, so two writes share a key only when an
 * unchanged object is announced again; that round writes the same rows again, and the table keeps them as they were.
 */
@Tag("functional")
class TestUnstructuredOrderingField extends UtilitiesTestBase {

  private static final String ORDERING_FIELD = BlobTableSchema.SOURCE_MODIFICATION_TIME;

  @TempDir
  Path tempDir;

  @BeforeAll
  static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void cleanupClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  /** {@code cfg} with the ordering field and event-time ordering, as a structured stream sets them. */
  private static HoodieStreamer.Config withOrdering(HoodieStreamer.Config cfg, HoodieTableType tableType) {
    cfg.tableType = tableType.name();
    cfg.sourceOrderingFields = ORDERING_FIELD;
    Triple<RecordMergeMode, String, String> merging = HoodieTableConfig.inferMergingConfigsForWrites(
        RecordMergeMode.EVENT_TIME_ORDERING, null, null, ORDERING_FIELD, HoodieTableVersion.current());
    cfg.recordMergeMode = merging.getLeft();
    cfg.payloadClassName = merging.getMiddle();
    cfg.recordMergeStrategyId = merging.getRight();
    return cfg;
  }

  private static TypedProperties withOrdering(TypedProperties props) {
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), ORDERING_FIELD);
    props.setProperty("hoodie.write.record.merge.mode", RecordMergeMode.EVENT_TIME_ORDERING.name());
    return props;
  }

  private static void sync(HoodieStreamer.Config cfg, TypedProperties props) throws Exception {
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  /**
   * The table's rows without the commit metadata a later write of the same row changes, sorted. A blob row's out-of-line reference
   * names the copy of the round that wrote it, so of the blob only its storage type and length are compared.
   */
  private static List<String> rows(String table) {
    Dataset<Row> rows = sparkSession.read().format("hudi").load(table);
    List<Column> columns = Arrays.stream(rows.columns())
        .filter(name -> !HoodieRecord.HOODIE_META_COLUMNS.contains(name)).sorted()
        .map(name -> name.equals(BlobTableSchema.BLOB)
            ? concat_ws(":", new Column(name + ".type"), new Column(name + ".reference.length").cast("string"))
            : new Column(name))
        .collect(Collectors.toList());
    return rows.select(columns.toArray(new Column[0])).collectAsList().stream().map(Row::toString).sorted()
        .collect(Collectors.toList());
  }

  private static HoodieTableMetaClient metaClient(String table) {
    return HoodieTableMetaClient.builder().setBasePath(table).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build();
  }

  @ParameterizedTest(name = "{0} {1}")
  @CsvSource({"blob,COPY_ON_WRITE", "blob,MERGE_ON_READ", "embedding,COPY_ON_WRITE", "embedding,MERGE_ON_READ"})
  void reannouncedUnchangedObjectUnderEventTimeOrdering(String mode, HoodieTableType tableType) throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
    String table = tempDir.resolve(mode + "-table").toUri().toString();
    HoodieStreamer.Config cfg;
    TypedProperties props;
    S3EventsTableFixture.Event event;
    if ("blob".equals(mode)) {
      event = harness.put("docs/a.pdf", BlobJobHarness.fileBytes("a.pdf", 1024));
      cfg = BlobJobHarness.streamerConfig(table);
      props = harness.jobProperties(64);
    } else {
      event = harness.put("docs/a.pdf", PdfFixtures.text("Omicron pi rho sigma."));
      cfg = EmbeddingStreamers.config(table);
      props = EmbeddingStreamers.jobProps(table, harness.eventsPath(), BucketFolderFileSystem.SCHEME, 1024 * 1024,
          EmbeddingTableConfig.MODE_PREP_ONLY, new TypedProperties());
    }
    withOrdering(cfg, tableType);
    withOrdering(props);
    assertEquals(BucketFolderFileSystem.SCHEME, props.getString(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key()));

    harness.announce(sparkSession, Collections.singletonList(event));
    sync(cfg, props);
    List<String> first = rows(table);
    assertFalse(first.isEmpty());

    // announced again without touching the object: every row is written again under the same key and ordering value
    harness.announce(sparkSession, Collections.singletonList(event));
    sync(cfg, props);
    HoodieTableMetaClient metaClient = metaClient(table);
    assertEquals(2, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants(),
        "the second announcement is consumed");
    assertEquals(first, rows(table), "the rows are unchanged");
    assertEquals(Option.of(ORDERING_FIELD), metaClient.getTableConfig().getOrderingFieldsStr());
    assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig().getRecordMergeMode());
  }
}
