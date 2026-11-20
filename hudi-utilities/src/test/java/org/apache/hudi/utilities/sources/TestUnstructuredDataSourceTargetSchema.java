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

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.streamer.StreamSync;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A user's target schema on the blob job: checked against the blob table's contract when the source is built, and again at the
 * start of each round, before the round's instant.
 */
@Tag("functional")
class TestUnstructuredDataSourceTargetSchema extends UtilitiesTestBase {

  private static final long INLINE_MAX = 64;

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

  @AfterEach
  void resetSchema() {
    UserTargetSchemaProvider.target = null;
  }

  private BlobJobHarness harness() {
    return BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
  }

  private String table() {
    return tempDir.resolve("table").toUri().toString();
  }

  private static HoodieSchema schema(StructType struct) {
    return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(struct, "blob_record", "hoodie.blob");
  }

  private static StructType withExtra() {
    return BlobTableSchema.schema().add(DataTypes.createStructField("ingest_note", DataTypes.StringType, true));
  }

  private static StructType withoutBlob() {
    return new StructType(Arrays.stream(BlobTableSchema.schema().fields()).filter(f -> !f.name().equals(BlobTableSchema.BLOB))
        .toArray(StructField[]::new));
  }

  private static HoodieStreamer.Config config(String table, boolean withProvider) {
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(table);
    if (withProvider) {
      cfg.schemaProviderClassName = UserTargetSchemaProvider.class.getName();
    }
    return cfg;
  }

  private static void sync(HoodieStreamer.Config cfg, TypedProperties props) throws Exception {
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
  }

  private static List<HoodieInstant> instants(String table) {
    return HoodieTableMetaClient.builder().setBasePath(table).setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration())).build()
        .getActiveTimeline().getInstants();
  }

  @Test
  void userSchemaMissingBlobRefusedAtStart() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32));
    UserTargetSchemaProvider.target = schema(withoutBlob());
    HoodieStreamer.Config cfg = config(table(), true);
    TypedProperties jobProps = harness.jobProperties(INLINE_MAX);
    Exception failure = assertThrows(Exception.class, () -> sync(cfg, jobProps));
    String refusal = EmbeddingStreamers.causeOf(failure, HoodieSchemaException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
    assertTrue(refusal.startsWith("UnstructuredDataSource (mode blob) refuses the target schema: column blob must be "), refusal);
    assertTrue(refusal.endsWith("found: no such column"), refusal);
    // the source is built in the streamer's upstream place, after the table is initialised: the refusal leaves no instant
    assertTrue(instants(table()).isEmpty(), "nothing was written: no instant on the timeline");
  }

  @Test
  void compatibleUserSchemaWithExtraColumnStarts() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32));
    UserTargetSchemaProvider.target = schema(withExtra());
    sync(config(table(), true), harness.jobProperties(INLINE_MAX));
    assertEquals(1, instants(table()).size());
    assertTrue(Arrays.asList(sparkSession.read().format("hudi").load(table()).columns()).contains("ingest_note"));
  }

  @Test
  void providerSchemaChangedBeforeRound2RefusedBeforeInstant() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32));
    UserTargetSchemaProvider.target = schema(withExtra());
    TypedProperties props = harness.jobProperties(INLINE_MAX);
    // set by HoodieStreamer for the StreamSync it builds; this test builds its own to run two rounds on one
    props.setProperty("hoodie.datasource.write.reconcile.schema", "false");
    UserTargetSchemaProvider provider = new UserTargetSchemaProvider(props, jsc);
    try (StreamSync sync = new StreamSync(config(table(), true), sparkSession, props, new HoodieSparkEngineContext(jsc),
        HadoopFSUtils.getFs(table(), jsc.hadoopConfiguration()), jsc.hadoopConfiguration(), client -> true,
        new DefaultStreamContext(provider, Option.empty()))) {
      sync.syncOnce();
      List<HoodieInstant> afterFirst = instants(table());
      assertEquals(1, afterFirst.size());

      harness.putAndAnnounce(sparkSession, Collections.singletonMap("b.pdf", 32));
      UserTargetSchemaProvider.target = schema(withoutBlob());
      Exception failure = assertThrows(Exception.class, sync::syncOnce);
      String refusal = EmbeddingStreamers.causeOf(failure, HoodieSchemaException.class)
          .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure))).getMessage();
      assertTrue(refusal.contains("refuses the target schema: column blob"), refusal);
      assertEquals(afterFirst, instants(table()), "no instant was started for the refused round");
    }
  }

  @Test
  void noSchemaProviderStartsUnchanged() throws Exception {
    BlobJobHarness harness = harness();
    harness.putAndAnnounce(sparkSession, Collections.singletonMap("a.pdf", 32));
    sync(config(table(), false), harness.jobProperties(INLINE_MAX));
    assertEquals(1, instants(table()).size());
    assertFalse(Arrays.asList(sparkSession.read().format("hudi").load(table()).columns()).contains("ingest_note"));
  }

  /** A schema provider whose target schema the test sets, and changes between rounds. */
  public static class UserTargetSchemaProvider extends SchemaProvider {
    static volatile HoodieSchema target;

    public UserTargetSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      return target;
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      return target;
    }
  }
}
