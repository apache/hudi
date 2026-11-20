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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Spark SQL {@code read_blob()} over the blob table as the blob job writes it: a file above the inline threshold is an unmanaged
 * reference ({@code managed=false}) whose {@code external_path} is the source object's own URI, and reading it returns the source
 * object's bytes where they are; nothing is copied under the table.
 */
@Slf4j
@Tag("functional")
public class TestBlobTableReadBlob extends UtilitiesTestBase {

  @TempDir
  Path tempDir;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @ParameterizedTest
  @CsvSource({"COPY_ON_WRITE,8,false", "COPY_ON_WRITE,8192,false", "MERGE_ON_READ,8,false", "MERGE_ON_READ,8192,false", "COPY_ON_WRITE,8192,true"})
  public void readBlobResolvesTheSourceReference(String tableType, int size, boolean recompute) throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir).install(jsc.hadoopConfiguration());
    String tableBasePath = basePath + "/blob_table_read_blob";
    Map<String, byte[]> written = harness.putAndAnnounce(sparkSession, Collections.singletonMap("docs/big.pdf", size));
    byte[] source = written.get("docs/big.pdf");
    HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(tableBasePath);
    cfg.tableType = tableType;
    TypedProperties props = harness.jobProperties(16);
    if (recompute) {
      cfg.transformerClassNames = Collections.singletonList(RecomputingBlobQuarantineTransformer.class.getName());
      BlobJobHarness.withErrorTable(props, tempDir.resolve("errors").toString());
    }
    HoodieStreamer streamer = new HoodieStreamer(cfg, jsc, Option.of(props));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }

    sparkSession.read().format("hudi").load(tableBasePath).createOrReplaceTempView("blob_table_read_blob");
    Row stored = sparkSession.sql("select blob.type as t, blob.reference.external_path as p, blob.reference.managed as m, "
        + "blob.reference.offset as o, blob.reference.length as l "
        + "from blob_table_read_blob").first();
    if (size > 16) {
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE, stored.getString(0));
      assertEquals(BlobJobHarness.sourcePath("docs/big.pdf"), stored.getString(1));
      assertFalse(stored.getBoolean(2));
      assertNull(stored.get(3));
      assertNull(stored.get(4));
    } else {
      assertEquals(HoodieSchema.Blob.INLINE, stored.getString(0));
    }

    String readerConf = "spark.sql.parquet.enableVectorizedReader";
    boolean wasSet = sparkSession.conf().contains(readerConf);
    String before = sparkSession.conf().get(readerConf);
    byte[] vectorized;
    byte[] rowBased;
    try {
      sparkSession.conf().set(readerConf, "true");
      vectorized = readBytes(source, true);
      sparkSession.conf().set(readerConf, "false");
      rowBased = readBytes(source, false);
    } finally {
      if (wasSet) {
        sparkSession.conf().set(readerConf, before);
      } else {
        sparkSession.conf().unset(readerConf);
      }
    }
    assertArrayEquals(vectorized, rowBased, "both reader choices return the same source bytes");
    assertFalse(Files.exists(Paths.get(URI.create(tableBasePath)).resolve(".hoodie/.blob")), "nothing is copied under the table");
    log.info("READ-BLOB table type={} read {} bytes equal to the source", tableType, source.length);
  }

  /**
   * {@code read_blob()} resolves unmanaged source references through an alias, a {@code UNION ALL} of the same table, a self-join
   * and a join of two blob tables whose blob columns share the name {@code blob}: each read returns the bytes of its own side.
   */
  @Test
  public void readBlobResolvesSourceReferencesThroughAliasUnionAndJoins() throws Exception {
    byte[] first = syncOneObject("first", "blob_table_first", "docs/first.pdf", 8192);
    byte[] second = syncOneObject("second", "blob_table_second", "docs/second.pdf", 4096);

    Row aliased = sparkSession.sql("select read_blob(b) as data from (select blob as b from blob_table_first)").first();
    assertArrayEquals(first, (byte[]) aliased.getAs("data"), "aliased blob column");

    List<Row> union = sparkSession.sql("select read_blob(blob) as data from "
        + "(select blob from blob_table_first union all select blob from blob_table_first)").collectAsList();
    assertEquals(2, union.size());
    for (Row row : union) {
      assertArrayEquals(first, (byte[]) row.getAs("data"), "union of the same table");
    }

    Row selfJoined = sparkSession.sql("select read_blob(x.blob) as left_data, read_blob(y.blob) as right_data "
        + "from blob_table_first x join blob_table_first y on x.source_path_sha = y.source_path_sha").first();
    assertArrayEquals(first, (byte[]) selfJoined.getAs("left_data"), "self-join, left side");
    assertArrayEquals(first, (byte[]) selfJoined.getAs("right_data"), "self-join, right side");

    Row joined = sparkSession.sql("select read_blob(x.blob) as first_data, read_blob(y.blob) as second_data "
        + "from blob_table_first x cross join blob_table_second y").first();
    assertArrayEquals(first, (byte[]) joined.getAs("first_data"), "join, first table's blob column");
    assertArrayEquals(second, (byte[]) joined.getAs("second_data"), "join, second table's blob column");
  }

  /**
   * Runs the blob job over one object into its own table, registers the table as a view, and returns the object's bytes. Every
   * table's objects share one objects root, which reads resolve through; each job has its own events table under {@code root}.
   */
  private byte[] syncOneObject(String root, String table, String key, int size) throws Exception {
    BlobJobHarness harness = new BlobJobHarness(tempDir.resolve("objects"), tempDir.resolve(root).resolve("events").toUri().toString())
        .install(jsc.hadoopConfiguration());
    String tablePath = basePath + "/" + table;
    byte[] bytes = harness.putAndAnnounce(sparkSession, Collections.singletonMap(key, size)).get(key);
    HoodieStreamer streamer = new HoodieStreamer(BlobJobHarness.streamerConfig(tablePath), jsc, Option.of(harness.jobProperties(16)));
    try {
      streamer.sync();
    } finally {
      streamer.shutdownGracefully();
    }
    sparkSession.read().format("hudi").load(tablePath).createOrReplaceTempView(table);
    Row stored = sparkSession.sql("select blob.type as t, blob.reference.external_path as p from " + table).first();
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, stored.getString(0));
    assertEquals(BlobJobHarness.sourcePath(key), stored.getString(1));
    return bytes;
  }

  private byte[] readBytes(byte[] source, boolean vectorized) {
    // Create a fresh Hudi relation after each setting change so scan planning uses that reader choice.
    sparkSession.read().format("hudi").load(basePath + "/blob_table_read_blob").createOrReplaceTempView("blob_table_read_blob");
    Dataset<Row> query = sparkSession.sql("select source_path_sha, read_blob(blob) as data from blob_table_read_blob");
    Row read = query.first();
    System.out.println("READ-BLOB requestedVectorized=" + vectorized
        + " effectiveVectorized=" + sparkSession.conf().get("spark.sql.parquet.enableVectorizedReader")
        + " nestedVectorized=" + sparkSession.conf().get("spark.sql.parquet.enableNestedColumnVectorizedReader"));
    byte[] bytes = read.getAs("data");
    assertArrayEquals(source, bytes);
    assertEquals(BlobFileNames.sourcePathSha(BlobJobHarness.sourcePath("docs/big.pdf")), read.getAs("source_path_sha"));
    return bytes;
  }

  /** Discards a materialized partition once, then materializes and returns its recomputed rows. */
  public static class RecomputingBlobQuarantineTransformer extends BlobQuarantineTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props) {
      Dataset<Row> output = super.apply(jsc, spark, rows, props);
      output.unpersist(true);
      output.persist(StorageLevel.DISK_ONLY());
      output.count();
      return output;
    }
  }
}
