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
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.streamer.ErrorTableUtils;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real eager transformer of the blob source: gone-row routing to the error table and the output cache lifecycle. */
@Tag("functional")
public class TestBlobQuarantineTransformer extends UtilitiesTestBase {
  private static final String CORRUPT_RECORD = BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;

  @BeforeAll
  public static void startServices() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void stopServices() throws Exception {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  private TypedProperties properties(boolean errorTable) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.recordkey.field", KEY_FIELDS);
    props.setProperty("hoodie.datasource.write.partitionpath.field", BlobTableSchema.SOURCE_FILE_EXTENSION);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), Boolean.toString(errorTable));
    return props;
  }

  private static final String KEY_FIELDS = BlobTableSchema.SOURCE_PATH_SHA + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME + ","
      + BlobTableSchema.SOURCE_FILE_SIZE;

  /** The record key the complex key generator builds for {@code row} (a schemaless RowFactory row: columns by schema index). */
  private static String recordKey(Row row) {
    StructType schema = BlobTableSchema.schema();
    String sourcePath = row.getString(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_PATH));
    return BlobTableSchema.SOURCE_PATH_SHA + ":" + BlobFileNames.sourcePathSha(sourcePath) + "," + BlobTableSchema.SOURCE_MODIFICATION_TIME
        + ":" + row.getLong(schema.fieldIndex(BlobTableSchema.SOURCE_MODIFICATION_TIME)) + "," + BlobTableSchema.SOURCE_FILE_SIZE
        + ":" + row.getLong(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_SIZE));
  }

  /** A blob row whose {@code source_file_name} is {@code tag}, so a test can tell its rows apart. */
  private static Row row(String source, String tag, Row blob, long length) {
    String sha = BlobFileNames.sourcePathSha(source);
    return RowFactory.create(sha, 1234L, source, tag, "pdf", length, "application/pdf", blob);
  }

  private Dataset<Row> rows(Row... rows) {
    return sparkSession.createDataFrame(Arrays.asList(rows), BlobTableSchema.schema());
  }

  /** Inline bytes and an unmanaged reference pass unchanged; the output is materialized and released by the next round. */
  @Test
  public void passesInlineAndReferencedRowsUnchangedAndReleasesThePriorOutput() {
    byte[] bytes = new byte[] {1, 2, 3, 4};
    Row inline = row("file:///inline.pdf", "inline", RowFactory.create(HoodieSchema.Blob.INLINE, bytes, null), 4);
    Row referenced = row("s3a://landing/large.pdf", "referenced", RowFactory.create(HoodieSchema.Blob.OUT_OF_LINE, null,
        RowFactory.create("s3a://landing/large.pdf", 0L, 4096L, false)), 4096);
    BlobQuarantineTransformer transformer = new BlobQuarantineTransformer();
    Dataset<Row> output = transformer.apply(jsc, sparkSession, rows(inline, referenced), properties(true));
    int tagColumn = BlobTableSchema.schema().fieldIndex(BlobTableSchema.SOURCE_FILE_NAME);
    int blobColumn = BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB);
    Map<String, Row> byTag = output.collectAsList().stream().collect(Collectors.toMap(r -> r.getString(tagColumn), r -> r));
    assertEquals(2, byTag.size());
    assertArrayEquals(bytes, (byte[]) byTag.get("inline").getStruct(blobColumn).get(1));
    assertEquals(referenced.getStruct(blobColumn), byTag.get("referenced").getStruct(blobColumn));
    for (Row result : byTag.values()) {
      assertNull(result.getAs(CORRUPT_RECORD));
    }
    assertTrue(output.storageLevel().useMemory() || output.storageLevel().useDisk());
    transformer.apply(jsc, sparkSession, rows(), properties(true));
    assertEquals(StorageLevel.NONE(), output.storageLevel(), "the following round releases the prior output");
  }

  @Test
  public void goneRowKeepsJsonIdentityAndOnePreAddedCorruptColumn() throws Exception {
    Row missingBytes = row("file:///gone.pdf", "gone", null, 10);
    Row inline = row("file:///inline.pdf", "inline", RowFactory.create(HoodieSchema.Blob.INLINE, new byte[] {9}, null), 1);
    Dataset<Row> input = rows(missingBytes, inline).withColumn(CORRUPT_RECORD,
        functions.when(functions.col(BlobTableSchema.SOURCE_FILE_NAME).equalTo("inline"), functions.lit("earlier-transformer-error")));
    BlobQuarantineTransformer transformer = new BlobQuarantineTransformer();
    Dataset<Row> output = transformer.apply(jsc, sparkSession, input, properties(true));
    assertEquals(1, Collections.frequency(Arrays.asList(output.columns()), CORRUPT_RECORD));
    List<Row> result = output.orderBy(BlobTableSchema.SOURCE_FILE_NAME).collectAsList();
    JsonNode identity = new ObjectMapper().readTree(result.get(0).getAs(CORRUPT_RECORD).toString());
    assertEquals(recordKey(missingBytes), identity.get("record_key").asText());
    assertEquals("pdf", identity.get("partition_path").asText());
    assertEquals(BlobTableSchema.GONE_SINCE_ANNOUNCED + ": no bytes were read when the row was prepared: source_path_sha "
        + BlobFileNames.sourcePathSha("file:///gone.pdf") + " file:///gone.pdf 10", identity.get("error").asText());
    assertEquals("earlier-transformer-error", result.get(1).getAs(CORRUPT_RECORD));
    transformer.apply(jsc, sparkSession, rows(), properties(true));
  }

  @Test
  public void goneRowKeepsJsonIdentityWithUntypedPreAddedCorruptColumn() throws Exception {
    Row missingBytes = row("file:///gone.pdf", "gone", null, 10);
    Dataset<Row> input = ErrorTableUtils.addNullValueErrorTableCorruptRecordColumn(rows(missingBytes));
    assertEquals(DataTypes.NullType, input.schema().apply(CORRUPT_RECORD).dataType(), "the actual chain pre-adds an untyped null");
    BlobQuarantineTransformer transformer = new BlobQuarantineTransformer();
    try {
      Dataset<Row> output = transformer.apply(jsc, sparkSession, input, properties(true));
      assertEquals(DataTypes.StringType, output.schema().apply(CORRUPT_RECORD).dataType(), "the identity JSON requires a string field");
      assertEquals(1, Collections.frequency(Arrays.asList(output.columns()), CORRUPT_RECORD));
      List<Row> result = output.collectAsList();
      assertEquals(1, result.size());
      JsonNode identity = new ObjectMapper().readTree(result.get(0).getAs(CORRUPT_RECORD).toString());
      assertEquals(recordKey(missingBytes), identity.get("record_key").asText());
      assertEquals("pdf", identity.get("partition_path").asText());
      assertTrue(identity.get("error").asText().startsWith(BlobTableSchema.GONE_SINCE_ANNOUNCED), identity.toString());
    } finally {
      transformer.apply(jsc, sparkSession, rows(), properties(true));
    }
  }

  @Test
  public void goneRowWithoutErrorWriterThrowsInsideApply() {
    BlobQuarantineTransformer transformer = new BlobQuarantineTransformer();
    RuntimeException failure = assertThrows(RuntimeException.class,
        () -> transformer.apply(jsc, sparkSession, rows(row("file:///gone.pdf", "gone", null, 10)), properties(false)));
    assertTrue(failure.getMessage().contains("without an error table"), failure.getMessage());
    assertTrue(failure.getMessage().contains(BlobTableSchema.GONE_SINCE_ANNOUNCED), failure.getMessage());
  }

  /**
   * A task that fails while the output is materialized (here the key generator, on a record key field the rows lack) surfaces as
   * Spark's checked SparkException, not a RuntimeException; the output persisted for the round is released all the same.
   */
  @Test
  public void failedTaskReleasesTheRoundsOutput() {
    TypedProperties props = properties(true);
    props.setProperty("hoodie.datasource.write.recordkey.field", "no_such_column");
    int persistedBefore = jsc.getPersistentRDDs().size();
    BlobQuarantineTransformer transformer = new BlobQuarantineTransformer();
    Exception failure = assertThrows(Exception.class,
        () -> transformer.apply(jsc, sparkSession, rows(row("file:///gone.pdf", "gone", null, 10)), props));
    assertTrue(failure instanceof SparkException, "a failed task surfaces as SparkException: " + failure);
    assertEquals(persistedBefore, jsc.getPersistentRDDs().size(), "the failed round's output is unpersisted");
  }

  @Test
  public void passedRowsWithoutErrorWriterKeepSourceSchema() {
    Row inline = row("file:///inline.pdf", "inline", RowFactory.create(HoodieSchema.Blob.INLINE, new byte[] {9}, null), 1);
    BlobQuarantineTransformer transformer = new BlobQuarantineTransformer();
    Dataset<Row> output = transformer.apply(jsc, sparkSession, rows(inline), properties(false));
    assertEquals(BlobTableSchema.schema(), output.schema());
    assertEquals(Collections.singletonList(inline), output.collectAsList());
    transformer.apply(jsc, sparkSession, rows(), properties(false));
  }
}
