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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Handle release of a PDF file driven by {@link PreprocessingIterator} through the default PDF preprocessor: the file's scratch
 * dir (staged copy, PDFBox scratch) is gone when the iterator moves on to the next file, when it is exhausted, and when its
 * consumer closes it with the PDF half read. Outside Spark the staging root is {@code java.io.tmpdir}, which these tests point at
 * a temp dir of their own.
 */
public class TestPreprocessingIteratorPdfHandleRelease {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType BLOB_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, true)});

  @TempDir
  Path temp;
  private Path staging;
  private String savedTmpDir;
  private String embeddingTable;

  @BeforeAll
  public static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @BeforeEach
  public void setUp() throws IOException {
    // outside Spark the PDF staging root is java.io.tmpdir; a live SparkEnv may point it at a Spark local dir instead
    assumeTrue(SparkEnv.get() == null, "no SparkEnv: the PDF staging root is java.io.tmpdir");
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.MERGE_ON_READ).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
    staging = Files.createDirectory(temp.resolve("staging"));
    savedTmpDir = System.getProperty("java.io.tmpdir");
    System.setProperty("java.io.tmpdir", staging.toString());
  }

  @AfterEach
  public void restoreTmpDir() {
    System.setProperty("java.io.tmpdir", savedTmpDir);
  }

  @Test
  public void pdfHalfReadThenClosedLeavesNoScratch() throws IOException {
    PreprocessingIterator iterator = iterator(pdfRow("sha-a", PdfFixtures.prose(21L, 12, 60)));
    assertTrue(iterator.hasNext());
    iterator.next();
    assertFalse(pdfScratchDirs().isEmpty(), "the PDF is open mid-way");
    iterator.close();
    assertTrue(pdfScratchDirs().isEmpty(), "scratch left after the consumer closed a half-read PDF: " + pdfScratchDirs());
  }

  @Test
  public void pdfsReadToTheEndLeaveNoScratch() throws IOException {
    PreprocessingIterator iterator = iterator(pdfRow("sha-a", PdfFixtures.prose(22L, 12, 60)),
        pdfRow("sha-b", "%PDF-1.7 broken".getBytes(StandardCharsets.US_ASCII)), pdfRow("sha-c", PdfFixtures.prose(23L, 3, 30)));
    int rows = 0;
    boolean sawScratch = false;
    while (iterator.hasNext()) {
      iterator.next();
      rows++;
      sawScratch |= !pdfScratchDirs().isEmpty();
      assertTrue(pdfScratchDirs().size() <= 1, "at most the current file's scratch exists: " + pdfScratchDirs());
    }
    assertTrue(rows > 1, "rows of the two readable files");
    assertTrue(sawScratch, "a PDF scratch dir appeared under the staging root while a file was read");
    assertTrue(pdfScratchDirs().isEmpty(), "scratch left after the iterator was exhausted: " + pdfScratchDirs());
    iterator.close();
  }

  private PreprocessingIterator iterator(Row... rows) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    return new PreprocessingIterator(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props),
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), new PreprocessorRegistry(props),
        new WordCountTokenizer(),
        new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
            new LongAccumulator()),
        null);
  }

  private static Row pdfRow(String sha, byte[] pdf) {
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, pdf, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, "pdf", (long) pdf.length, 1234L, blob, "s3a://bucket/dir/" + sha + ".pdf",
        sha + ".pdf"}, BLOB_ROW);
  }

  private List<Path> pdfScratchDirs() throws IOException {
    try (Stream<Path> paths = Files.list(staging)) {
      return paths.filter(p -> p.getFileName().toString().startsWith("hudi-pdf-")).collect(Collectors.toList());
    }
  }
}
