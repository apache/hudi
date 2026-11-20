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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.sources.blob.BlobJobHarness;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.S3EventsTableFixture;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Shared setup of the embedding round tests: objects in a bucket under {@code basePath()} announced by an S3 events table
 * ({@link BlobJobHarness}), the embedding job ({@link EmbeddingStreamers}) over it, text objects prepared by
 * {@link TextPipePreprocessor}.
 */
public abstract class EmbeddingRoundTestBase extends SparkClientFunctionalTestHarness {

  /** Every object is inline in its source row unless a test says otherwise. */
  protected static final long INLINE_MAX_BYTES = 1L << 20;
  /**
   * The extension of the S3 events tests' text files: one the source selects, mapped to {@link TextPipePreprocessor} by
   * {@link #props}.
   */
  protected static final String TEXT_EXTENSION = "tif";

  protected String table() {
    return table("embedding_table");
  }

  protected String table(String name) {
    return basePath() + "/" + name;
  }

  protected StorageConfiguration<?> storage() {
    return EmbeddingStreamers.storageConf(spark());
  }

  /** The objects of the S3 events tests: buckets under {@code basePath()/source/objects}, announced by its events table. */
  protected BlobJobHarness objects() {
    return BlobJobHarness.under(localPath(basePath()).resolve("source"))
        .install(spark().sparkContext().hadoopConfiguration());
  }

  /** An object of the S3 events tests' bucket at {@code key}. */
  protected static FixtureFile object(String key, byte[] bytes) {
    return new FixtureFile(BlobJobHarness.sourcePath(key), bytes);
  }

  /** A text object (extension {@value #TEXT_EXTENSION}) whose text is {@code parts} joined by {@code |} (one chunk per part). */
  protected static FixtureFile text(String name, String... parts) {
    return object(name + "." + TEXT_EXTENSION, String.join("|", parts).getBytes(StandardCharsets.UTF_8));
  }

  /** Writes {@code files} to the bucket and announces them in one events commit. */
  protected void announce(FixtureFile... files) throws IOException {
    BlobJobHarness objects = objects();
    List<S3EventsTableFixture.Event> events = new ArrayList<>();
    for (FixtureFile file : files) {
      events.add(objects.put(keyOf(file), file.bytes));
    }
    objects.announce(spark(), events);
  }

  /** A local filesystem path for a base path that may be a {@code file:} URI (the test harness hands out URIs). */
  protected static Path localPath(String basePath) {
    return Paths.get(basePath.replaceFirst("^file:(//)?", ""));
  }

  /** The object key of {@code file} in the S3 events tests' bucket. */
  protected static String keyOf(FixtureFile file) {
    return file.sourcePath.substring(BlobJobHarness.sourcePath("").length());
  }

  /**
   * The embedding job's properties for {@code table} over the S3 events table, objects inline up to {@code inlineMaxBytes}:
   * {@value #TEXT_EXTENSION} files by {@link TextPipePreprocessor}.
   */
  protected TypedProperties props(String table, String mode, long inlineMaxBytes, String... overrides) {
    TypedProperties extra = new TypedProperties();
    extra.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + TEXT_EXTENSION
        + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX, TextPipePreprocessor.class.getName());
    extra.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    extra.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "2");
    for (int i = 0; i + 1 < overrides.length; i += 2) {
      extra.setProperty(overrides[i], overrides[i + 1]);
    }
    return EmbeddingStreamers.jobProps(table, objects().eventsPath(), BucketFolderFileSystem.SCHEME, inlineMaxBytes, mode, extra);
  }

  /** {@link #props(String, String, long, String...)} with every object inline ({@link #INLINE_MAX_BYTES}). */
  protected TypedProperties props(String table, String mode, String... overrides) {
    return props(table, mode, INLINE_MAX_BYTES, overrides);
  }

  /** The table's rows by record key, without Hudi's meta columns. */
  protected Map<String, Row> rows(String table) {
    Dataset<Row> snapshot = spark().read().format("hudi").load(table);
    List<String> columns = Arrays.stream(snapshot.columns())
        .filter(column -> !HoodieRecord.HOODIE_META_COLUMNS.contains(column)).collect(Collectors.toList());
    StructType dataSchema = new StructType(columns.stream().map(snapshot.schema()::apply).toArray(StructField[]::new));
    Map<String, Row> rows = new TreeMap<>();
    for (Row row : snapshot.collectAsList()) {
      String recordKey = row.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD);
      Object[] values = columns.stream().map(row::getAs).toArray();
      Row previous = rows.put(recordKey, new GenericRowWithSchema(values, dataSchema));
      if (previous != null) {
        throw new AssertionError("duplicate key " + recordKey);
      }
    }
    return rows;
  }
}
