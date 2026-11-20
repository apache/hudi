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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingPrepFailedException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The embedding round preserves its caller's reader setting while preprocessing, including failed rounds. */
@Tag("functional")
class TestEmbeddingReaderConfig extends EmbeddingRoundTestBase {
  private static final String READER_CONF = "spark.sql.parquet.enableVectorizedReader";

  @ParameterizedTest(name = "reader={0}, failed={1}")
  @CsvSource({"true,false", "false,false", "unset,false", "true,true", "false,true", "unset,true"})
  void callerSettingSurvivesDuringWork(String setting, boolean failed) throws Exception {
    SparkSession session = spark();
    boolean wasSet = session.conf().contains(READER_CONF);
    String before = session.conf().get(READER_CONF);
    if ("unset".equals(setting)) {
      session.conf().unset(READER_CONF);
    } else {
      session.conf().set(READER_CONF, setting);
    }
    boolean callerWasSet = session.conf().contains(READER_CONF);
    String expected = session.conf().get(READER_CONF);
    RecordingPreprocessor.session = session;
    RecordingPreprocessor.OBSERVED.clear();
    String target = table();
    TypedProperties props = props(target, EmbeddingTableConfig.MODE_PREP_ONLY,
        HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false",
        EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), TEXT_EXTENSION,
        EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + TEXT_EXTENSION + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        RecordingPreprocessor.class.getName());
    // The streamer starts the table before invoking the transformer; prep's payload destination requires it.
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.embedding(props))
        .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("reader_config")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS)).initTable(storage(), target);
    byte[] bytes = (failed ? TextPipePreprocessor.BROKEN : "alpha").getBytes(StandardCharsets.UTF_8);
    Row source = RowFactory.create("source", 100L, "file:///source.tif", "source.tif", TEXT_EXTENSION, (long) bytes.length,
        "text/plain", RowFactory.create(HoodieSchema.Blob.INLINE, bytes, null));
    Dataset<Row> batch = session.createDataFrame(Collections.singletonList(source), BlobTableSchema.schema());
    try {
      if (failed) {
        EmbeddingTableTransformer transformer = new EmbeddingTableTransformer();
        JavaSparkContext context = jsc();
        assertThrows(EmbeddingPrepFailedException.class, () -> transformer.apply(context, session, batch, props, "20261031000000000"));
      } else {
        List<Row> output = new EmbeddingTableTransformer().apply(jsc(), session, batch, props, "20261031000000000").collectAsList();
        assertEquals(1, output.size());
        assertEquals("alpha", output.get(0).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
      }
      assertEquals(Collections.singletonList(expected), new ArrayList<>(RecordingPreprocessor.OBSERVED),
          "the caller's setting must be visible DURING preprocessing");
      assertEquals(expected, session.conf().get(READER_CONF));
      assertEquals(callerWasSet, session.conf().contains(READER_CONF));
    } finally {
      EmbeddingTableTransformer.releaseRound(target);
      RecordingPreprocessor.session = null;
      RecordingPreprocessor.OBSERVED.clear();
      if (wasSet) {
        session.conf().set(READER_CONF, before);
      } else {
        session.conf().unset(READER_CONF);
      }
    }
  }

  /** Local Spark tasks record the driver's session setting at the point that real prep consumes the bytes. */
  public static class RecordingPreprocessor extends TextPipePreprocessor {
    private static volatile SparkSession session;
    private static final ConcurrentLinkedQueue<String> OBSERVED = new ConcurrentLinkedQueue<>();

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
        throws IOException {
      OBSERVED.add(session.conf().get(READER_CONF));
      return super.open(bytes, file, tokenizer, props);
    }
  }
}
