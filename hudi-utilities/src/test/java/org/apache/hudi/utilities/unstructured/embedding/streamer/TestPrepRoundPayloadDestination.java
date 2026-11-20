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
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.prep.TestPreprocessingIterator.PipeSplitPreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The prep round builds its payload destination (the input store's out-of-line writer, whose markers must be timeline-server
 * based) only when a preprocessor of the job stores inputs: a job whose prepped extensions store none runs under direct
 * markers, and one that may store inputs is refused under them before any payload is prepared.
 */
@Tag("functional")
class TestPrepRoundPayloadDestination extends SparkClientFunctionalTestHarness {

  private static final StructType SOURCE = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BlobType.dataType(), false)});

  /**
   * Preps one text file under direct markers; {@code extensions} is the job's prep allow-list, empty for none (every mapped
   * extension, the image and video defaults included).
   */
  private PrepRound.Prepared prepareUnderDirectMarkers(String extensions) throws Exception {
    Path meta = new Path(basePath(), HoodieTableMetaClient.METAFOLDER_NAME);
    if (!meta.getFileSystem(jsc().hadoopConfiguration()).exists(meta)) {
      HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
          .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
          .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
          .initTable(HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()), basePath());
    }
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), basePath());
    props.setProperty(HoodieWriteConfig.MARKERS_TYPE.key(), MarkerType.DIRECT.name());
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        PipeSplitPreprocessor.class.getName());
    if (!extensions.isEmpty()) {
      props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), extensions);
    }
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), "word-count");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType table = EmbeddingTableSchema.build(config);
    StructType output = table.add(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true);
    Row text = RowFactory.create("sha-a", 1000L, 5L, "s3a://bucket/a.txt", "a.txt", "txt",
        RowFactory.create(HoodieSchema.Blob.INLINE, "alpha|beta".getBytes(StandardCharsets.UTF_8), null));
    return PrepRound.prepare(spark(), spark().createDataFrame(Collections.singletonList(text), SOURCE), config,
        HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()), output, "001", StorageLevel.MEMORY_AND_DISK());
  }

  @Test
  void missingTimelineServerIsRefusedAtRoundStart() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), basePath());
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), "word-count");
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "jpg");
    props.setProperty(HoodieWriteConfig.MARKERS_TYPE.key(), MarkerType.TIMELINE_SERVER_BASED.name());
    props.setProperty(HoodieWriteConfig.EMBEDDED_TIMELINE_SERVER_ENABLE.key(), "false");
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    SparkSession spark = spark();
    Option<StructType> noTable = Option.empty();
    HoodieValidationException refused = assertThrows(HoodieValidationException.class,
        () -> EmbeddingTableTransformer.checkRound(config, spark, noTable));
    assertTrue(refused.getMessage().contains("timeline server"), refused.getMessage());
    // The same start check permits a PDF-only round under DIRECT markers, without reading any batch.
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf");
    props.setProperty(HoodieWriteConfig.MARKERS_TYPE.key(), MarkerType.DIRECT.name());
    assertEquals(Option.empty(), EmbeddingTableTransformer.checkRound(EmbeddingTableConfig.from(props), spark(), Option.empty()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"txt", "pdf,txt"})
  void jobWhoseExtensionsStoreNoInputsRunsUnderDirectMarkers(String extensions) throws Exception {
    PrepRound.Prepared prepared = prepareUnderDirectMarkers(extensions);
    try {
      assertEquals(2L, prepared.output.count());
      assertEquals(1L, prepared.counters.getFilesOk().value());
    } finally {
      prepared.output.unpersist(false);
      prepared.balance.close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"jpg,txt", ""})
  void jobThatMayStoreInputsIsRefusedUnderDirectMarkers(String extensions) {
    Exception failure = assertThrows(Exception.class, () -> prepareUnderDirectMarkers(extensions));
    Throwable cause = failure;
    while (cause != null && !(cause instanceof HoodieValidationException)) {
      cause = cause.getCause();
    }
    assertTrue(cause != null && cause.getMessage().contains("TIMELINE_SERVER_BASED"), String.valueOf(failure));
  }
}
