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
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.prep.TestPreprocessingIterator.PipeSplitPreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Prep retains each source version of a path. */
@Tag("functional")
public class TestPrepRoundTwoVersions extends SparkClientFunctionalTestHarness {

  private static final StructType SOURCE = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BlobType.dataType(), false),
      DataTypes.createStructField("team", DataTypes.StringType, true)});

  private static Row source(Long time, Long size, String text) {
    return RowFactory.create("sha-a", time, size, "s3a://bucket/a.txt", "a.txt", "txt",
        RowFactory.create(HoodieSchema.Blob.INLINE, text.getBytes(StandardCharsets.UTF_8), null), "blue");
  }

  private PrepRound.Prepared prepare(Row... rows) throws Exception {
    // the round's table exists before prep runs (the streamer starts the instant first); prep writes large payloads under it
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), basePath());
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        PipeSplitPreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), "word-count");
    props.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), "2");
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "team");
    Path meta = new Path(basePath(), HoodieTableMetaClient.METAFOLDER_NAME);
    if (!meta.getFileSystem(jsc().hadoopConfiguration()).exists(meta)) {
      HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.embedding(props)).setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
          .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
          .initTable(HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()), basePath());
    }
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    StructType table = EmbeddingTableSchema.build(config);
    StructType output = table.add(PrepRound.CORRUPT_RECORD, DataTypes.StringType, true);
    return PrepRound.prepare(spark(), spark().createDataFrame(Arrays.asList(rows), SOURCE), config,
        HadoopFSUtils.getStorageConf(jsc().hadoopConfiguration()), table, output, "001", StorageLevel.MEMORY_AND_DISK());
  }

  private static List<Row> data(Dataset<Row> rows) {
    return rows.filter(functions.col(PrepRound.CORRUPT_RECORD).isNull())
        .orderBy(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, EmbeddingTableSchema.SOURCE_FILE_SIZE, EmbeddingTableSchema.CHUNK_ID)
        .selectExpr("source_path_sha", "source_modification_time", "source_file_size", "modality", "chunk_id",
            "source_file_path", "source_file_name", "source_file_extension", "prepared_text_chunk", "chunk_token_count", "to_json(aux)", "team")
        .collectAsList();
  }

  private static Row expected(long time, long size, long chunk, String text) {
    return RowFactory.create("sha-a", time, size, "text", chunk, "s3a://bucket/a.txt", "a.txt", "txt", text, 1, "{\"part\":\"" + chunk + "\"}", "blue");
  }

  private static void close(PrepRound.Prepared prepared) {
    prepared.output.unpersist(false);
    prepared.balance.close();
  }

  @Test
  public void twoVersionsOfOnePathInOneBatchAreBothPrepared() throws Exception {
    PrepRound.Prepared prepared = prepare(source(1000L, 5L, "alpha"), source(2000L, 9L, "beta|gamma"));
    try {
      assertEquals(Arrays.asList(expected(1000L, 5L, 0L, "alpha"), expected(2000L, 9L, 0L, "beta"),
          expected(2000L, 9L, 1L, "gamma")), data(prepared.output));
      assertEquals(3L, prepared.output.count());
      assertEquals(2L, prepared.counters.getFilesOk().value());
      assertEquals(0L, prepared.counters.getFilesFailed().value());
    } finally {
      close(prepared);
    }
  }

  @Test
  public void differentSizeIsAnotherVersion() throws Exception {
    PrepRound.Prepared prepared = prepare(source(1000L, 5L, "alpha"), source(1000L, 4L, "beta"));
    try {
      assertEquals(Arrays.asList(expected(1000L, 4L, 0L, "beta"), expected(1000L, 5L, 0L, "alpha")), data(prepared.output));
      assertEquals(2L, prepared.output.count());
      assertEquals(2L, prepared.counters.getFilesOk().value());
    } finally {
      close(prepared);
    }
  }
}
