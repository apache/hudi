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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Stored keys include the source version, modality and chunk; repeated keys use ordinary UPSERT. */
@Tag("functional")
class TestEmbeddingRecordKey extends SparkClientFunctionalTestHarness {

  private static final String MODEL = "dummy_v1";
  private static final String SOURCE_SHA = "0123456789abcdef0123456789abcdef";

  private String embeddingTablePath(String name) {
    return basePath() + "/" + name;
  }

  private TypedProperties jobProps(String table) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTablePath(table));
    props.setProperty(EmbeddingTableConfig.MODELS.key(), MODEL);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "8");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + MODEL + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    return props;
  }

  @Test
  void keyIsTheFiveFieldValuePairs() {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(jobProps("version_key"));
    StructType schema = EmbeddingTableSchema.build(config);
    prepWrite(config, schema, row(schema, "text", 3L, "chunk 3"));
    Row stored = spark().read().format("hudi").load(config.targetTablePath()).select(
        HoodieRecord.RECORD_KEY_METADATA_FIELD, EmbeddingTableSchema.PREPARED_TEXT_CHUNK).first();
    assertEquals(expectedKey("text", 3L), stored.getString(0));
    assertEquals("chunk 3", stored.getString(1));
  }

  @Test
  void twoModalitiesOfOneChunkAreTwoRows() {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(jobProps("modalities"));
    StructType schema = EmbeddingTableSchema.build(config);
    prepWrite(config, schema, row(schema, "text", 0L, "text input"));
    prepWrite(config, schema, row(schema, "image", 0L, "image input"));
    List<Row> stored = spark().read().format("hudi").load(config.targetTablePath())
        .select(HoodieRecord.RECORD_KEY_METADATA_FIELD, EmbeddingTableSchema.PREPARED_TEXT_CHUNK)
        .orderBy(HoodieRecord.RECORD_KEY_METADATA_FIELD).collectAsList();
    assertEquals(Arrays.asList(RowFactory.create(expectedKey("image", 0L), "image input"),
        RowFactory.create(expectedKey("text", 0L), "text input")), stored);
  }

  @Test
  void anExactRepeatUpsertedAgainIsOneRow() {
    EmbeddingTableConfig config = EmbeddingTableConfig.from(jobProps("repeat"));
    StructType schema = EmbeddingTableSchema.build(config);
    prepWrite(config, schema, row(schema, "text", 3L, "first"));
    prepWrite(config, schema, row(schema, "text", 3L, "updated"));
    List<Row> stored = spark().read().format("hudi").load(config.targetTablePath())
        .select(HoodieRecord.RECORD_KEY_METADATA_FIELD, EmbeddingTableSchema.PREPARED_TEXT_CHUNK).collectAsList();
    assertEquals(Collections.singletonList(RowFactory.create(expectedKey("text", 3L), "updated")), stored);
    assertEquals(2, EmbeddingStreamers.commits(config.targetTablePath(), EmbeddingStreamers.storageConf(spark())).size());
  }

  private static String expectedKey(String modality, long chunkId) {
    return "source_path_sha:" + SOURCE_SHA + ",source_modification_time:1234,source_file_size:99,modality:" + modality + ",chunk_id:" + chunkId;
  }

  /** Raw upsert with the same key generator and writer settings as the embedding stream. */
  private void prepWrite(EmbeddingTableConfig config, StructType schema, Row row) {
    TypedProperties shape = EmbeddingStreamers.jobProps(config.targetTablePath(), basePath() + "/events", "file", 0L,
        EmbeddingTableConfig.MODE_PREP_ONLY, new TypedProperties());
    Map<String, String> options = new HashMap<>();
    for (String key : Arrays.asList("hoodie.datasource.write.recordkey.field", "hoodie.datasource.write.partitionpath.field",
        "hoodie.datasource.write.keygenerator.class", "hoodie.datasource.write.hive_style_partitioning",
        "hoodie.datasource.write.partitionpath.urlencode", "hoodie.write.record.merge.mode")) {
      options.put(key, shape.getString(key));
    }
    options.put("hoodie.table.name", "embedding_key_test");
    options.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");
    options.put("hoodie.datasource.write.operation", "upsert");
    spark().createDataFrame(Collections.singletonList(row), schema).write().format("hudi").options(options)
        .mode(SaveMode.Append).save(config.targetTablePath());
  }

  private static Row row(StructType schema, String modality, long chunkId, String text) {
    Object[] values = new Object[schema.fields().length];
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_PATH_SHA)] = SOURCE_SHA;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME)] = 1234L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_SIZE)] = 99L;
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_PATH)] = "s3://bucket/a.pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_NAME)] = "a.pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.SOURCE_FILE_EXTENSION)] = "pdf";
    values[schema.fieldIndex(EmbeddingTableSchema.CHUNK_ID)] = chunkId;
    values[schema.fieldIndex(EmbeddingTableSchema.MODALITY)] = modality;
    values[schema.fieldIndex(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)] = text;
    return RowFactory.create(values);
  }
}
