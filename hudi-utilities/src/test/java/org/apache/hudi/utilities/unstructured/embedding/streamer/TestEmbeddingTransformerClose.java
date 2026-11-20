/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableChecks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Closing one embedding transformer retires its round caches while a caller keeps Spark and other jobs alive. */
@Tag("functional")
class TestEmbeddingTransformerClose extends EmbeddingRoundTestBase {
  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType SOURCE_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false)});
  private final Set<String> targets = new HashSet<>();

  @AfterEach
  void releaseTestCaches() {
    targets.forEach(EmbeddingTableTransformer::releaseRound);
    spark().catalog().clearCache();
  }

  @Test
  void closeRetiresTheLastRoundAndKeepsCallerCachesAndContext() throws Exception {
    Dataset<Row> caller = spark().range(3).toDF().persist(StorageLevel.MEMORY_ONLY());
    caller.count();
    Set<Integer> before = cacheIds();
    EmbeddingTableTransformer transformer = new EmbeddingTableTransformer();
    prepare(transformer, table(), "001");
    assertTrue(cacheIds().size() > before.size(), "the round has materialized caches");
    transformer.close();
    transformer.close();
    assertEquals(before, cacheIds(), "closing the final round releases only its own caches");
    assertFalse(jsc().sc().isStopped(), "the caller still owns the Spark context");
    assertEquals(3, caller.count());
  }

  @Test
  void closingAnOlderInstanceDoesNotRetireANewerRoundForTheSameTarget() throws Exception {
    EmbeddingTableTransformer older = new EmbeddingTableTransformer();
    EmbeddingTableTransformer newer = new EmbeddingTableTransformer();
    prepare(older, table(), "001");
    prepare(newer, table(), "002");
    Set<Integer> newerIds = cacheIds();
    assertFalse(newerIds.isEmpty(), "the replacement round retains its caches until its owner closes");
    older.close();
    assertEquals(newerIds, cacheIds(), "an old owner cannot clear a newer owner's cache");
    newer.close();
  }

  @Test
  void closingOneTargetKeepsTheOtherTransformersRound() throws Exception {
    Set<Integer> before = cacheIds();
    EmbeddingTableTransformer first = new EmbeddingTableTransformer();
    EmbeddingTableTransformer second = new EmbeddingTableTransformer();
    prepare(first, table(), "001");
    Set<Integer> firstIds = cacheIds();
    firstIds.removeAll(before);
    prepare(second, table() + "-other", "002");
    Set<Integer> secondIds = cacheIds();
    secondIds.removeAll(before);
    secondIds.removeAll(firstIds);
    assertFalse(secondIds.isEmpty(), "the other target has a materialized cache");
    first.close();
    Set<Integer> remaining = cacheIds();
    assertTrue(remaining.containsAll(secondIds), "the other target stays cached");
    remaining.retainAll(firstIds);
    assertTrue(remaining.isEmpty(), "the closed target releases every retained cache");
    second.close();
    assertEquals(before, cacheIds());
  }

  private Set<Integer> cacheIds() {
    return new HashSet<>(jsc().getPersistentRDDs().keySet());
  }

  private void prepare(EmbeddingTableTransformer transformer, String target, String instant) throws Exception {
    targets.add(target);
    TypedProperties props = props(target, EmbeddingTableConfig.MODE_PREP_ONLY,
        EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), TEXT_EXTENSION);
    if (!EmbeddingTableChecks.tableExists(target, storage())) {
      HoodieTableMetaClient.newTableBuilder().setTableVersion(9).setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName("cache-ownership").setAdditionalProperties(UnstructuredTableTestProperties.embedding(props))
          .initTable(storage(), target);
    }
    byte[] text = "one".getBytes(StandardCharsets.UTF_8);
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, text, null}, BLOB);
    Row row = new GenericRowWithSchema(new Object[] {"source-version", TEXT_EXTENSION, (long) text.length, 1L, blob,
        "file:///source/one." + TEXT_EXTENSION, "one." + TEXT_EXTENSION}, SOURCE_ROW);
    Dataset<Row> source = spark().createDataFrame(Collections.singletonList(row), SOURCE_ROW);
    assertTrue(transformer.apply(jsc(), spark(), source, props, instant).count() > 0);
  }
}
