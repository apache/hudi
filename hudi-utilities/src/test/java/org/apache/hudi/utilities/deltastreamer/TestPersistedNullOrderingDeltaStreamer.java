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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.utilities.config.DFSPathSelectorConfig;
import org.apache.hudi.utilities.config.FilebasedSchemaProviderConfig;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deltastreamer 0.x control for the persisted null ordering value scenario. A record with a null
 * ordering (precombine) value is seeded with a bulk_insert sync (which builds the payload with the
 * default natural-order value, so the null precombine column is persisted without tripping the
 * payload null check), then an upsert sync merges a real-ordering record onto the same key.
 *
 * On 0.x the payload based merge resolves this cleanly (the persisted null loses to the incoming
 * value). The 1.x engine-native merger instead ClassCastExceptions when comparing the boxed default
 * against the incoming Long, so this is the 0.x control.
 */
public class TestPersistedNullOrderingDeltaStreamer extends UtilitiesTestBase {

  private static final String SOURCE_AVSC =
      "{\"type\":\"record\",\"name\":\"rec\",\"namespace\":\"h\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"long\"},"
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"ts\",\"type\":[\"null\",\"long\"],\"default\":null}]}";

  private static final StructType SCHEMA = new StructType()
      .add("id", DataTypes.LongType, false)
      .add("name", DataTypes.StringType, true)
      .add("ts", DataTypes.LongType, true);

  @BeforeAll
  public static void setupClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  public static void teardownClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  private void writeSourceBatch(String sourceDir, SaveMode mode, List<Row> rows) {
    Dataset<Row> df = sparkSession.createDataFrame(rows, SCHEMA);
    df.write().mode(mode).parquet(sourceDir);
  }

  private String writeProps(String name, String sourceDir, String avscPath) throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.datasource.write.recordkey.field", "id");
    props.setProperty("hoodie.datasource.write.precombine.field", "ts");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "");
    props.setProperty("hoodie.datasource.write.keygenerator.class",
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
    props.setProperty(DFSPathSelectorConfig.ROOT_INPUT_PATH.key(), sourceDir);
    props.setProperty(FilebasedSchemaProviderConfig.SOURCE_SCHEMA_FILE.key(), avscPath);
    props.setProperty("hoodie.embed.timeline.server", "false");
    props.setProperty("hoodie.metadata.enable", "false");
    String path = basePath + "/" + name;
    UtilitiesTestBase.Helpers.savePropsToDFS(props, storage, path);
    return path;
  }

  private HoodieDeltaStreamer.Config makeCfg(String targetPath, String propsPath, WriteOperationType op) {
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    cfg.targetBasePath = targetPath;
    cfg.targetTableName = "persisted_null_ds";
    cfg.tableType = "COPY_ON_WRITE";
    cfg.sourceClassName = ParquetDFSSource.class.getName();
    cfg.schemaProviderClassName = FilebasedSchemaProvider.class.getName();
    cfg.propsFilePath = propsPath;
    cfg.sourceOrderingField = "ts";
    cfg.payloadClassName = DefaultHoodieRecordPayload.class.getName();
    cfg.operation = op;
    cfg.sourceLimit = Long.MAX_VALUE;
    return cfg;
  }

  @Test
  public void persistedNullOrderingMergesCleanly() throws Exception {
    String sourceDir = basePath + "/persisted_null_src";
    String avscPath = basePath + "/persisted_null.avsc";
    String targetPath = basePath + "/persisted_null_tgt";
    UtilitiesTestBase.Helpers.saveStringsToDFS(new String[] {SOURCE_AVSC}, storage, avscPath);
    String propsPath = writeProps("persisted_null.properties", sourceDir, avscPath);

    // 1. Seed a persisted null ordering value with a bulk_insert sync.
    writeSourceBatch(sourceDir, SaveMode.Overwrite,
        Arrays.asList(RowFactory.create(1L, "base", null)));
    new HoodieDeltaStreamer(makeCfg(targetPath, propsPath, WriteOperationType.BULK_INSERT), jsc).sync();

    List<Row> seeded = sparkSession.read().format("hudi").load(targetPath)
        .select("id", "name", "ts").collectAsList();
    assertEquals(1, seeded.size());
    assertTrue(seeded.get(0).isNullAt(2), "expected a persisted null ordering value");

    // 2. Upsert a real-ordering record on the same key. This merges the incoming Long ordering
    //    value against the persisted null, the exact path that ClassCastExceptions on 1.x.
    writeSourceBatch(sourceDir, SaveMode.Append,
        Arrays.asList(RowFactory.create(1L, "updated", 100L)));
    new HoodieDeltaStreamer(makeCfg(targetPath, propsPath, WriteOperationType.UPSERT), jsc).sync();

    // 3. The merge succeeds and the incoming record wins.
    List<Row> merged = sparkSession.read().format("hudi").load(targetPath)
        .select("id", "name", "ts").collectAsList();
    assertEquals(1, merged.size());
    assertEquals(1L, merged.get(0).getLong(0));
    assertEquals("updated", merged.get(0).getString(1));
    assertEquals(100L, merged.get(0).getLong(2));
  }
}
