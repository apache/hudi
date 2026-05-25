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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the supported mid-life transition of {@link HoodieTableConfig#POPULATE_META_FIELDS}.
 *
 * <p>Spark datasource writer-side validation ({@code HoodieWriterUtils.validateTableConfig}) rejects
 * any write that sets {@code hoodie.populate.meta.fields} to a value different from the persisted
 * {@code hoodie.properties}. So the only supported way to transition is via direct
 * {@code HoodieTableConfig.update(...)} (which is what the {@code hudi-cli table update-configs}
 * command invokes). After that update, subsequent writes pass the conflict check and proceed.
 *
 * <p>Scenario:
 * <ol>
 *   <li>Create a table with default options (populateMetaFields=true).</li>
 *   <li>Two commits via Spark datasource.</li>
 *   <li>Persist {@code POPULATE_META_FIELDS=false} via {@code HoodieTableConfig.update} (CLI path).</li>
 *   <li>One more commit via Spark datasource.</li>
 *   <li>Assert {@code tableConfig.populateMetaFields()} reflects false at every relevant point.</li>
 * </ol>
 */
class TestPopulateMetaFieldsTransition extends SparkClientFunctionalTestHarness {

  @Test
  void testPopulateMetaFieldsTrueToFalseViaTableConfigUpdate() {
    String path = basePath();
    StructType schema = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("column1", DataTypes.StringType, true),
        DataTypes.createStructField("column2", DataTypes.StringType, true),
        DataTypes.createStructField("column3", DataTypes.StringType, true)
    }).asNullable();

    Map<String, String> baseOptions = new HashMap<>();
    baseOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "column1");
    baseOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "column2");
    baseOptions.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "column3");
    baseOptions.put(HoodieTableConfig.NAME.key(), "test_populate_meta_transition");
    baseOptions.put(HoodieMetadataConfig.ENABLE.key(), "false");
    baseOptions.put(HoodieWriteConfig.AUTO_UPGRADE_VERSION.key(), "false");

    // Commit 1 & 2: default options. Table is created with populateMetaFields=true.
    writeRow(baseOptions, schema, path, "k1");
    writeRow(baseOptions, schema, path, "k2");

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertTrue(metaClient.getTableConfig().populateMetaFields(),
        "After initial commits, populateMetaFields should be true");
    assertEquals("true",
        metaClient.getTableConfig().getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "hoodie.properties should record populate.meta.fields=true after creation");

    // Flip populate.meta.fields=false directly in hoodie.properties via the CLI-equivalent API.
    Properties update = new Properties();
    update.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), update);

    // Re-read meta client; assert the property was persisted.
    metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertEquals("false",
        metaClient.getTableConfig().getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "hoodie.properties should reflect the flip to false after HoodieTableConfig.update");
    assertEquals(false, metaClient.getTableConfig().populateMetaFields(),
        "populateMetaFields() must return false after the flip");

    // Commit 3: writer also passes populate.meta.fields=false to avoid the writer-side conflict check
    // (this is the same value as the now-persisted table config, so the check passes).
    Map<String, String> commit3Options = new HashMap<>(baseOptions);
    commit3Options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    writeRow(commit3Options, schema, path, "k3");

    metaClient = HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    assertEquals(false, metaClient.getTableConfig().populateMetaFields(),
        "populateMetaFields() should remain false after a subsequent write");
    assertEquals("false",
        metaClient.getTableConfig().getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "hoodie.properties should remain populate.meta.fields=false after a subsequent write");
  }

  private void writeRow(Map<String, String> options, StructType schema, String path, String key) {
    List<Row> records = Collections.singletonList(RowFactory.create(key, "p1", "v1"));
    spark().createDataset(records,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(path);
  }
}
