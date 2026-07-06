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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark-datasource tests for the {@code hoodie.meta.fields.mode} property on CoW tables.
 *
 * <p>Modes covered:
 * <ul>
 *   <li>ALL — {@code populate.meta.fields=true} (default).</li>
 *   <li>NONE — {@code populate.meta.fields=false} and mode empty.</li>
 *   <li>COMMIT_TIME_ONLY — {@code mode=_hoodie_commit_time}.</li>
 *   <li>FILE_NAME_ONLY — {@code mode=_hoodie_file_name}.</li>
 *   <li>COMMIT_TIME_AND_FILE_NAME — both tokens in the mode list.</li>
 * </ul>
 *
 * <p>Rejection paths: an unknown token or the ambiguous
 * {@code populate.meta.fields=true} + non-empty mode combination must fail at writer init. MoR +
 * non-empty mode is likewise rejected until log-write support lands (tracked as follow-up).
 */
class TestMetaFieldsMode extends SparkClientFunctionalTestHarness {

  private static final String COMMIT_TIME_TOKEN = HoodieRecord.COMMIT_TIME_METADATA_FIELD;
  private static final String FILE_NAME_TOKEN = HoodieRecord.FILENAME_METADATA_FIELD;

  private static StructType simpleSchema() {
    return DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("column1", DataTypes.StringType, true),
        DataTypes.createStructField("column2", DataTypes.StringType, true),
        DataTypes.createStructField("column3", DataTypes.StringType, true)
    }).asNullable();
  }

  private Map<String, String> baseOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "column1");
    opts.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "column2");
    opts.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "column3");
    opts.put(HoodieTableConfig.NAME.key(), "test_meta_fields_mode");
    opts.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    opts.put(HoodieMetadataConfig.ENABLE.key(), "false");
    return opts;
  }

  private void writeRows(List<Row> records, StructType schema, Map<String, String> options, String path, SaveMode mode) {
    spark().createDataset(records,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema))
        .write()
        .format("hudi")
        .options(options)
        .mode(mode)
        .save(path);
  }

  private HoodieTableConfig writeAndReadTableConfig(Map<String, String> options, String path) {
    writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
        simpleSchema(), options, path, SaveMode.Overwrite);
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(path).setConf(storageConf()).build();
    return metaClient.getTableConfig();
  }

  @Test
  void commitTimeOnlyModePersistsPropertyAndReportsMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), COMMIT_TIME_TOKEN);
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeAndReadTableConfig(options, basePath());

    assertEquals("false", tc.getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()));
    assertEquals(COMMIT_TIME_TOKEN, tc.getProps().getProperty(HoodieTableConfig.META_FIELDS_MODE.key()));
    assertFalse(tc.populateMetaFields());
    assertTrue(tc.isCommitTimePopulated());
    assertFalse(tc.isFileNamePopulated());
    assertFalse(tc.isRecordKeyPopulated());
  }

  @Test
  void fileNameOnlyModePersistsPropertyAndReportsMode() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), FILE_NAME_TOKEN);
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeAndReadTableConfig(options, basePath());

    assertEquals(FILE_NAME_TOKEN, tc.getProps().getProperty(HoodieTableConfig.META_FIELDS_MODE.key()));
    assertFalse(tc.populateMetaFields());
    assertFalse(tc.isCommitTimePopulated());
    assertTrue(tc.isFileNamePopulated());
    assertFalse(tc.isRecordKeyPopulated());
  }

  @Test
  void commitTimeAndFileNameModePersistsAndReports() {
    String combined = COMMIT_TIME_TOKEN + "," + FILE_NAME_TOKEN;
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), combined);
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeAndReadTableConfig(options, basePath());

    assertEquals(combined, tc.getProps().getProperty(HoodieTableConfig.META_FIELDS_MODE.key()));
    assertTrue(tc.isCommitTimePopulated());
    assertTrue(tc.isFileNamePopulated());
    assertFalse(tc.isRecordKeyPopulated());
  }

  @Test
  void noneModePersistsAndReportsCorrectly() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeAndReadTableConfig(options, basePath());

    assertFalse(tc.populateMetaFields());
    assertTrue(tc.getMetaFieldsMode().isEmpty());
    assertFalse(tc.isCommitTimePopulated());
    assertFalse(tc.isFileNamePopulated());
    assertFalse(tc.isRecordKeyPopulated());
  }

  @Test
  void defaultAllModeReportsPopulated() {
    Map<String, String> options = baseOptions();
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    HoodieTableConfig tc = writeAndReadTableConfig(options, basePath());

    assertTrue(tc.populateMetaFields());
    assertTrue(tc.isCommitTimePopulated());
    assertTrue(tc.isFileNamePopulated());
    assertTrue(tc.isRecordKeyPopulated());
  }

  @Test
  void populateTrueWithNonEmptyModeIsRejected() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), COMMIT_TIME_TOKEN);
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, basePath(), SaveMode.Overwrite));

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains(HoodieTableConfig.META_FIELDS_MODE.key())
            || rootMessage.contains(HoodieTableConfig.POPULATE_META_FIELDS.key()),
        "Expected validation error to name one of the conflicting properties, got: " + rootMessage);
  }

  @Test
  void unknownTokenInModeIsRejected() {
    Map<String, String> options = baseOptions();
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), HoodieRecord.RECORD_KEY_METADATA_FIELD);
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, basePath(), SaveMode.Overwrite));

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        "Expected error to name the rejected token, got: " + rootMessage);
  }

  @Test
  void morWithNonEmptyModeIsRejected() {
    Map<String, String> options = baseOptions();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    options.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    options.put(HoodieTableConfig.META_FIELDS_MODE.key(), COMMIT_TIME_TOKEN);
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());

    Throwable thrown = assertThrows(Throwable.class, () ->
        writeRows(Collections.singletonList(RowFactory.create("k1", "p1", "v1")),
            simpleSchema(), options, basePath(), SaveMode.Overwrite));

    String rootMessage = rootMessageOf(thrown);
    assertTrue(rootMessage.contains("COPY_ON_WRITE") || rootMessage.contains("MoR") || rootMessage.contains("MERGE_ON_READ"),
        "Expected MoR-restriction error, got: " + rootMessage);
  }

  private static String rootMessageOf(Throwable thrown) {
    Throwable root = thrown;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root.getMessage() == null ? "" : root.getMessage();
  }
}
