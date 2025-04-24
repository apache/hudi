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

package org.apache.hudi.hadoop;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.HoodieFileGroupReaderOnJavaTestBase;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.hadoop.utils.ObjectInspectorCache;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.testutils.ArrayWritableTestUtil;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.HoodieFileGroupReaderBasedRecordReader.getStoredPartitionFieldNames;

public class TestHoodieFileGroupReaderOnHive extends HoodieFileGroupReaderOnJavaTestBase<ArrayWritable> {

  private static final String PARTITION_COLUMN = "datestr";
  private static JobConf baseJobConf;
  private static HdfsTestService hdfsTestService;
  private static HoodieStorage storage;
  private static FileSystem fs;
  private static StorageConfiguration<Configuration> storageConf;

  //currently always true. If we ever have a test with a nonpartitioned table, the usages of this should be tied together
  private static final boolean USE_FAKE_PARTITION = true;

  @BeforeAll
  public static void setUpClass() throws IOException {
    // Append is not supported in LocalFileSystem. HDFS needs to be setup.
    hdfsTestService = new HdfsTestService();
    fs = hdfsTestService.start(true).getFileSystem();
    storageConf = HoodieTestUtils.getDefaultStorageConf();
    baseJobConf = new JobConf(storageConf.unwrap());
    baseJobConf.set(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(), String.valueOf(1024 * 1024));
    fs.setConf(baseJobConf);
    storage = new HoodieHadoopStorage(fs);
  }

  @AfterAll
  public static void tearDownClass() throws IOException {
    hdfsTestService.stop();
    if (fs != null) {
      fs.close();
      storage.close();
    }
  }

  @Override
  public StorageConfiguration<?> getStorageConf() {
    return storageConf;
  }

  @Override
  public HoodieReaderContext<ArrayWritable> getHoodieReaderContext(String tablePath, Schema avroSchema, StorageConfiguration<?> storageConf, HoodieTableMetaClient metaClient) {
    HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator = (inputSplit, jobConf) -> new MapredParquetInputFormat().getRecordReader(inputSplit, jobConf, null);
    JobConf jobConf = new JobConf(storageConf.unwrapAs(Configuration.class));
    setupJobconf(jobConf, metaClient.getTableConfig().populateMetaFields());
    return new HiveHoodieReaderContext(readerCreator,
        getStoredPartitionFieldNames(new JobConf(storageConf.unwrapAs(Configuration.class)), avroSchema),
        new ObjectInspectorCache(avroSchema, jobConf), storageConf, metaClient.getTableConfig());
  }

  @Override
  public void assertRecordsEqual(Schema schema, ArrayWritable expected, ArrayWritable actual) {
    ArrayWritableTestUtil.assertArrayWritableEqual(schema, expected, actual, false);
  }

  private void setupJobconf(JobConf jobConf, boolean populateMetaFields) {
    Schema schema = populateMetaFields ? HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SCHEMA) : HoodieTestDataGenerator.AVRO_SCHEMA;
    List<Schema.Field> fields = schema.getFields();
    setHiveColumnNameProps(fields, jobConf, USE_FAKE_PARTITION);
    String metaFieldTypes = "string,string,string,string,string,";
    jobConf.set("columns.types", (populateMetaFields ? metaFieldTypes : "") + HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES + ",string");
  }

  private void setHiveColumnNameProps(List<Schema.Field> fields, JobConf jobConf, boolean isPartitioned) {
    String names = fields.stream().map(Schema.Field::name).collect(Collectors.joining(","));
    String positions = fields.stream().map(f -> String.valueOf(f.pos())).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, names);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, positions);

    String hiveOrderedColumnNames = fields.stream().filter(field -> !field.name().equalsIgnoreCase(PARTITION_COLUMN))
        .map(Schema.Field::name).collect(Collectors.joining(","));
    if (isPartitioned) {
      hiveOrderedColumnNames += "," + PARTITION_COLUMN;
      jobConf.set(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, PARTITION_COLUMN);
    }
    jobConf.set(hive_metastoreConstants.META_TABLE_COLUMNS, hiveOrderedColumnNames);
  }
}
