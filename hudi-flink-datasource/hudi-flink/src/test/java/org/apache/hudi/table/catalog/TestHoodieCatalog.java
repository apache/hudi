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

package org.apache.hudi.table.catalog;

import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.CatalogUtils;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.createMetaClient;
import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieCatalog}.
 */
public class TestHoodieCatalog {

  private static final String TEST_DEFAULT_DATABASE = "test_db";
  private static final String NONE_EXIST_DATABASE = "none_exist_database";
  private static final List<Column> CREATE_COLUMNS = Arrays.asList(
      Column.physical("uuid", DataTypes.VARCHAR(20)),
      Column.physical("name", DataTypes.VARCHAR(20)),
      Column.physical("age", DataTypes.INT()),
      Column.physical("tss", DataTypes.TIMESTAMP(3)),
      Column.physical("partition", DataTypes.VARCHAR(10))
  );
  private static final UniqueConstraint CONSTRAINTS = UniqueConstraint.primaryKey("uuid", Arrays.asList("uuid"));
  private static final ResolvedSchema CREATE_TABLE_SCHEMA =
      new ResolvedSchema(
          CREATE_COLUMNS,
          Collections.emptyList(),
          CONSTRAINTS);

  private static final UniqueConstraint MULTI_KEY_CONSTRAINTS = UniqueConstraint.primaryKey("uuid", Arrays.asList("uuid", "name"));
  private static final ResolvedSchema CREATE_MULTI_KEY_TABLE_SCHEMA =
      new ResolvedSchema(
          CREATE_COLUMNS,
          Collections.emptyList(),
          MULTI_KEY_CONSTRAINTS);

  private static final List<Column> EXPECTED_TABLE_COLUMNS =
      CREATE_COLUMNS.stream()
          .map(
              col -> {
                // Flink char/varchar is transform to string in avro.
                if (col.getDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.VARCHAR)) {
                  DataType dataType = DataTypes.STRING();
                  if ("uuid".equals(col.getName())) {
                    dataType = dataType.notNull();
                  }
                  return Column.physical(col.getName(), dataType);
                } else {
                  return col;
                }
              })
          .collect(Collectors.toList());
  private static final ResolvedSchema EXPECTED_TABLE_SCHEMA =
      new ResolvedSchema(EXPECTED_TABLE_COLUMNS, Collections.emptyList(), CONSTRAINTS);

  private static final Map<String, String> EXPECTED_OPTIONS = new HashMap<>();

  static {
    EXPECTED_OPTIONS.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    EXPECTED_OPTIONS.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    EXPECTED_OPTIONS.put(FlinkOptions.PRE_COMBINE.key(), "true");
  }

  private static final ResolvedCatalogTable EXPECTED_CATALOG_TABLE = new ResolvedCatalogTable(
      CatalogUtils.createCatalogTable(
          Schema.newBuilder().fromResolvedSchema(CREATE_TABLE_SCHEMA).build(),
          Arrays.asList("partition"),
          EXPECTED_OPTIONS,
          "test"),
      CREATE_TABLE_SCHEMA
  );

  private TableEnvironment streamTableEnv;
  private String catalogPathStr;
  private HoodieCatalog catalog;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);

    try {
      FileSystem fs = FileSystem.get(HadoopConfigurations.getHadoopConf(new Configuration()));
      fs.mkdirs(new org.apache.hadoop.fs.Path(tempFile.getPath()));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create tempFile dir.", e);
    }

    catalog = new HoodieCatalog("hudi", Configuration.fromMap(getDefaultCatalogOption()));
    catalog.open();
  }

  Map<String, String> getDefaultCatalogOption() {
    Map<String, String> catalogOptions = new HashMap<>();
    assertThrows(ValidationException.class,
        () -> catalog = new HoodieCatalog("hudi", Configuration.fromMap(catalogOptions)));
    catalogPathStr = tempFile.getAbsolutePath();
    catalogOptions.put(CATALOG_PATH.key(), catalogPathStr);
    catalogOptions.put(DEFAULT_DATABASE.key(), TEST_DEFAULT_DATABASE);
    return catalogOptions;
  }

  @AfterEach
  void afterEach() {
    if (catalog != null) {
      catalog.close();
    }
  }

  @Test
  public void testListDatabases() {
    List<String> actual = catalog.listDatabases();
    assertTrue(actual.contains(TEST_DEFAULT_DATABASE));
    assertFalse(actual.contains(NONE_EXIST_DATABASE));
  }

  @Test
  public void testDatabaseExists() {
    assertTrue(catalog.databaseExists(TEST_DEFAULT_DATABASE));
    assertFalse(catalog.databaseExists(NONE_EXIST_DATABASE));
  }

  @Test
  public void testCreateAndDropDatabase() throws Exception {
    CatalogDatabase expected = new CatalogDatabaseImpl(Collections.emptyMap(), null);
    catalog.createDatabase("db1", expected, true);

    CatalogDatabase actual = catalog.getDatabase("db1");
    assertTrue(catalog.listDatabases().contains("db1"));
    assertEquals(expected.getProperties(), actual.getProperties());

    // create exist database
    assertThrows(DatabaseAlreadyExistException.class,
        () -> catalog.createDatabase("db1", expected, false));

    // drop exist database
    catalog.dropDatabase("db1", true);
    assertFalse(catalog.listDatabases().contains("db1"));

    // drop non-exist database
    assertThrows(DatabaseNotExistException.class,
        () -> catalog.dropDatabase(NONE_EXIST_DATABASE, false));
  }

  @Test
  public void testCreateDatabaseWithOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("k1", "v1");
    options.put("k2", "v2");

    assertThrows(
        CatalogException.class,
        () -> catalog.createDatabase("db1", new CatalogDatabaseImpl(options, null), true));
  }

  @Test
  public void testCreateTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // test create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    // test table exist
    assertTrue(catalog.tableExists(tablePath));

    // validate the full name of table create schema
    HoodieTableConfig tableConfig = StreamerUtil.getTableConfig(
        catalog.getTable(tablePath).getOptions().get(FlinkOptions.PATH.key()),
        HadoopConfigurations.getHadoopConf(new Configuration())).get();
    Option<org.apache.avro.Schema> tableCreateSchema = tableConfig.getTableCreateSchema();
    assertTrue(tableCreateSchema.isPresent(), "Table should have been created");
    assertThat(tableCreateSchema.get().getFullName(), is("hoodie.tb1.tb1_record"));

    // test create exist table
    assertThrows(TableAlreadyExistException.class,
        () -> catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, false));

    // validate key generator for partitioned table
    HoodieTableMetaClient metaClient = createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())),
        catalog.inferTablePath(catalogPathStr, tablePath));
    String keyGeneratorClassName = metaClient.getTableConfig().getKeyGeneratorClassName();
    assertEquals(keyGeneratorClassName, SimpleAvroKeyGenerator.class.getName());

    // validate single key and multiple partition for partitioned table
    ObjectPath singleKeyMultiplePartitionPath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb_skmp" + System.currentTimeMillis());
    final ResolvedCatalogTable singleKeyMultiplePartitionTable = new ResolvedCatalogTable(
        CatalogUtils.createCatalogTable(
            Schema.newBuilder().fromResolvedSchema(CREATE_TABLE_SCHEMA).build(),
            Lists.newArrayList("par1", "par2"),
            EXPECTED_OPTIONS,
            "test"),
        CREATE_TABLE_SCHEMA
    );

    catalog.createTable(singleKeyMultiplePartitionPath, singleKeyMultiplePartitionTable, false);
    metaClient = createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())),
        catalog.inferTablePath(catalogPathStr, singleKeyMultiplePartitionPath));
    keyGeneratorClassName = metaClient.getTableConfig().getKeyGeneratorClassName();
    assertThat(keyGeneratorClassName, is(ComplexAvroKeyGenerator.class.getName()));

    // validate multiple key and single partition for partitioned table
    ObjectPath multipleKeySinglePartitionPath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb_mksp" + System.currentTimeMillis());
    final ResolvedCatalogTable multipleKeySinglePartitionTable = new ResolvedCatalogTable(
        CatalogUtils.createCatalogTable(
            Schema.newBuilder().fromResolvedSchema(CREATE_MULTI_KEY_TABLE_SCHEMA).build(),
            Lists.newArrayList("par1"),
            EXPECTED_OPTIONS,
            "test"),
        CREATE_TABLE_SCHEMA
    );

    catalog.createTable(multipleKeySinglePartitionPath, multipleKeySinglePartitionTable, false);
    metaClient = createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())),
        catalog.inferTablePath(catalogPathStr, singleKeyMultiplePartitionPath));
    keyGeneratorClassName = metaClient.getTableConfig().getKeyGeneratorClassName();
    assertThat(keyGeneratorClassName, is(ComplexAvroKeyGenerator.class.getName()));

    // validate key generator for non partitioned table
    ObjectPath nonPartitionPath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb");
    final ResolvedCatalogTable nonPartitionCatalogTable = new ResolvedCatalogTable(
        CatalogUtils.createCatalogTable(
            Schema.newBuilder().fromResolvedSchema(CREATE_TABLE_SCHEMA).build(),
            new ArrayList<>(),
            EXPECTED_OPTIONS,
            "test"),
        CREATE_TABLE_SCHEMA
    );

    catalog.createTable(nonPartitionPath, nonPartitionCatalogTable, false);

    metaClient = createMetaClient(
        new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(new Configuration())),
        catalog.inferTablePath(catalogPathStr, nonPartitionPath));
    keyGeneratorClassName = metaClient.getTableConfig().getKeyGeneratorClassName();
    assertEquals(keyGeneratorClassName, NonpartitionedAvroKeyGenerator.class.getName());
  }

  @Test
  void testCreateTableWithPartitionBucketIndex() throws TableAlreadyExistException, DatabaseNotExistException, IOException {
    String rule = "regex";
    String expressions = "\\d{4}-(06-(01|17|18)|11-(01|10|11)),256";
    String defaultBucketNumber = "20";
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    EXPECTED_CATALOG_TABLE.getOptions().put(FlinkOptions.BUCKET_INDEX_PARTITION_RULE.key(), rule);
    EXPECTED_CATALOG_TABLE.getOptions().put(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS.key(), expressions);
    EXPECTED_CATALOG_TABLE.getOptions().put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), defaultBucketNumber);
    // test create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    // test table exist
    assertTrue(catalog.tableExists(tablePath));

    String tablePathStr = catalog.inferTablePath(catalogPathStr, tablePath);
    Configuration flinkConf = TestConfigurations.getDefaultConf(tablePathStr);
    HoodieTableMetaClient metaClient = HoodieTestUtils
        .createMetaClient(
            new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(flinkConf)), tablePathStr);
    HoodieStorage storage = metaClient.getStorage();
    StoragePath initialHashingConfig =
        new StoragePath(metaClient.getHashingMetadataConfigPath(), PartitionBucketIndexHashingConfig.INITIAL_HASHING_CONFIG_INSTANT + PartitionBucketIndexHashingConfig.HASHING_CONFIG_FILE_SUFFIX);
    StoragePathInfo info = storage.getPathInfo(initialHashingConfig);
    Option<PartitionBucketIndexHashingConfig> hashingConfig = PartitionBucketIndexHashingConfig.loadHashingConfig(storage, info);
    assertTrue(hashingConfig.isPresent());
    assertEquals(hashingConfig.get().getDefaultBucketNumber(), Integer.parseInt(defaultBucketNumber));
    assertEquals(hashingConfig.get().getRule(), rule);
    assertEquals(hashingConfig.get().getExpressions(), expressions);
  }

  @Test
  void testCreateTableWithoutPreCombineKey() {
    Map<String, String> options = getDefaultCatalogOption();
    options.put(FlinkOptions.PAYLOAD_CLASS_NAME.key(), DefaultHoodieRecordPayload.class.getName());
    catalog = new HoodieCatalog("hudi", Configuration.fromMap(options));
    catalog.open();
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    assertThrows(HoodieValidationException.class,
        () -> catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true),
        "Option 'precombine.field' is required for payload class: "
            + "org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    Map<String, String> options2 = getDefaultCatalogOption();
    options2.put(FlinkOptions.PRECOMBINE_FIELD.key(), "not_exists");
    catalog = new HoodieCatalog("hudi", Configuration.fromMap(options2));
    catalog.open();
    ObjectPath tablePath2 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb2");
    assertThrows(HoodieValidationException.class,
        () -> catalog.createTable(tablePath2, EXPECTED_CATALOG_TABLE, true),
        "Field not_exists does not exist in the table schema. Please check 'precombine.field' option.");
  }

  @Test
  public void testListTable() throws Exception {
    ObjectPath tablePath1 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    ObjectPath tablePath2 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb2");

    // create table
    catalog.createTable(tablePath1, EXPECTED_CATALOG_TABLE, true);
    catalog.createTable(tablePath2, EXPECTED_CATALOG_TABLE, true);

    // test list table
    List<String> tables = catalog.listTables(TEST_DEFAULT_DATABASE);
    assertTrue(tables.contains(tablePath1.getObjectName()));
    assertTrue(tables.contains(tablePath2.getObjectName()));

    // test list non-exist database table
    assertThrows(DatabaseNotExistException.class,
        () -> catalog.listTables(NONE_EXIST_DATABASE));
  }

  @Test
  public void testGetTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    Map<String, String> expectedOptions = new HashMap<>(EXPECTED_OPTIONS);
    expectedOptions.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    expectedOptions.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    expectedOptions.put(FlinkOptions.PRE_COMBINE.key(), "true");
    expectedOptions.put("connector", "hudi");
    expectedOptions.put(
        FlinkOptions.PATH.key(),
        String.format("%s/%s/%s", tempFile.getAbsolutePath(), tablePath.getDatabaseName(), tablePath.getObjectName()));

    // test get table
    CatalogBaseTable actualTable = catalog.getTable(tablePath);
    // validate schema
    Schema actualSchema = actualTable.getUnresolvedSchema();
    Schema expectedSchema = Schema.newBuilder().fromResolvedSchema(EXPECTED_TABLE_SCHEMA).build();
    assertEquals(expectedSchema, actualSchema);
    // validate options
    Map<String, String> actualOptions = actualTable.getOptions();
    assertEquals(expectedOptions, actualOptions);
    // validate comment
    assertEquals(EXPECTED_CATALOG_TABLE.getComment(), actualTable.getComment());
    // validate partition key
    assertEquals(EXPECTED_CATALOG_TABLE.getPartitionKeys(), ((CatalogTable) actualTable).getPartitionKeys());
  }

  @Test
  public void testDropTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    // test drop table
    catalog.dropTable(tablePath, true);
    assertFalse(catalog.tableExists(tablePath));

    // drop non-exist table
    assertThrows(TableNotExistException.class,
        () -> catalog.dropTable(new ObjectPath(TEST_DEFAULT_DATABASE, "non_exist"), false));
  }

  @Test
  public void testDropPartition() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(new HashMap<String, String>() {
      {
        put("partition", "par1");
      }
    });
    // drop non-exist partition
    assertThrows(PartitionNotExistException.class,
        () -> catalog.dropPartition(tablePath, partitionSpec, false));

    String tablePathStr = catalog.inferTablePath(catalogPathStr, tablePath);
    Configuration flinkConf = TestConfigurations.getDefaultConf(tablePathStr);
    HoodieTableMetaClient metaClient = HoodieTestUtils
        .createMetaClient(
            new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(flinkConf)), tablePathStr);
    TestData.writeData(TestData.DATA_SET_INSERT, flinkConf);
    assertTrue(catalog.partitionExists(tablePath, partitionSpec));

    // drop partition 'par1'
    catalog.dropPartition(tablePath, partitionSpec, false);

    HoodieInstant latestInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().orElse(null);
    assertNotNull(latestInstant, "Delete partition commit should be completed");
    HoodieCommitMetadata commitMetadata = WriteProfiles.getCommitMetadata("tb1", new Path(tablePathStr), latestInstant, metaClient.getActiveTimeline());
    assertThat(commitMetadata, instanceOf(HoodieReplaceCommitMetadata.class));
    HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
    assertThat(replaceCommitMetadata.getPartitionToReplaceFileIds().size(), is(1));
    assertFalse(catalog.partitionExists(tablePath, partitionSpec));
  }
}
