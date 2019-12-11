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

package org.apache.hudi.hive;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.apache.hudi.hive.HoodieHiveClient.PartitionEvent;
import org.apache.hudi.hive.HoodieHiveClient.PartitionEvent.PartitionEventType;
import org.apache.hudi.hive.util.SchemaUtil;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ConstantConditions")
@RunWith(Parameterized.class)
public class TestHiveSyncTool {

  // Test sync tool using both jdbc and metastore client
  private boolean useJdbc;

  public TestHiveSyncTool(Boolean useJdbc) {
    this.useJdbc = useJdbc;
  }

  @Parameterized.Parameters(name = "UseJdbc")
  public static Collection<Boolean[]> data() {
    return Arrays.asList(new Boolean[][] {{false}, {true}});
  }

  @Before
  public void setUp() throws IOException, InterruptedException, URISyntaxException {
    TestUtil.setUp();
  }

  @After
  public void teardown() throws IOException, InterruptedException {
    TestUtil.clear();
  }

  /**
   * Testing converting array types to Hive field declaration strings. According to the Parquet-113 spec:
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
   */
  @Test
  public void testSchemaConvertArray() throws IOException {
    // Testing the 3-level annotation structure
    MessageType schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .optional(PrimitiveType.PrimitiveTypeName.INT32).named("element").named("list").named("int_list")
        .named("ArrayOfInts");

    String schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list` ARRAY< int>", schemaString);

    // A array of arrays
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup().requiredGroup()
        .as(OriginalType.LIST).repeatedGroup().required(PrimitiveType.PrimitiveTypeName.INT32).named("element")
        .named("list").named("element").named("list").named("int_list_list").named("ArrayOfArrayOfInts");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list_list` ARRAY< ARRAY< int>>", schemaString);

    // A list of integers
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeated(PrimitiveType.PrimitiveTypeName.INT32)
        .named("element").named("int_list").named("ArrayOfInts");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`int_list` ARRAY< int>", schemaString);

    // A list of structs with two fields
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("num").named("element").named("tuple_list").named("ArrayOfTuples");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`tuple_list` ARRAY< STRUCT< `str` : binary, `num` : int>>", schemaString);

    // A list of structs with a single field
    // For this case, since the inner group name is "array", we treat the
    // element type as a one-element struct.
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").named("array").named("one_tuple_list")
        .named("ArrayOfOneTuples");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

    // A list of structs with a single field
    // For this case, since the inner group name ends with "_tuple", we also treat the
    // element type as a one-element struct.
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").named("one_tuple_list_tuple")
        .named("one_tuple_list").named("ArrayOfOneTuples2");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< STRUCT< `str` : binary>>", schemaString);

    // A list of structs with a single field
    // Unlike the above two cases, for this the element type is the type of the
    // only field in the struct.
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup()
        .required(PrimitiveType.PrimitiveTypeName.BINARY).named("str").named("one_tuple_list").named("one_tuple_list")
        .named("ArrayOfOneTuples3");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`one_tuple_list` ARRAY< binary>", schemaString);

    // A list of maps
    schema = Types.buildMessage().optionalGroup().as(OriginalType.LIST).repeatedGroup().as(OriginalType.MAP)
        .repeatedGroup().as(OriginalType.MAP_KEY_VALUE).required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.UTF8).named("string_key").required(PrimitiveType.PrimitiveTypeName.INT32).named("int_value")
        .named("key_value").named("array").named("map_list").named("ArrayOfMaps");

    schemaString = SchemaUtil.generateSchemaString(schema);
    assertEquals("`map_list` ARRAY< MAP< string, int>>", schemaString);
  }

  @Test
  public void testBasicSync() throws Exception {
    TestUtil.hiveSyncConfig.useJdbc = this.useJdbc;
    String commitTime = "100";
    TestUtil.createCOWDataset(commitTime, 5);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    assertFalse("Table " + TestUtil.hiveSyncConfig.tableName + " should not exist initially",
        hiveClient.doesTableExist());
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue("Table " + TestUtil.hiveSyncConfig.tableName + " should exist after sync completes",
        hiveClient.doesTableExist());
    assertEquals("Hive Schema should match the dataset schema + partition field", hiveClient.getTableSchema().size(),
        hiveClient.getDataSchema().getColumns().size() + 1);
    assertEquals("Table partitions should match the number of partitions we wrote", 5,
        hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be updated in the TBLPROPERTIES", commitTime,
        hiveClient.getLastCommitTimeSynced().get());
  }

  @Test
  public void testSyncIncremental() throws Exception {
    TestUtil.hiveSyncConfig.useJdbc = this.useJdbc;
    String commitTime1 = "100";
    TestUtil.createCOWDataset(commitTime1, 5);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();
    assertEquals("Table partitions should match the number of partitions we wrote", 5,
        hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be updated in the TBLPROPERTIES", commitTime1,
        hiveClient.getLastCommitTimeSynced().get());

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "101";
    TestUtil.addCOWPartitions(1, true, dateTime, commitTime2);

    // Lets do the sync
    hiveClient = new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(commitTime1));
    assertEquals("We should have one partition written after 100 commit", 1, writtenPartitionsSince.size());
    List<Partition> hivePartitions = hiveClient.scanTablePartitions();
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals("There should be only one paritition event", 1, partitionEvents.size());
    assertEquals("The one partition event must of type ADD", PartitionEventType.ADD,
        partitionEvents.iterator().next().eventType);

    tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();
    // Sync should add the one partition
    assertEquals("The one partition we wrote should be added to hive", 6, hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be 101", commitTime2,
        hiveClient.getLastCommitTimeSynced().get());
  }

  @Test
  public void testSyncIncrementalWithSchemaEvolution() throws Exception {
    TestUtil.hiveSyncConfig.useJdbc = this.useJdbc;
    String commitTime1 = "100";
    TestUtil.createCOWDataset(commitTime1, 5);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();

    int fields = hiveClient.getTableSchema().size();

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "101";
    TestUtil.addCOWPartitions(1, false, dateTime, commitTime2);

    // Lets do the sync
    tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();

    assertEquals("Hive Schema has evolved and should not be 3 more field", fields + 3,
        hiveClient.getTableSchema().size());
    assertEquals("Hive Schema has evolved - Field favorite_number has evolved from int to long", "BIGINT",
        hiveClient.getTableSchema().get("favorite_number"));
    assertTrue("Hive Schema has evolved - Field favorite_movie was added",
        hiveClient.getTableSchema().containsKey("favorite_movie"));

    // Sync should add the one partition
    assertEquals("The one partition we wrote should be added to hive", 6, hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be 101", commitTime2,
        hiveClient.getLastCommitTimeSynced().get());
  }

  @Test
  public void testSyncMergeOnRead() throws Exception {
    TestUtil.hiveSyncConfig.useJdbc = this.useJdbc;
    String commitTime = "100";
    String deltaCommitTime = "101";
    TestUtil.createMORDataset(commitTime, deltaCommitTime, 5);
    HoodieHiveClient hiveClient =
        new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    assertFalse("Table " + TestUtil.hiveSyncConfig.tableName + " should not exist initially",
        hiveClient.doesTableExist());
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue("Table " + TestUtil.hiveSyncConfig.tableName + " should exist after sync completes",
        hiveClient.doesTableExist());
    assertEquals("Hive Schema should match the dataset schema + partition field", hiveClient.getTableSchema().size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + 1);
    assertEquals("Table partitions should match the number of partitions we wrote", 5,
        hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be updated in the TBLPROPERTIES", deltaCommitTime,
        hiveClient.getLastCommitTimeSynced().get());

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    TestUtil.addCOWPartitions(1, true, dateTime, commitTime2);
    TestUtil.addMORPartitions(1, true, false, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClient = new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);

    assertEquals("Hive Schema should match the evolved dataset schema + partition field",
        hiveClient.getTableSchema().size(), SchemaTestUtil.getEvolvedSchema().getFields().size() + 1);
    // Sync should add the one partition
    assertEquals("The 2 partitions we wrote should be added to hive", 6, hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be 103", deltaCommitTime2,
        hiveClient.getLastCommitTimeSynced().get());
  }

  @Test
  public void testSyncMergeOnReadRT() throws Exception {
    TestUtil.hiveSyncConfig.useJdbc = this.useJdbc;
    String commitTime = "100";
    String deltaCommitTime = "101";
    String roTablename = TestUtil.hiveSyncConfig.tableName;
    TestUtil.hiveSyncConfig.tableName = TestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_REALTIME_TABLE;
    TestUtil.createMORDataset(commitTime, deltaCommitTime, 5);
    HoodieHiveClient hiveClientRT =
        new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);

    assertFalse("Table " + TestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_REALTIME_TABLE
        + " should not exist initially", hiveClientRT.doesTableExist());

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue("Table " + TestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_REALTIME_TABLE
        + " should exist after sync completes", hiveClientRT.doesTableExist());

    assertEquals("Hive Schema should match the dataset schema + partition field", hiveClientRT.getTableSchema().size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + 1);
    assertEquals("Table partitions should match the number of partitions we wrote", 5,
        hiveClientRT.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be updated in the TBLPROPERTIES", deltaCommitTime,
        hiveClientRT.getLastCommitTimeSynced().get());

    // Now lets create more parititions and these are the only ones which needs to be synced
    DateTime dateTime = DateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    TestUtil.addCOWPartitions(1, true, dateTime, commitTime2);
    TestUtil.addMORPartitions(1, true, false, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClientRT = new HoodieHiveClient(TestUtil.hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);

    assertEquals("Hive Schema should match the evolved dataset schema + partition field",
        hiveClientRT.getTableSchema().size(), SchemaTestUtil.getEvolvedSchema().getFields().size() + 1);
    // Sync should add the one partition
    assertEquals("The 2 partitions we wrote should be added to hive", 6, hiveClientRT.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be 103", deltaCommitTime2,
        hiveClientRT.getLastCommitTimeSynced().get());
    TestUtil.hiveSyncConfig.tableName = roTablename;
  }

  @Test
  public void testMultiPartitionKeySync() throws Exception {
    TestUtil.hiveSyncConfig.useJdbc = this.useJdbc;
    String commitTime = "100";
    TestUtil.createCOWDataset(commitTime, 5);

    HiveSyncConfig hiveSyncConfig = HiveSyncConfig.copy(TestUtil.hiveSyncConfig);
    hiveSyncConfig.partitionValueExtractorClass = MultiPartKeysValueExtractor.class.getCanonicalName();
    hiveSyncConfig.tableName = "multi_part_key";
    hiveSyncConfig.partitionFields = Lists.newArrayList("year", "month", "day");
    TestUtil.getCreatedTablesSet().add(hiveSyncConfig.databaseName + "." + hiveSyncConfig.tableName);

    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    assertFalse("Table " + hiveSyncConfig.tableName + " should not exist initially", hiveClient.doesTableExist());
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, TestUtil.getHiveConf(), TestUtil.fileSystem);
    tool.syncHoodieTable();
    assertTrue("Table " + hiveSyncConfig.tableName + " should exist after sync completes", hiveClient.doesTableExist());
    assertEquals("Hive Schema should match the dataset schema + partition fields", hiveClient.getTableSchema().size(),
        hiveClient.getDataSchema().getColumns().size() + 3);
    assertEquals("Table partitions should match the number of partitions we wrote", 5,
        hiveClient.scanTablePartitions().size());
    assertEquals("The last commit that was sycned should be updated in the TBLPROPERTIES", commitTime,
        hiveClient.getLastCommitTimeSynced().get());
  }
}
