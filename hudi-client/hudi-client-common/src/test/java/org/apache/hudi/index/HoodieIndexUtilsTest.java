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

package org.apache.hudi.index;

import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.config.HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for HoodieIndexUtils.
 */
public class HoodieIndexUtilsTest extends HoodieIndexUtils {
  public static final Schema SCHEMA = new Schema.Parser().parse(
      "{\"type\": \"record\",\"name\": \"trip\",\"namespace\": \"example.schema\",\"fields\": "
          + "[{\"name\": \"_row_key\",\"type\": \"string\"}]}\n");

  /**
   * Test for getKeygenAndUpdatedWriteConfig method when payload class is not ExpressionPayload.
   */
  @Test
  public void testGetKeygenAndUpdatedWriteConfigWithoutExpressionPayload() {
    // Setup
    TypedProperties props = new TypedProperties();
    props.setProperty(WRITE_PAYLOAD_CLASS_NAME.key(),
        "org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test"); // Add this line
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProperties(props).build();

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    // Test
    Pair<HoodieWriteConfig, Option<BaseKeyGenerator>> result =
        HoodieIndexUtils.getKeygenAndUpdatedWriteConfig(config, tableConfig);

    // Verify
    HoodieWriteConfig updatedConfig = result.getLeft();
    Option<BaseKeyGenerator> keyGeneratorOpt = result.getRight();

    assertEquals(config, updatedConfig);
    assertFalse(keyGeneratorOpt.isPresent());
  }

  @Test
  public void testGetKeygenAndUpdatedWriteConfig() {
    // Setup
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadConfig.PAYLOAD_CLASS_NAME.key(),
        "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty(WRITE_PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProperties(props).build();

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");

    // Test
    Pair<HoodieWriteConfig, Option<BaseKeyGenerator>> result =
        HoodieIndexUtils.getKeygenAndUpdatedWriteConfig(config, tableConfig);

    // Verify
    HoodieWriteConfig updatedConfig = result.getLeft();
    Option<BaseKeyGenerator> keyGeneratorOpt = result.getRight();

    assertEquals("org.apache.hudi.common.model.DefaultHoodieRecordPayload", updatedConfig.getWritePayloadClass());
    assertTrue(keyGeneratorOpt.isPresent());
    assertNotNull(keyGeneratorOpt.get());
  }

  @Test
  public void testGetKeygenAndUpdatedWriteConfigWithInvalidKeyGenerator() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty(HoodiePayloadConfig.PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    props.setProperty(WRITE_PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    // Set the KeyGenerator class to a class that does not extend BaseKeyGenerator
    props.setProperty(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), "org.apache.hudi.index.HoodieIndexUtilsTest.NotAKeyGenClass");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withProperties(props).build();
    // Mock HoodieTableConfig to return a payload class
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    // Now call the method
    RuntimeException ex = assertThrows(RuntimeException.class, () -> HoodieIndexUtils.getKeygenAndUpdatedWriteConfig(config, tableConfig));
    assertTrue(ex.getMessage().contains("KeyGenerator must inherit from BaseKeyGenerator to update a records partition path using spark sql merge into"));
  }

  @Test
  public void testMergeNoExistingRecord() {
    // Create sample incoming records and locations
    HoodieKey key1 = new HoodieKey("recordKey1", "partitionPath1");
    HoodieRecord<TestPayload> record1 = new HoodieAvroRecord<>(key1, new TestPayload("value1"));
    // Record with no existing location
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecord1 =
        Pair.of(record1, Option.empty());

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecord1);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/hudi").build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock HoodieIndexUtilsHelper.getExistingRecords using MockedStatic
    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return an empty HoodieData when getExistingRecords is called
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(HoodieListData.eager(Collections.emptyList()));

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // Perform assertions
      assertEquals(1, resultList.size());
      assertEquals(resultList.get(0).getRecordKey(), (key1.getRecordKey()));
      assertEquals(resultList.get(0).getPartitionPath(), (key1.getPartitionPath()));
    }
  }

  /**
   * Test mergeForPartitionUpdatesIfNeeded when the existing record is present and the incoming record is a delete.
   */
  @Test
  public void testMergeForPartitionUpdatesIfNeeded_IncomingDelete() throws Exception {
    HoodieKey key = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload deletePayload = new TestPayload("", true); // Delete payload
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key, deletePayload);

    // Existing location
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation(
        "partitionPath1", "001", "fileId1");
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.of(location));

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi").withSchema(String.valueOf(SCHEMA)).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("001", "fileId1"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // There should be one record in the result
      assertEquals(1, resultList.size());
      HoodieRecord<TestPayload> resultRecord = resultList.get(0);

      // Verify that the record is tagged to the old partition and location
      assertEquals(existingRecord.getPartitionPath(), resultRecord.getPartitionPath());
      assertEquals(existingRecord.getCurrentLocation(), resultRecord.getCurrentLocation());
      // Verify that the record is a delete
      assertTrue(resultRecord.isDelete(new Schema.Parser().parse(config.getWriteSchema()), config.getProps()));
    }
  }

  @Test
  public void testMergeForPartitionUpdatesIfNeeded_PartitionPathUpdated() throws Exception {
    HoodieKey key = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload deletePayload = new TestPayload("");
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key, deletePayload);

    // Existing location
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation(
        "partitionPath1", "001", "fileId1");
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.of(location));

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi").withSchema(String.valueOf(SCHEMA)).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieKey key2 = new HoodieKey("recordKey1", "partitionPath2");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key2, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("001", "fileId1"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // There should be one record in the result
      assertEquals(2, resultList.size());
      boolean foundDeleted = false;
      boolean foundUpdated = false;

      for (HoodieRecord<TestPayload> resultRecord : resultList) {
        if (resultRecord.isDelete(new Schema.Parser().parse(config.getWriteSchema()), config.getProps())) {
          assertEquals(resultRecord.getPartitionPath(), "partitionPath2");
          foundDeleted = true;
        } else {
          assertEquals(resultRecord.getPartitionPath(), "partitionPath1");
          foundUpdated = true;
        }
      }
      assertTrue(foundUpdated);
      assertTrue(foundDeleted);
    }
  }

  @Test
  public void testMergeForPartitionUpdatesIfNeeded_ValueUpdated() {
    HoodieKey key = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload deletePayload = new TestPayload("incoming");
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key, deletePayload);

    // Existing location
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation(
        "partitionPath1", "001", "fileId1");
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.of(location));

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi").withSchema(String.valueOf(SCHEMA)).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieKey key2 = new HoodieKey("recordKey1", "partitionPath1");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key2, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("002", "fileId1"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // There should be one record in the result
      assertEquals(1, resultList.size());
      assertEquals(resultList.get(0).getCurrentLocation().getInstantTime(), "002");
    }
  }

  @Test
  public void testMergeForPartitionUpdatesIfNeeded_MismatchRecordKey() {
    HoodieKey key1 = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload deletePayload = new TestPayload("incoming"); // Delete payload
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key1, deletePayload);

    // Existing location
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partitionPath1", "001", "fileId1");
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.of(location));

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/hudi").withSchema(String.valueOf(SCHEMA)).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieKey key2 = new HoodieKey("recordKey2", "partitionPath2");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key2, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("002", "fileId2"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // There should be one record in the result
      assertEquals(1, resultList.size());
      assertEquals(resultList.get(0).getData().value, "incoming");
    }
  }

  @Test
  public void testMergeForPartitionUpdatesIfNeeded2_BrandNewRecord() {
    HoodieKey key1 = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload deletePayload = new TestPayload("incoming"); // Delete payload
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key1, deletePayload);

    // No location
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.empty());

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/hudi").withSchema(String.valueOf(SCHEMA)).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieKey key2 = new HoodieKey("recordKey2", "partitionPath2");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key2, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("002", "fileId2"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // There should be one record in the result
      assertEquals(1, resultList.size());
      assertEquals(resultList.get(0).getData().value, "incoming");
    }
  }

  /**
   * Test mergeForPartitionUpdatesIfNeeded when merge results in an updated record with the same partition.
   */
  @Test
  public void testMergeForPartitionUpdatesIfNeeded_MergeWithExprPayload() {

    // Create sample incoming record, and existing location is present
    HoodieKey key = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload incomingPayload = new TestPayload("incoming_value", false, false);
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key, incomingPayload);

    // Existing location
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partitionPath1", "001", "fileId1");
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.of(location));

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig that generates non empty key gen class.
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadConfig.PAYLOAD_CLASS_NAME.key(),
        "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty(HoodiePayloadConfig.PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    props.setProperty(WRITE_PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi")
        .withSchema(String.valueOf(SCHEMA))
        .withProperties(props).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("002", "fileId1"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      assertEquals(2, resultList.size());
      HoodieRecord<TestPayload> resultRecord = resultList.get(0);

      assertEquals(existingRecord.getPartitionPath(), resultRecord.getPartitionPath());
      assertEquals(existingRecord.getCurrentLocation(), resultRecord.getCurrentLocation());
    }
  }

  /**
   * Test mergeForPartitionUpdatesIfNeeded when merge results in an updated record with the same partition.
   */
  @Test
  public void testMergeForPartitionUpdatesIfNeeded_MergeWithExprPayloadDelete() {

    // Create sample incoming record, and existing location is present
    HoodieKey key = new HoodieKey("recordKey1", "partitionPath1");
    TestPayload incomingPayload = new TestPayload("incoming_value", false, true);
    HoodieRecord<TestPayload> incomingRecord = new HoodieAvroRecord<>(key, incomingPayload);

    // Existing location
    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation("partitionPath1", "001", "fileId1");
    Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>> incomingRecordPair =
        Pair.of(incomingRecord, Option.of(location));

    List<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingList = Collections.singletonList(incomingRecordPair);
    HoodieData<Pair<HoodieRecord<TestPayload>, Option<HoodieRecordGlobalLocation>>> incomingRecordsAndLocations =
        HoodieListData.eager(incomingList);

    // Create a HoodieWriteConfig that generates non empty key gen class.
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodiePayloadConfig.PAYLOAD_CLASS_NAME.key(),
        "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty(HoodiePayloadConfig.PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");
    props.setProperty(WRITE_PAYLOAD_CLASS_NAME.key(), "org.apache.spark.sql.hudi.command.payload.ExpressionPayload");

    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi")
        .withSchema(String.valueOf(SCHEMA))
        .withProperties(props).build();

    // Mock HoodieTable and its methods
    HoodieTable hoodieTable = mock(HoodieTable.class);
    mockHoodieTable(hoodieTable);

    // Mock getExistingRecords to return the existing record
    TestPayload existingPayload = new TestPayload("existing_value");
    HoodieRecord<TestPayload> existingRecord = new HoodieAvroRecord<>(key, existingPayload);
    existingRecord.setCurrentLocation(new HoodieRecordLocation("001", "fileId1"));

    try (MockedStatic<HoodieIndexUtilsHelper> mockedStatic = Mockito.mockStatic(HoodieIndexUtilsHelper.class)) {
      // Return existingRecord when getExistingRecords is called
      HoodieData<HoodieRecord<TestPayload>> existingRecords = HoodieListData.eager(Collections.singletonList(existingRecord));
      mockedStatic.when(() -> HoodieIndexUtilsHelper.getExistingRecords(any(), any(), any()))
          .thenReturn(existingRecords);

      // Call the method
      HoodieData<HoodieRecord<TestPayload>> result = HoodieIndexUtils.mergeForPartitionUpdatesIfNeeded(
          incomingRecordsAndLocations, config, hoodieTable);

      // Collect the result and assert
      List<HoodieRecord<TestPayload>> resultList = result.collectAsList();
      // There should be one record in the result
      assertEquals(1, resultList.size());
      HoodieRecord<TestPayload> resultRecord = resultList.get(0);

      // Verify that the record is tagged to the current location
      assertEquals(existingRecord.getPartitionPath(), resultRecord.getPartitionPath());
      assertEquals(existingRecord.getCurrentLocation(), resultRecord.getCurrentLocation());
      assertEquals(existingRecord.getData().value, "existing_value");
    }
  }

  /**
   * A simple test payload implementing HoodieRecordPayload.
   */
  private static class TestPayload implements HoodieRecordPayload<TestPayload> {
    public final String value;
    private final boolean isDelete;
    private final boolean combineReturnEmpty;

    public TestPayload(String value) {
      this.value = value;
      this.isDelete = false;
      this.combineReturnEmpty = true;
    }

    public TestPayload(String value, boolean isDelete) {
      this.value = value;
      this.isDelete = isDelete;
      this.combineReturnEmpty = true;
    }

    public TestPayload(String value, boolean isDelete, boolean combineReturnEmpty) {
      this.value = value;
      this.isDelete = false;
      this.combineReturnEmpty = combineReturnEmpty;
    }

    @Override
    public TestPayload preCombine(TestPayload another) {
      return this;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
      TestIndexedRecord r = new TestIndexedRecord();
      r.put(0, currentValue.get(0) + value);
      return combineReturnEmpty ? Option.empty() : Option.of(r);
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) {
      TestIndexedRecord r = new TestIndexedRecord();
      r.put(0, "1");
      return isDelete ? Option.empty() : Option.of(r);
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private static class TestIndexedRecord implements GenericRecord {
    private String val;

    @Override
    public Schema getSchema() {
      return SCHEMA;
    }

    @Override
    public void put(int i, Object v) {
      val = (String) v;
    }

    @Override
    public Object get(int i) {
      return val;
    }

    @Override
    public void put(String key, Object v) {
      val = (String) v;
    }

    @Override
    public Object get(String key) {
      return val;
    }
  }

  private class NotAKeyGenClass {
    NotAKeyGenClass(TypedProperties props) {
    }
  }

  static void mockHoodieTable(HoodieTable hoodieTable) {
    try {
      org.apache.hudi.common.table.HoodieTableMetaClient metaClient = mock(org.apache.hudi.common.table.HoodieTableMetaClient.class);
      org.apache.hudi.common.table.HoodieTableConfig tableConfig = mock(org.apache.hudi.common.table.HoodieTableConfig.class);
      org.apache.hudi.common.table.timeline.HoodieActiveTimeline activeTimeline = mock(org.apache.hudi.common.table.timeline.HoodieActiveTimeline.class);

      when(hoodieTable.getMetaClient()).thenReturn(metaClient);
      when(metaClient.getTableConfig()).thenReturn(tableConfig);
      when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.DefaultHoodieRecordPayload");
      when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
      when(activeTimeline.filterCompletedInstants()).thenReturn(activeTimeline);
      when(activeTimeline.lastInstant()).thenReturn(Option.empty());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}