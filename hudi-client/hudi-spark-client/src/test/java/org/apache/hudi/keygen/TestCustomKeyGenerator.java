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

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TestCustomKeyGenerator extends KeyGeneratorTestUtilities {

  /**
   * Method to create props used for common cases.
   *
   * @param getComplexRecordKey      Use complex record key or not
   * @param useKeyGeneratorClassName Use KeyGenerator class name initialize KeyGenerator or not.
   *                                 true use {@code HoodieWriteConfig.KEYGENERATOR_CLASS_PROP},
   *                                 false use {@code HoodieWriteConfig.KEYGENERATOR_TYPE_PROP}
   * @return TypedProperties used to initialize KeyGenerator.
   */
  private TypedProperties getCommonProps(boolean getComplexRecordKey, boolean useKeyGeneratorClassName) {
    TypedProperties properties = new TypedProperties();
    if (getComplexRecordKey) {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key, pii_col");
    } else {
      properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    }
    if (useKeyGeneratorClassName) {
      properties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), CustomKeyGenerator.class.getName());
    } else {
      properties.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.CUSTOM.name());
    }
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    return properties;
  }

  private TypedProperties getPropertiesForSimpleKeyGen(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(false, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp:simple");
    return properties;
  }

  private TypedProperties getImproperPartitionFieldFormatProp(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(false, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp");
    return properties;
  }

  private TypedProperties getInvalidPartitionKeyTypeProps(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(false, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp:dummy");
    return properties;
  }

  private TypedProperties getComplexRecordKeyWithSimplePartitionProps(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(true, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp:simple");
    return properties;
  }

  private TypedProperties getComplexRecordKeyAndPartitionPathProps(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(true, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp:simple,ts_ms:timestamp");
    populateNecessaryPropsForTimestampBasedKeyGen(properties);
    return properties;
  }

  private TypedProperties getPropsWithoutRecordKeyFieldProps(boolean useKeyGeneratorClassName) {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "timestamp:simple");
    if (useKeyGeneratorClassName) {
      properties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), CustomKeyGenerator.class.getName());
    } else {
      properties.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.CUSTOM.name());
    }
    return properties;
  }

  private void populateNecessaryPropsForTimestampBasedKeyGen(TypedProperties properties) {
    properties.put("hoodie.deltastreamer.keygen.timebased.timestamp.type", "DATE_STRING");
    properties.put("hoodie.deltastreamer.keygen.timebased.input.dateformat", "yyyy-MM-dd");
    properties.put("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
  }

  private TypedProperties getPropertiesForTimestampBasedKeyGen(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(false, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "ts_ms:timestamp");
    populateNecessaryPropsForTimestampBasedKeyGen(properties);
    return properties;
  }

  private TypedProperties getPropertiesForNonPartitionedKeyGen(boolean useKeyGeneratorClassName) {
    TypedProperties properties = getCommonProps(false, useKeyGeneratorClassName);
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    return properties;
  }

  private String stackTraceToString(Throwable e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw.toString();
  }

  @Test
  void testSimpleKeyGeneratorWithKeyGeneratorClass() {
    testSimpleKeyGenerator(getPropertiesForSimpleKeyGen(true));
  }

  @Test
  void testSimpleKeyGeneratorWithKeyGeneratorType() {
    testSimpleKeyGenerator(getPropertiesForSimpleKeyGen(false));
  }

  public void testSimpleKeyGenerator(TypedProperties props) {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("key1", key.getRecordKey());
    assertEquals("timestamp=4357686", key.getPartitionPath());
    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("key1", keyGenerator.getRecordKey(row));
    assertEquals("timestamp=4357686", keyGenerator.getPartitionPath(row));
    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    assertEquals(UTF8String.fromString("timestamp=4357686"), keyGenerator.getPartitionPath(internalRow, row.schema()));
  }

  @Test
  void testTimestampBasedKeyGeneratorWithKeyGeneratorClass() {
    testTimestampBasedKeyGenerator(getPropertiesForTimestampBasedKeyGen(true));
  }

  @Test
  void testTimestampBasedKeyGeneratorWithKeyGeneratorType() {
    testTimestampBasedKeyGenerator(getPropertiesForTimestampBasedKeyGen(false));
  }

  @Test
  void testCustomKeyGeneratorPartitionType() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS.key(), "random:simple,ts_ms:timestamp");
    Object[] partitionTypes = CustomAvroKeyGenerator.getPartitionTypes(tableConfig).toArray();
    assertArrayEquals(new CustomAvroKeyGenerator.PartitionKeyType[] {CustomAvroKeyGenerator.PartitionKeyType.SIMPLE, CustomAvroKeyGenerator.PartitionKeyType.TIMESTAMP}, partitionTypes);
    Pair<String, Option<CustomAvroKeyGenerator.PartitionKeyType>> partitionFieldAndType = CustomAvroKeyGenerator.getPartitionFieldAndKeyType("random:simple");
    assertEquals(Pair.of("random", Option.of(CustomAvroKeyGenerator.PartitionKeyType.SIMPLE)), partitionFieldAndType);
  }

  @Test
  void testCustomKeyGeneratorTimestampFieldsAPI() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS.key(), "simple1:simple,ts1:timestamp,ts2:timestamp");
    Object[] timestampFields = CustomAvroKeyGenerator.getTimestampFields(tableConfig).get().toArray();
    // Only two timestamp fields are returned
    assertArrayEquals(new String[] {"ts1", "ts2"}, timestampFields);

    tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS.key(), "simple1,ts1,ts2");
    Option<?> timestampFieldsOpt = CustomAvroKeyGenerator.getTimestampFields(tableConfig);
    // Empty option is returned since no partition type is available
    assertTrue(timestampFieldsOpt.isEmpty());

    tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS.key(), "simple1:simple,simple2:simple,simple3:simple");
    timestampFields = CustomAvroKeyGenerator.getTimestampFields(tableConfig).get().toArray();
    // No timestamp partitions
    assertEquals(0, timestampFields.length);
  }

  public void testTimestampBasedKeyGenerator(TypedProperties props) {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("key1", key.getRecordKey());
    assertEquals("ts_ms=20200321", key.getPartitionPath());
    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("key1", keyGenerator.getRecordKey(row));
    assertEquals("ts_ms=20200321", keyGenerator.getPartitionPath(row));
    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    assertEquals(UTF8String.fromString("ts_ms=20200321"), keyGenerator.getPartitionPath(internalRow, row.schema()));
  }

  @Test
  void testNonPartitionedKeyGeneratorWithKeyGeneratorClass() {
    testNonPartitionedKeyGenerator(getPropertiesForNonPartitionedKeyGen(true));
  }

  @Test
  void testNonPartitionedKeyGeneratorWithKeyGeneratorType() {
    testNonPartitionedKeyGenerator(getPropertiesForNonPartitionedKeyGen(false));
  }

  public void testNonPartitionedKeyGenerator(TypedProperties props) {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("key1", key.getRecordKey());
    assertTrue(key.getPartitionPath().isEmpty());
    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("key1", keyGenerator.getRecordKey(row));
    assertTrue(keyGenerator.getPartitionPath(row).isEmpty());

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    assertEquals(0, keyGenerator.getPartitionPath(internalRow, row.schema()).numBytes());
  }

  @Test
  void testInvalidPartitionKeyTypeWithKeyGeneratorClass() {
    testInvalidPartitionKeyType(getInvalidPartitionKeyTypeProps(true));
  }

  @Test
  void testInvalidPartitionKeyTypeWithKeyGeneratorType() {
    testInvalidPartitionKeyType(getInvalidPartitionKeyTypeProps(false));
  }

  public void testInvalidPartitionKeyType(TypedProperties props) {
    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      keyGenerator.getKey(getRecord());
      fail("should fail when invalid PartitionKeyType is provided!");
    } catch (Exception e) {
      assertTrue(getNestedConstructorErrorCause(e).getMessage().contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"));
    }

    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      GenericRecord avroRecord = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
      keyGenerator.getPartitionPath(row);
      fail("should fail when invalid PartitionKeyType is provided!");
    } catch (Exception e) {
      assertTrue(getNestedConstructorErrorCause(e).getMessage().contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"));
    }
  }

  @Test
  void testNoRecordKeyFieldPropWithKeyGeneratorClass() {
    testNoRecordKeyFieldProp(true);
  }

  @Test
  void testNoRecordKeyFieldPropWithKeyGeneratorType() {
    testNoRecordKeyFieldProp(false);
  }

  public void testNoRecordKeyFieldProp(boolean useKeyGeneratorClassName) {
    TypedProperties propsWithoutRecordKeyFieldProps = getPropsWithoutRecordKeyFieldProps(useKeyGeneratorClassName);
    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(propsWithoutRecordKeyFieldProps);

      keyGenerator.getKey(getRecord());
      fail("should fail when record key field is not provided!");
    } catch (Exception e) {
      if (useKeyGeneratorClassName) {
        // "Property hoodie.datasource.write.recordkey.field not found" exception cause CustomKeyGenerator init fail
        assertTrue(e.getMessage()
            .contains("Unable to find field names for record key in cfg"));
      } else {
        assertTrue(stackTraceToString(e).contains("Unable to find field names for record key in cfg"));
      }

    }

    try {
      BuiltinKeyGenerator keyGenerator = new CustomKeyGenerator(propsWithoutRecordKeyFieldProps);

      GenericRecord avroRecord = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
      keyGenerator.getRecordKey(row);
      fail("should fail when record key field is not provided!");
    } catch (Exception e) {
      if (useKeyGeneratorClassName) {
        // "Property hoodie.datasource.write.recordkey.field not found" exception cause CustomKeyGenerator init fail
        assertTrue(e.getMessage()
            .contains("All of the values for ([]) were either null or empty"));
      } else {
        assertTrue(stackTraceToString(e).contains("All of the values for ([]) were either null or empty"));
      }
    }
  }

  @Test
  void testPartitionFieldsInImproperFormatWithKeyGeneratorClass() {
    testPartitionFieldsInImproperFormat(getImproperPartitionFieldFormatProp(true));
  }

  @Test
  void testPartitionFieldsInImproperFormatWithKeyGeneratorType() {
    testPartitionFieldsInImproperFormat(getImproperPartitionFieldFormatProp(false));
  }

  public void testPartitionFieldsInImproperFormat(TypedProperties props) {
    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      keyGenerator.getKey(getRecord());
      fail("should fail when partition key field is provided in improper format!");
    } catch (Exception e) {
      assertTrue(getNestedConstructorErrorCause(e).getMessage().contains("Unable to find field names for partition path in proper format"));
    }

    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      GenericRecord avroRecord = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
      keyGenerator.getPartitionPath(row);
      fail("should fail when partition key field is provided in improper format!");
    } catch (Exception e) {
      assertTrue(getNestedConstructorErrorCause(e).getMessage().contains("Unable to find field names for partition path in proper format"));
    }
  }

  @Test
  void testComplexRecordKeyWithSimplePartitionPathWithKeyGeneratorClass() {
    testComplexRecordKeyWithSimplePartitionPath(getComplexRecordKeyWithSimplePartitionProps(true));
  }

  @Test
  void testComplexRecordKeyWithSimplePartitionPathWithKeyGeneratorType() {
    testComplexRecordKeyWithSimplePartitionPath(getComplexRecordKeyWithSimplePartitionProps(false));
  }

  public void testComplexRecordKeyWithSimplePartitionPath(TypedProperties props) {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", key.getRecordKey());
    assertEquals("timestamp=4357686", key.getPartitionPath());

    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", keyGenerator.getRecordKey(row));
    assertEquals("timestamp=4357686", keyGenerator.getPartitionPath(row));

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    assertEquals(UTF8String.fromString("timestamp=4357686"), keyGenerator.getPartitionPath(internalRow, row.schema()));
  }

  @Test
  void testComplexRecordKeysWithComplexPartitionPathWithKeyGeneratorClass() {
    testComplexRecordKeysWithComplexPartitionPath(getComplexRecordKeyAndPartitionPathProps(true));
  }

  @Test
  void testComplexRecordKeysWithComplexPartitionPathWithKeyGeneratorType() {
    testComplexRecordKeysWithComplexPartitionPath(getComplexRecordKeyAndPartitionPathProps(false));
  }

  public void testComplexRecordKeysWithComplexPartitionPath(TypedProperties props) {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord avroRecord = getRecord();
    HoodieKey key = keyGenerator.getKey(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", key.getRecordKey());
    assertEquals("timestamp=4357686/ts_ms=20200321", key.getPartitionPath());

    Row row = KeyGeneratorTestUtilities.getRow(avroRecord);
    assertEquals("_row_key:key1,pii_col:pi", keyGenerator.getRecordKey(row));
    assertEquals("timestamp=4357686/ts_ms=20200321", keyGenerator.getPartitionPath(row));

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    assertEquals(UTF8String.fromString("timestamp=4357686/ts_ms=20200321"), keyGenerator.getPartitionPath(internalRow, row.schema()));
  }

  private static Throwable getNestedConstructorErrorCause(Exception e) {
    // custom key generator will fail in the constructor, and we must unwrap the cause for asserting error messages
    return e.getCause().getCause();
  }
}
