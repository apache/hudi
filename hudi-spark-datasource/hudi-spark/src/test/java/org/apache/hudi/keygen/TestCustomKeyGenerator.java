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

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

public class TestCustomKeyGenerator extends KeyGeneratorTestUtilities {

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
  public void testSimpleKeyGeneratorWithKeyGeneratorClass() throws IOException {
    testSimpleKeyGenerator(getPropertiesForSimpleKeyGen(true));
  }

  @Test
  public void testSimpleKeyGeneratorWithKeyGeneratorType() throws IOException {
    testSimpleKeyGenerator(getPropertiesForSimpleKeyGen(false));
  }

  public void testSimpleKeyGenerator(TypedProperties props) throws IOException {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686");
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686");
    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(keyGenerator.getPartitionPath(internalRow, row.schema()), "timestamp=4357686");
  }

  @Test
  public void testTimestampBasedKeyGeneratorWithKeyGeneratorClass() throws IOException {
    testTimestampBasedKeyGenerator(getPropertiesForTimestampBasedKeyGen(true));
  }

  @Test
  public void testTimestampBasedKeyGeneratorWithKeyGeneratorType() throws IOException {
    testTimestampBasedKeyGenerator(getPropertiesForTimestampBasedKeyGen(false));
  }

  public void testTimestampBasedKeyGenerator(TypedProperties props) throws IOException {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertEquals(key.getPartitionPath(), "ts_ms=20200321");
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "ts_ms=20200321");
    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(keyGenerator.getPartitionPath(internalRow, row.schema()), "ts_ms=20200321");
  }

  @Test
  public void testNonPartitionedKeyGeneratorWithKeyGeneratorClass() throws IOException {
    testNonPartitionedKeyGenerator(getPropertiesForNonPartitionedKeyGen(true));
  }

  @Test
  public void testNonPartitionedKeyGeneratorWithKeyGeneratorType() throws IOException {
    testNonPartitionedKeyGenerator(getPropertiesForNonPartitionedKeyGen(false));
  }

  public void testNonPartitionedKeyGenerator(TypedProperties props) throws IOException {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "key1");
    Assertions.assertTrue(key.getPartitionPath().isEmpty());
    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "key1");
    Assertions.assertTrue(keyGenerator.getPartitionPath(row).isEmpty());

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertTrue(keyGenerator.getPartitionPath(internalRow, row.schema()).isEmpty());
  }

  @Test
  public void testInvalidPartitionKeyTypeWithKeyGeneratorClass() {
    testInvalidPartitionKeyType(getInvalidPartitionKeyTypeProps(true));
  }

  @Test
  public void testInvalidPartitionKeyTypeWithKeyGeneratorType() {
    testInvalidPartitionKeyType(getInvalidPartitionKeyTypeProps(false));
  }

  public void testInvalidPartitionKeyType(TypedProperties props) {
    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      keyGenerator.getKey(getRecord());
      Assertions.fail("should fail when invalid PartitionKeyType is provided!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"));
    }

    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      GenericRecord record = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(record);
      keyGenerator.getPartitionPath(row);
      Assertions.fail("should fail when invalid PartitionKeyType is provided!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("No enum constant org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType.DUMMY"));
    }
  }

  @Test
  public void testNoRecordKeyFieldPropWithKeyGeneratorClass() {
    testNoRecordKeyFieldProp(true);
  }

  @Test
  public void testNoRecordKeyFieldPropWithKeyGeneratorType() {
    testNoRecordKeyFieldProp(false);
  }

  public void testNoRecordKeyFieldProp(boolean useKeyGeneratorClassName) {
    TypedProperties propsWithoutRecordKeyFieldProps = getPropsWithoutRecordKeyFieldProps(useKeyGeneratorClassName);
    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(propsWithoutRecordKeyFieldProps);

      keyGenerator.getKey(getRecord());
      Assertions.fail("should fail when record key field is not provided!");
    } catch (Exception e) {
      if (useKeyGeneratorClassName) {
        // "Property hoodie.datasource.write.recordkey.field not found" exception cause CustomKeyGenerator init fail
        Assertions.assertTrue(e
            .getCause()
            .getCause()
            .getCause()
            .getMessage()
            .contains("Property hoodie.datasource.write.recordkey.field not found"));
      } else {
        Assertions.assertTrue(stackTraceToString(e).contains("Property hoodie.datasource.write.recordkey.field not found"));
      }

    }

    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(propsWithoutRecordKeyFieldProps);

      GenericRecord record = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(record);
      keyGenerator.getRecordKey(row);
      Assertions.fail("should fail when record key field is not provided!");
    } catch (Exception e) {
      if (useKeyGeneratorClassName) {
        // "Property hoodie.datasource.write.recordkey.field not found" exception cause CustomKeyGenerator init fail
        Assertions.assertTrue(e
            .getCause()
            .getCause()
            .getCause()
            .getMessage()
            .contains("Property hoodie.datasource.write.recordkey.field not found"));
      } else {
        Assertions.assertTrue(stackTraceToString(e).contains("Property hoodie.datasource.write.recordkey.field not found"));
      }
    }
  }

  @Test
  public void testPartitionFieldsInImproperFormatWithKeyGeneratorClass() {
    testPartitionFieldsInImproperFormat(getImproperPartitionFieldFormatProp(true));
  }

  @Test
  public void testPartitionFieldsInImproperFormatWithKeyGeneratorType() {
    testPartitionFieldsInImproperFormat(getImproperPartitionFieldFormatProp(false));
  }

  public void testPartitionFieldsInImproperFormat(TypedProperties props) {
    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      keyGenerator.getKey(getRecord());
      Assertions.fail("should fail when partition key field is provided in improper format!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Unable to find field names for partition path in proper format"));
    }

    try {
      BuiltinKeyGenerator keyGenerator =
          (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

      GenericRecord record = getRecord();
      Row row = KeyGeneratorTestUtilities.getRow(record);
      keyGenerator.getPartitionPath(row);
      Assertions.fail("should fail when partition key field is provided in improper format!");
    } catch (Exception e) {
      Assertions.assertTrue(e.getMessage().contains("Unable to find field names for partition path in proper format"));
    }
  }

  @Test
  public void testComplexRecordKeyWithSimplePartitionPathWithKeyGeneratorClass() throws IOException {
    testComplexRecordKeyWithSimplePartitionPath(getComplexRecordKeyWithSimplePartitionProps(true));
  }

  @Test
  public void testComplexRecordKeyWithSimplePartitionPathWithKeyGeneratorType() throws IOException {
    testComplexRecordKeyWithSimplePartitionPath(getComplexRecordKeyWithSimplePartitionProps(false));
  }

  public void testComplexRecordKeyWithSimplePartitionPath(TypedProperties props) throws IOException {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686");

    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686");

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(keyGenerator.getPartitionPath(internalRow, row.schema()), "timestamp=4357686");
  }

  @Test
  public void testComplexRecordKeysWithComplexPartitionPathWithKeyGeneratorClass() throws IOException {
    testComplexRecordKeysWithComplexPartitionPath(getComplexRecordKeyAndPartitionPathProps(true));
  }

  @Test
  public void testComplexRecordKeysWithComplexPartitionPathWithKeyGeneratorType() throws IOException {
    testComplexRecordKeysWithComplexPartitionPath(getComplexRecordKeyAndPartitionPathProps(false));
  }

  public void testComplexRecordKeysWithComplexPartitionPath(TypedProperties props) throws IOException {
    BuiltinKeyGenerator keyGenerator =
        (BuiltinKeyGenerator) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);

    GenericRecord record = getRecord();
    HoodieKey key = keyGenerator.getKey(record);
    Assertions.assertEquals(key.getRecordKey(), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(key.getPartitionPath(), "timestamp=4357686/ts_ms=20200321");

    Row row = KeyGeneratorTestUtilities.getRow(record);
    Assertions.assertEquals(keyGenerator.getRecordKey(row), "_row_key:key1,pii_col:pi");
    Assertions.assertEquals(keyGenerator.getPartitionPath(row), "timestamp=4357686/ts_ms=20200321");

    InternalRow internalRow = KeyGeneratorTestUtilities.getInternalRow(row);
    Assertions.assertEquals(keyGenerator.getPartitionPath(internalRow, row.schema()), "timestamp=4357686/ts_ms=20200321");
  }
}
