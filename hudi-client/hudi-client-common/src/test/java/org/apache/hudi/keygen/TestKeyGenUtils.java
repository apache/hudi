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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORDKEY_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKeyGenUtils {

  @Test
  public void testInferKeyGeneratorType() {
    assertEquals(
        KeyGeneratorType.SIMPLE,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1"), "partition1"));

    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1"), "partition1,partition2"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), "partition1"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), "partition1,partition2"));

    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1"), "partition1:simple,partition2:timestamp"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), "partition1:simple"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), "partition1:simple,partition2:timestamp"));

    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), ""));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), null));

    // Test key generator type with auto generation of record keys
    assertEquals(
        KeyGeneratorType.SIMPLE,
        KeyGenUtils.inferKeyGeneratorType(Option.empty(), "partition1"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorType(Option.empty(), "partition1,partition2"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorType(Option.empty(), "partition1:simple"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorType(Option.empty(), "partition1:simple,partition2:timestamp"));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.empty(), ""));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.empty(), null));
  }

  @Test
  public void testInferKeyGeneratorTypeFromPartitionFields() {
    assertEquals(
        KeyGeneratorType.SIMPLE,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields("partition1"));
    assertEquals(
        KeyGeneratorType.COMPLEX,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields("partition1,partition2"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields("partition1:simple"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields("partition1:timestamp"));
    assertEquals(
        KeyGeneratorType.CUSTOM,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields("partition1:simple,partition2:timestamp"));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields(""));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorTypeFromPartitionFields(null));
  }

  @Test
  public void testExtractRecordKeys() {
    // if for recordKey one column only is used, then there is no added column name before value
    String[] s1 = KeyGenUtils.extractRecordKeys("2024-10-22 14:11:53.023");
    Assertions.assertArrayEquals(new String[] {"2024-10-22 14:11:53.023"}, s1);

    // test complex key form: field1:val1,field2:val2,...
    String[] s2 = KeyGenUtils.extractRecordKeys("id:1,id:2");
    Assertions.assertArrayEquals(new String[] {"1", "2"}, s2);

    String[] s3 = KeyGenUtils.extractRecordKeys("id:1,id2:__null__,id3:__empty__");
    Assertions.assertArrayEquals(new String[] {"1", null, ""}, s3);

    String[] s4 = KeyGenUtils.extractRecordKeys("id:ab:cd,id2:ef");
    Assertions.assertArrayEquals(new String[] {"ab:cd", "ef"}, s4);

    // test simple key form: val1
    String[] s5 = KeyGenUtils.extractRecordKeys("1");
    Assertions.assertArrayEquals(new String[] {"1"}, s5);

    String[] s6 = KeyGenUtils.extractRecordKeys("id:1,id2:2,2");
    Assertions.assertArrayEquals(new String[]{"1", "2,2"}, s6);
  }

  @Test
  public void testExtractRecordKeysWithFields() {
    List<String> fields = new ArrayList<>(1);
    fields.add("id2");

    String[] s1 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2"}, s1);

    String[] s2 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2,2"}, s2);

    String[] s3 = KeyGenUtils.extractRecordKeysByFields("id1:1,1,1,id2:,2,2,,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {",2,2,"}, s3);

    fields.addAll(Arrays.asList("id1", "id3", "id4"));
    // tough case with a lot of ',' and ':'
    String[] s4 = KeyGenUtils.extractRecordKeysByFields("id1:1,,,id2:2024-10-22 14:11:53.023,id3:,,3,id4:::1:2::4::", fields);
    Assertions.assertArrayEquals(new String[] {"1,,", "2024-10-22 14:11:53.023", ",,3", "::1:2::4::"}, s4);
  }

  @Test
  public void testIsComplexKeyGeneratorWithSingleRecordKeyField() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertTrue(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX_AVRO.name());
    assertTrue(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  public void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnMultipleFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX_AVRO.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId,name");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  public void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNonComplexGenerator() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE_AVRO.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.TIMESTAMP.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.CUSTOM.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  public void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNoRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }

  @Test
  public void testIsComplexKeyGeneratorWithSingleRecordKeyFieldEmptyRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "");
    assertFalse(KeyGenUtils.isComplexKeyGeneratorWithSingleRecordKeyField(tableConfig));
  }
}
