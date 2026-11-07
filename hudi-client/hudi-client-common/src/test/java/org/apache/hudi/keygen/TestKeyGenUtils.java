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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORDKEY_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), ""));
    assertEquals(
        KeyGeneratorType.NON_PARTITION,
        KeyGenUtils.inferKeyGeneratorType(Option.of("col1,col2"), null));
  }

  @Test
  public void testExtractRecordKeys() {
    // test complex key form: field1:val1,field2:val2,...
    String[] s1 = KeyGenUtils.extractRecordKeys("id:1");
    Assertions.assertArrayEquals(new String[] {"1"}, s1);

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
    Assertions.assertArrayEquals(new String[]{"1", "2", "2"}, s6);
  }

  @Test
  public void testExtractRecordKeysWithFields() {
    List<String> fields = new ArrayList<>(1);
    fields.add("id2");

    String[] s1 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2"}, s1);

    String[] s2 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2", "2"}, s2);
  }

  @Test
  void testGetRecordKey() {
    Schema nullableStringSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    Schema schema = Schema.createRecord("TestRecord", "doc", "test", false,
        Arrays.asList(
            new Schema.Field("key1", nullableStringSchema, "", null),
            new Schema.Field("key2", nullableStringSchema, "", null),
            new Schema.Field("key3", nullableStringSchema, "", null),
            new Schema.Field("key4", nullableStringSchema, "", null)
        ));
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("key1", "value1");
    avroRecord.put("key2", "value2");
    avroRecord.put("key3", null);
    avroRecord.put("key4", "");

    assertEquals("key1:value1",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1"), true));
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key3"), true),
        "recordKey values: \"key3:__null__\" for fields: [key3] cannot be entirely null or empty.");
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key4"), true),
        "recordKey values: \"key4:__empty__\" for fields: [key4] cannot be entirely null or empty.");
    assertEquals("key1:value1,key2:value2",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1", "key2"), true));
    assertEquals("key1:value1,key3:__null__",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1", "key3"), true));
    assertEquals("key1:value1,key4:__empty__",
        KeyGenUtils.getRecordKey(avroRecord, Arrays.asList("key1", "key4"), true));

    assertEquals("value1",
        KeyGenUtils.getRecordKey(avroRecord, "key1", true));
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, "key3", true),
        "recordKey value: \"null\" for field: \"key3\" cannot be null or empty.");
    assertThrows(HoodieKeyException.class,
        () -> KeyGenUtils.getRecordKey(avroRecord, "key4", true),
        "recordKey value: \"\" for field: \"key4\" cannot be null or empty.");
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyField() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertTrue(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexAvroKeyGenerator");
    assertTrue(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnMultipleFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexAvroKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId,name");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNonComplexGenerator() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.SimpleKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.SimpleAvroKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.TimestampBasedKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.CustomKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNoRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldEmptyRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    tableConfig.setValue(RECORDKEY_FIELDS, "");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @TempDir
  Path tempDir;

  private static TypedProperties keyGenProps(String recordedEncoding, Boolean newEncoding) {
    TypedProperties props = new TypedProperties();
    if (recordedEncoding != null) {
      props.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), recordedEncoding);
    }
    if (newEncoding != null) {
      props.setProperty(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), newEncoding.toString());
    }
    return props;
  }

  @Test
  void testRecordedEncodingWinsOverNewEncodingConfig() {
    assertTrue(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps("FIELD_PREFIXED", true)));
    assertFalse(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps("VALUE_ONLY", false)));
    assertFalse(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps("value_only", null)));
    // without a recorded encoding the writer config decides, as before
    assertTrue(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps(null, null)));
    assertTrue(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps(null, false)));
    assertFalse(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps(null, true)));
    assertThrows(IllegalArgumentException.class,
        () -> KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(keyGenProps("BOGUS", null)));
  }

  @Test
  void testDeclaredComplexKeyGenEncoding() {
    assertEquals(ComplexKeyGenEncoding.FIELD_PREFIXED, KeyGenUtils.getDeclaredComplexKeyGenEncoding(keyGenProps(null, null)));
    assertEquals(ComplexKeyGenEncoding.VALUE_ONLY, KeyGenUtils.getDeclaredComplexKeyGenEncoding(keyGenProps(null, true)));
    assertEquals(ComplexKeyGenEncoding.FIELD_PREFIXED, KeyGenUtils.getDeclaredComplexKeyGenEncoding(keyGenProps("FIELD_PREFIXED", true)));
  }

  @Test
  void testRequireComplexKeyGenEncodingTracked() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertTrue(KeyGenUtils.requireComplexKeyGenEncodingTracked(tableConfig));
    // without the meta fields no stored key can diverge from the key generator's
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    assertFalse(KeyGenUtils.requireComplexKeyGenEncodingTracked(tableConfig));
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "true");
    tableConfig.setValue(RECORDKEY_FIELDS, "id,name");
    assertFalse(KeyGenUtils.requireComplexKeyGenEncodingTracked(tableConfig));
  }

  private static HoodieConfig writeConfig(String... keyValues) {
    Properties props = new Properties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return new HoodieConfig(props);
  }

  @Test
  void testMayNeedComplexKeyGenEncodingRecorded() {
    String complex = "org.apache.hudi.keygen.ComplexKeyGenerator";
    String keyGenOption = HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key();
    String recordKeyOption = KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key();
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig(keyGenOption, complex, recordKeyOption, "id")));
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(
        writeConfig(keyGenOption, ComplexAvroKeyGenerator.class.getName(), recordKeyOption, "id")));
    // a write option named like the table property does not prove that the table records it
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig(keyGenOption, complex, recordKeyOption, "id",
        HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), "FIELD_PREFIXED")));
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig(keyGenOption, complex, recordKeyOption, "id,name")));
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(
        writeConfig(keyGenOption, "org.apache.hudi.keygen.SimpleKeyGenerator", recordKeyOption, "id")));
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig(keyGenOption, complex, recordKeyOption, "id",
        HoodieTableConfig.POPULATE_META_FIELDS.key(), "false")));
    // a wrapper or an unnamed key generator cannot be ruled out without the table config
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(
        writeConfig(keyGenOption, "org.apache.spark.sql.hudi.command.SqlKeyGenerator", recordKeyOption, "id")));
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig(recordKeyOption, "id")));
    // the table's own key generator, when merged into the options, decides over a wrapper
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeConfig(
        keyGenOption, "org.apache.spark.sql.hudi.command.SqlKeyGenerator", recordKeyOption, "id",
        KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.SimpleKeyGenerator")));
  }

  private HoodieTableMetaClient initComplexKeyGenTable() throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(KEY_GENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    tableProperties.setProperty(RECORDKEY_FIELDS.key(), "id");
    tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "p1");
    return HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tempDir.resolve("table").toString(), HoodieTableType.COPY_ON_WRITE, tableProperties);
  }

  private HoodieWriteConfig complexKeyGenWriteConfig(String basePath, boolean newEncoding) {
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), String.valueOf(newEncoding));
    return HoodieWriteConfig.newBuilder().withPath(basePath).withProperties(props).build();
  }

  /**
   * The builder records only a declared encoding: it also re-initializes existing tables, whose data a default
   * would misdescribe.
   */
  @Test
  void testTableBuilderRecordsOnlyADeclaredEncoding() throws IOException {
    HoodieTableMetaClient metaClient = initComplexKeyGenTable();
    assertEquals(Option.empty(), metaClient.getTableConfig().getComplexKeyGenEncoding());

    String declaredPath = tempDir.resolve("declared").toString();
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("declared")
        .setKeyGeneratorClassProp(ComplexAvroKeyGenerator.class.getName())
        .setRecordKeyFields("id")
        .setComplexKeyGenEncoding(ComplexKeyGenEncoding.VALUE_ONLY)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), declaredPath);
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
        HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf(), declaredPath).getTableConfig().getComplexKeyGenEncoding());

    // re-initializing the table from its own properties keeps what it records
    Properties existing = HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf(), declaredPath).getTableConfig().getProps();
    HoodieTableMetaClient.withPropertyBuilder().fromProperties(existing)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), declaredPath);
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
        HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf(), declaredPath).getTableConfig().getComplexKeyGenEncoding());
  }

  @Test
  void testNeverWrittenTableRecordsTheConfiguredEncoding() throws IOException {
    HoodieTableMetaClient metaClient = initComplexKeyGenTable();
    String basePath = metaClient.getBasePath().toString();

    // no stored key exists, so the writer's configured encoding is what the table is going to carry
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
        KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, complexKeyGenWriteConfig(basePath, true)));
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED),
        KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, complexKeyGenWriteConfig(basePath, false)));
    HoodieWriteConfig declared = complexKeyGenWriteConfig(basePath, false);
    declared.setValue(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING, ComplexKeyGenEncoding.VALUE_ONLY.name());
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, declared));
    assertFalse(KeyGenUtils.hasCompletedCommits(metaClient));

    KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, complexKeyGenWriteConfig(basePath, true));
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), metaClient.getTableConfig().getComplexKeyGenEncoding());
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
        HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf(), basePath).getTableConfig().getComplexKeyGenEncoding());
    // recording is idempotent: a later writer with another config does not change it
    KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, complexKeyGenWriteConfig(basePath, false));
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY),
        HoodieTestUtils.createMetaClient(HoodieTestUtils.getDefaultStorageConf(), basePath).getTableConfig().getComplexKeyGenEncoding());
  }

}
