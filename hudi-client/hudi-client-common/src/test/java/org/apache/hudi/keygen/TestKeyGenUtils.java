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
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.KEY_GENERATOR_TYPE;
import static org.apache.hudi.common.table.HoodieTableConfig.RECORDKEY_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  public void testGetRecordKeyFields() {
    assertEquals(Collections.emptyList(), KeyGenUtils.getRecordKeyFields((String) null));
    assertEquals(Collections.emptyList(), KeyGenUtils.getRecordKeyFields(""));
    assertEquals(Arrays.asList("id", "ts", "name"), KeyGenUtils.getRecordKeyFields(" id,ts, name,, "));

    TypedProperties props = new TypedProperties();
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), " id,ts ");
    assertEquals(Arrays.asList("id", "ts"), KeyGenUtils.getRecordKeyFields(props));
  }

  @Test
  public void testGetIndexKeyFields() {
    assertEquals(Collections.emptyList(), KeyGenUtils.getIndexKeyFields(null));
    assertEquals(Collections.emptyList(), KeyGenUtils.getIndexKeyFields(""));
    assertEquals(Arrays.asList("id", "ts", "name"), KeyGenUtils.getIndexKeyFields(" id,ts, name,, "));
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
  void testGetRecordKey() {
    Schema nullableStringSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    Schema schema = Schema.createRecord("TestRecord", "doc", "test", false,
        Arrays.asList(
            new Schema.Field("key1", nullableStringSchema),
            new Schema.Field("key2", nullableStringSchema),
            new Schema.Field("key3", nullableStringSchema),
            new Schema.Field("key4", nullableStringSchema)
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
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertTrue(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX_AVRO.name());
    assertTrue(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnMultipleFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX_AVRO.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id,userId,name");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNonComplexGenerator() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE_AVRO.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "userId");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.TIMESTAMP.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());

    tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.CUSTOM.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldOnNoRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  @Test
  void testIsComplexKeyGeneratorWithSingleRecordKeyFieldEmptyRecordKeyFields() {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, "");
    assertFalse(tableConfig.isComplexKeyGenWithSingleRecordKeyField());
  }

  /**
   * The persisted table encoding, when present in the key generator props, wins over both the write table version
   * and hoodie.write.complex.keygen.new.encoding; otherwise version 9+ always prefixes and version 8 follows the config.
   * "-" stands for "not set".
   */
  @ParameterizedTest
  @CsvSource(value = {
      "8,-,-,true", "8,true,-,false", "8,false,-,true",
      "9,-,-,true", "9,true,-,true", "10,-,-,true", "10,true,-,true",
      "8,true,FIELD_PREFIXED,true", "8,false,VALUE_ONLY,false",
      "9,-,VALUE_ONLY,false", "9,false,VALUE_ONLY,false", "9,-,FIELD_PREFIXED,true",
      "10,-,VALUE_ONLY,false", "10,true,FIELD_PREFIXED,true", "10,-,value_only,false"})
  void testEncodeSingleKeyFieldNameForComplexKeyGen(String tableVersion, String newEncoding, String persistedEncoding,
                                                    boolean expectedFieldNameEncoded) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), tableVersion);
    if (!"-".equals(newEncoding)) {
      props.setProperty(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), newEncoding);
    }
    if (!"-".equals(persistedEncoding)) {
      props.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), persistedEncoding);
    }
    assertEquals(expectedFieldNameEncoded, KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(props));
  }

  @Test
  void testEncodeSingleKeyFieldNameForComplexKeyGenRejectsUnknownPersistedEncoding() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), "BOGUS");
    assertThrows(IllegalArgumentException.class, () -> KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(props));
  }

  private static HoodieTableConfig complexKeygenTableConfig(int tableVersion, String recordKeyFields, String persistedEncoding) {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.VERSION, String.valueOf(tableVersion));
    tableConfig.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableConfig.setValue(RECORDKEY_FIELDS, recordKeyFields);
    if (persistedEncoding != null) {
      tableConfig.setValue(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING, persistedEncoding);
    }
    return tableConfig;
  }

  private static HoodieTableMetaClient metaClientOf(HoodieTableConfig tableConfig) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    return metaClient;
  }

  /** The pre-check on the write config alone decides whether the table config is worth loading at all. */
  @Test
  void testMayNeedComplexKeyGenEncodingRecorded() {
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(new HoodieConfig()), "no key generator at all");

    HoodieConfig writeOptions = new HoodieConfig();
    writeOptions.setValue(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
    writeOptions.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, "id");
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeOptions));
    writeOptions.setValue(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING, ComplexKeyGenEncoding.VALUE_ONLY.name());
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(writeOptions), "already known to the writer");

    HoodieConfig byType = new HoodieConfig();
    byType.setValue(HoodieWriteConfig.KEYGENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    byType.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, "id");
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(byType));
    byType.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, "id,name");
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(byType), "several record key fields");

    // the table config merged into Spark's write options
    HoodieConfig tableKeys = new HoodieConfig();
    tableKeys.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    tableKeys.setValue(RECORDKEY_FIELDS, "id");
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(tableKeys));
    tableKeys.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(tableKeys), "no stored record key");

    HoodieConfig simple = new HoodieConfig();
    simple.setValue(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME, "org.apache.hudi.keygen.SimpleKeyGenerator");
    simple.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, "id");
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(simple));
    // a custom class, such as Spark SQL's wrapper: the merged table config decides, or the check cannot rule it out
    HoodieConfig custom = new HoodieConfig();
    custom.setValue(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME, "org.apache.spark.sql.hudi.command.SqlKeyGenerator");
    custom.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, "id");
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(custom), "custom class without table config");
    custom.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE.name());
    assertFalse(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(custom), "the table says simple");
    custom.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.COMPLEX.name());
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(custom), "the table says complex");

    // the type option carries a default: an explicit class wins over it
    HoodieConfig defaultedType = new HoodieConfig();
    defaultedType.setValue(HoodieWriteConfig.KEYGENERATOR_TYPE, KeyGeneratorType.SIMPLE.name());
    defaultedType.setValue(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME, "org.apache.hudi.keygen.ComplexKeyGenerator");
    defaultedType.setValue(KeyGeneratorOptions.RECORDKEY_FIELD_NAME, "id");
    assertTrue(KeyGenUtils.mayNeedComplexKeyGenEncodingRecorded(defaultedType));
  }

  @Test
  void testIsComplexKeyGenEncodingTracked() {
    assertTrue(KeyGenUtils.requireComplexKeyGenEncodingTracked(complexKeygenTableConfig(8, "id", null)));
    assertTrue(KeyGenUtils.requireComplexKeyGenEncodingTracked(complexKeygenTableConfig(10, "id", null)));
    assertFalse(KeyGenUtils.requireComplexKeyGenEncodingTracked(complexKeygenTableConfig(9, "id,name", null)));

    // without a stored record key there is no encoding to track
    HoodieTableConfig virtualKeys = complexKeygenTableConfig(9, "id", null);
    virtualKeys.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    assertFalse(KeyGenUtils.requireComplexKeyGenEncodingTracked(virtualKeys));
    HoodieTableConfig commitTimeOnly = complexKeygenTableConfig(10, "id", null);
    commitTimeOnly.setValue(HoodieTableConfig.META_FIELDS_MODE, MetaFieldsMode.COMMIT_TIME_ONLY.name());
    assertFalse(KeyGenUtils.requireComplexKeyGenEncodingTracked(commitTimeOnly));
  }

  @Test
  void testResolveComplexKeyGenEncodingFromTableConfig() {
    // the persisted property is the answer on every table version
    for (int tableVersion : new int[] {6, 8, 9, 10}) {
      assertEquals(ComplexKeyGenEncoding.VALUE_ONLY,
          KeyGenUtils.resolveComplexKeyGenEncoding(metaClientOf(complexKeygenTableConfig(tableVersion, "id", "VALUE_ONLY"))).get());
      assertEquals(ComplexKeyGenEncoding.FIELD_PREFIXED,
          KeyGenUtils.resolveComplexKeyGenEncoding(metaClientOf(complexKeygenTableConfig(tableVersion, "id", "field_prefixed"))).get());
    }

    // record key meta field not populated: the key generator follows the table version, nothing is recorded
    HoodieTableConfig virtualKeysV9 = complexKeygenTableConfig(9, "id", null);
    virtualKeysV9.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    assertEquals(ComplexKeyGenEncoding.FIELD_PREFIXED, KeyGenUtils.resolveComplexKeyGenEncoding(metaClientOf(virtualKeysV9)).get());
    HoodieTableConfig virtualKeysV8 = complexKeygenTableConfig(8, "id", null);
    virtualKeysV8.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    assertFalse(KeyGenUtils.resolveComplexKeyGenEncoding(metaClientOf(virtualKeysV8)).isPresent());

    // not a single-field complex key generator: nothing to resolve
    assertFalse(KeyGenUtils.resolveComplexKeyGenEncoding(metaClientOf(complexKeygenTableConfig(9, "id,name", "VALUE_ONLY"))).isPresent());
    HoodieTableConfig simple = new HoodieTableConfig();
    simple.setValue(HoodieTableConfig.VERSION, "9");
    simple.setValue(KEY_GENERATOR_TYPE, KeyGeneratorType.SIMPLE.name());
    simple.setValue(RECORDKEY_FIELDS, "id");
    assertFalse(KeyGenUtils.resolveComplexKeyGenEncoding(metaClientOf(simple)).isPresent());
  }

  @Test
  void testComplexKeyGenEncodingOnNewAndEmptyTable(@TempDir Path tempDir) throws IOException {
    Properties props = new Properties();
    props.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tempDir.toString(), HoodieTableType.COPY_ON_WRITE, props);
    // a new single-field complex keygen table records the field-prefixed encoding on creation
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), metaClient.getTableConfig().getComplexKeyGenEncoding());

    // a table without the property and without data files deduces the default instead of failing
    HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
        Collections.singleton(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    metaClient.reloadTableConfig();
    assertFalse(metaClient.getTableConfig().getComplexKeyGenEncoding().isPresent());
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), KeyGenUtils.deduceComplexKeyGenEncodingFromData(metaClient));
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), KeyGenUtils.resolveComplexKeyGenEncoding(metaClient));
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tempDir.toString()).build();
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, writeConfig));

    KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, writeConfig);
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), metaClient.getTableConfig().getComplexKeyGenEncoding());

    Properties recorded = new Properties();
    recorded.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), ComplexKeyGenEncoding.VALUE_ONLY.name());
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), recorded);
    metaClient.reloadTableConfig();
    KeyGenUtils.recordComplexKeygenEncodingIfMissing(metaClient, writeConfig);
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), metaClient.getTableConfig().getComplexKeyGenEncoding());
  }

  /**
   * A retry of an upgrade that died after the 7 to 8 hop had moved the instants into the version 2 timeline reads an
   * empty version 1 timeline. That must not be taken for a never-written table: the encoding cannot be deduced.
   */
  @Test
  void testComplexKeyGenEncodingNotDeducedFromTimelineMovedByUnfinishedUpgrade(@TempDir Path tempDir) throws IOException {
    Properties props = new Properties();
    props.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    props.put(HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.SIX.versionCode()));
    props.put(HoodieTableConfig.TIMELINE_LAYOUT_VERSION.key(), "1");
    HoodieTableMetaClient metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), tempDir.toString(), HoodieTableType.COPY_ON_WRITE, props);
    HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
        Collections.singleton(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig().getTableVersion());
    StoragePath timelinePath = new StoragePath(metaClient.getMetaPath(), HoodieTableConfig.TIMELINE_PATH.defaultValue());
    assertFalse(metaClient.getStorage().exists(timelinePath));

    // no version 2 timeline: a table that was never written deduces the default
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), KeyGenUtils.deduceComplexKeyGenEncodingFromData(metaClient));

    // the version 2 timeline of an unfinished upgrade: nothing can be deduced, so validation refuses to guess
    metaClient.getStorage().createDirectory(timelinePath);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(KeyGenUtils.deduceComplexKeyGenEncodingFromData(metaClient).isPresent());
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(tempDir.toString()).build();
    assertFalse(KeyGenUtils.resolveComplexKeyGenEncodingForWrite(metaClient, writeConfig).isPresent());
  }

  @Test
  void testWithComplexKeyGenEncoding() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), "FIELD_PREFIXED");
    // a table without the property leaves the props alone
    KeyGenUtils.withComplexKeyGenEncoding(props, complexKeygenTableConfig(8, "id", null));
    assertEquals("FIELD_PREFIXED", props.getProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    // the table's recorded encoding wins over whatever the props carried
    KeyGenUtils.withComplexKeyGenEncoding(props, complexKeygenTableConfig(8, "id", "VALUE_ONLY"));
    assertEquals("VALUE_ONLY", props.getProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    assertFalse(KeyGenUtils.encodeSingleKeyFieldNameForComplexKeyGen(props));
  }
}
