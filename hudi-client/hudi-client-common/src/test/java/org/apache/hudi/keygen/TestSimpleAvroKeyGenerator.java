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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestSimpleAvroKeyGenerator {
  private static final String DATA_SCHEMA =
            "  {\n"
          + "    \"namespace\": \"logical\",\n"
          + "    \"type\": \"record\",\n"
          + "    \"name\": \"test\",\n"
          + "    \"fields\": [\n"
          + "      {\"name\": \"_row_key\", \"type\": {\"type\": \"string\", \"logicalType\": \"string\"}},\n"
          + "      {\"name\": \"key_1\", \"type\": {\"type\": \"string\", \"logicalType\": \"string\"}},\n"
          + "      {\"name\": \"key_2\", \"type\": {\"type\": \"string\", \"logicalType\": \"string\"}}\n"
          + "    ]\n"
          + "  }";

  private static final String PRIMARY_KEY_NAME = "_row_key";
  private static final String PRIMARY_KEY_VALUE = "10000";

  private static final String KEY_1_NAME = "key_1";
  private static final String KEY_2_NAME = "key_2";
  private static final String KEY_1_VALUE = "valueA";
  private static final String KEY_2_VALUE = "valueB";

  @Test
  public void testMultiPartitionKeys() throws IOException {
    GenericData.Record record = getFakeRecord();
    TypedProperties props = getMultiPartitionKeysProperties();

    // set KeyGenerator type only
    KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(props);
    Assertions.assertEquals(SimpleAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());

    SimpleAvroKeyGenerator simpleAvroKeyGenerator = (SimpleAvroKeyGenerator) keyGenerator;

    String expectedPartitionValue = KEY_1_VALUE + "/" + KEY_2_VALUE;
    Assertions.assertEquals(simpleAvroKeyGenerator.getPartitionPath(record), expectedPartitionValue);
    Assertions.assertEquals(simpleAvroKeyGenerator.getRecordKey(record), PRIMARY_KEY_VALUE);
  }

  @Test
  public void testSinglePartitionKey() throws IOException {
    GenericData.Record record = getFakeRecord();
    TypedProperties props = getSinglePartitionKeyProperties();

    // set KeyGenerator type only
    KeyGenerator keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(props);
    Assertions.assertEquals(SimpleAvroKeyGenerator.class.getName(), keyGenerator.getClass().getName());

    SimpleAvroKeyGenerator simpleAvroKeyGenerator = (SimpleAvroKeyGenerator) keyGenerator;
    Assertions.assertEquals(simpleAvroKeyGenerator.getPartitionPath(record), KEY_1_VALUE);
    Assertions.assertEquals(simpleAvroKeyGenerator.getRecordKey(record), PRIMARY_KEY_VALUE);
  }

  private GenericData.Record getFakeRecord() {
    Schema schema = new Schema.Parser().parse(DATA_SCHEMA);
    GenericData.Record record = new GenericData.Record(schema);
    record.put(PRIMARY_KEY_NAME, PRIMARY_KEY_VALUE);
    record.put(KEY_1_NAME, KEY_1_VALUE);
    record.put(KEY_2_NAME, KEY_2_VALUE);

    return record;
  }

  private TypedProperties getMultiPartitionKeysProperties() {
    TypedProperties props = new TypedProperties();
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), PRIMARY_KEY_NAME);
    props.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name());
    String setMultiPartitionKey = KEY_1_NAME + "," + KEY_2_NAME;
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), setMultiPartitionKey);

    return props;
  }

  private TypedProperties getSinglePartitionKeyProperties() {
    TypedProperties props = new TypedProperties();
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), PRIMARY_KEY_NAME);
    props.put(HoodieWriteConfig.KEYGENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name());
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), KEY_1_NAME);

    return props;
  }

}
