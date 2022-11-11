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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link TestHoodieRecordPayload}.
 */
public class TestHoodieRecordPayload {
  private Schema tableSchema;
  private Schema recordSchema;
  private Properties props;

  @BeforeEach
  public void setUp() throws Exception {
    tableSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)
    ));
    props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, "ts");
    props.setProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY, "ts");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");
  }

  /**
   * test case:
   * when set hoodie.datasource.write.drop.partition.columns =true,
   * the records have been removed partition fields by {@link 'HoodieSparkSqlWriter'$write}
   * the table schema obtained from latest commit info and always contains partition fields
   */
  @Test
  public void testGetInsertValueWithoutPartitionFields() throws IOException {
    props.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), String.valueOf(true));
    recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("id", "1");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);
    Option<GenericRecord> recordOption = Option.of(record);
    HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(recordOption);
    assertEquals(hoodieAvroPayload.getInsertValue(tableSchema, props), recordOption);
  }

  /**
   * test case:
   * when set hoodie.datasource.write.drop.partition.columns =false,
   * the records contains partition fields
   * the table schema obtained from latest commit info and always contains partition fields
   */
  @Test
  public void testGetInsertValueWithPartitionFields() throws IOException {
    props.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), String.valueOf(false));
    recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("id", "1");
    record.put("partition", "001");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);
    Option<GenericRecord> recordOption = Option.of(record);
    HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(recordOption);
    assertEquals(hoodieAvroPayload.getInsertValue(tableSchema, props), recordOption);
  }

  /**
   * for non-partition table
   * @param dropPartitionFields if hoodie.datasource.write.drop.partition.columns = false & true
   */
  @ParameterizedTest
  @ValueSource(booleans = {true,false})
  public void testGetInsertValueWithNonPartitionTable(boolean dropPartitionFields) throws IOException {
    props.setProperty(HoodieTableConfig.DROP_PARTITION_COLUMNS.key(), String.valueOf(dropPartitionFields));
    props.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
    recordSchema = Schema.createRecord(Arrays.asList(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("partition", Schema.create(Schema.Type.STRING), "", null),
        new Schema.Field("ts", Schema.create(Schema.Type.LONG), "", null),
        new Schema.Field("_hoodie_is_deleted", Schema.create(Schema.Type.BOOLEAN), "", false)));
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("id", "1");
    record.put("partition", "001");
    record.put("ts", 0L);
    record.put("_hoodie_is_deleted", false);
    Option<GenericRecord> recordOption = Option.of(record);
    HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(recordOption);
    assertEquals(hoodieAvroPayload.getInsertValue(tableSchema, props), recordOption);
  }
}
