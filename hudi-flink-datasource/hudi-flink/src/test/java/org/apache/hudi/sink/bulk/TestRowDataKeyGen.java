/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieTableFactory;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Timestamp;

import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_INPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT;
import static org.apache.hudi.common.config.TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.utils.TestData.insertRow;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link RowDataKeyGen}.
 */
public class TestRowDataKeyGen {
  @Test
  void testSimpleKeyAndPartition() {
    Configuration conf = TestConfigurations.getDefaultConf("path1");
    final RowData rowData1 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString("par1"));
    final RowDataKeyGen keyGen1 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen1.getRecordKey(rowData1), is("id1"));
    assertThat(keyGen1.getPartitionPath(rowData1), is("par1"));

    // null record key and partition path
    final RowData rowData2 = insertRow(TestConfigurations.ROW_TYPE, null, StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), null);
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData2));
    assertThat(keyGen1.getPartitionPath(rowData2), is(DEFAULT_PARTITION_PATH));
    // empty record key and partition path
    final RowData rowData3 = insertRow(StringData.fromString(""), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString(""));
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData3));
    assertThat(keyGen1.getPartitionPath(rowData3), is(DEFAULT_PARTITION_PATH));

    // hive style partitioning
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    final RowDataKeyGen keyGen2 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen2.getPartitionPath(rowData1), is(String.format("partition=%s", "par1")));
    assertThat(keyGen2.getPartitionPath(rowData2), is(String.format("partition=%s", DEFAULT_PARTITION_PATH)));
    assertThat(keyGen2.getPartitionPath(rowData3), is(String.format("partition=%s", DEFAULT_PARTITION_PATH)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSingleKeyMultiplePartitionFields(boolean encodesKeyWithFieldName) {
    Configuration conf = TestConfigurations.getDefaultConf("path1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition,ts");
    conf.setString(HoodieWriteConfig.COMPLEX_KEYGEN_ENCODE_SINGLE_RECORD_KEY_FIELD_NAME.key(), String.valueOf(encodesKeyWithFieldName));
    RowData rowData1 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString("par1"));
    RowDataKeyGen keyGen1 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    String expectedKey = encodesKeyWithFieldName ? "uuid:id1" : "id1";
    assertThat(keyGen1.getRecordKey(rowData1), is(expectedKey));
    assertThat(keyGen1.getPartitionPath(rowData1), is("par1/1970-01-01T00:00:00.001"));

    // null record key and partition path
    final RowData rowData2 = insertRow(TestConfigurations.ROW_TYPE, null, null, 23, null, null);
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData2));
    assertThat(keyGen1.getPartitionPath(rowData2), is(String.format("%s/%s", DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH)));
    // empty record key and partition path
    final RowData rowData3 = insertRow(StringData.fromString(""), StringData.fromString(""), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString(""));
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData3));
    assertThat(keyGen1.getPartitionPath(rowData3), is(String.format("%s/1970-01-01T00:00:00.001", DEFAULT_PARTITION_PATH)));

    // hive style partitioning
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    final RowDataKeyGen keyGen2 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen2.getPartitionPath(rowData1), is(String.format("partition=%s/ts=%s", "par1", "1970-01-01T00:00:00.001")));
    assertThat(keyGen2.getPartitionPath(rowData2), is(String.format("partition=%s/ts=%s", DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH)));
    assertThat(keyGen2.getPartitionPath(rowData3), is(String.format("partition=%s/ts=%s", DEFAULT_PARTITION_PATH, "1970-01-01T00:00:00.001")));
  }

  @Test
  void testComplexKeyAndPartition() {
    Configuration conf = TestConfigurations.getDefaultConf("path1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition,ts");
    RowData rowData1 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString("par1"));
    RowDataKeyGen keyGen1 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen1.getRecordKey(rowData1), is("uuid:id1,name:Danny"));
    assertThat(keyGen1.getPartitionPath(rowData1), is("par1/1970-01-01T00:00:00.001"));

    // null record key and partition path
    final RowData rowData2 = insertRow(TestConfigurations.ROW_TYPE, null, null, 23, null, null);
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData2));
    assertThat(keyGen1.getPartitionPath(rowData2), is(String.format("%s/%s", DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH)));
    // empty record key and partition path
    final RowData rowData3 = insertRow(StringData.fromString(""), StringData.fromString(""), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString(""));
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData3));
    assertThat(keyGen1.getPartitionPath(rowData3), is(String.format("%s/1970-01-01T00:00:00.001", DEFAULT_PARTITION_PATH)));

    // hive style partitioning
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    final RowDataKeyGen keyGen2 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen2.getPartitionPath(rowData1), is(String.format("partition=%s/ts=%s", "par1", "1970-01-01T00:00:00.001")));
    assertThat(keyGen2.getPartitionPath(rowData2), is(String.format("partition=%s/ts=%s", DEFAULT_PARTITION_PATH, DEFAULT_PARTITION_PATH)));
    assertThat(keyGen2.getPartitionPath(rowData3), is(String.format("partition=%s/ts=%s", DEFAULT_PARTITION_PATH, "1970-01-01T00:00:00.001")));
  }

  @Test
  void testTimestampBasedKeyGenerator() {
    Configuration conf = TestConfigurations.getDefaultConf("path1");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "ts");
    HoodieTableFactory.setupTimestampKeygenOptions(conf, DataTypes.TIMESTAMP(3));
    final RowData rowData1 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(7200000), StringData.fromString("par1"));
    final RowDataKeyGen keyGen1 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);

    assertThat(keyGen1.getRecordKey(rowData1), is("id1"));
    assertThat(keyGen1.getPartitionPath(rowData1), is("1970010102"));

    // null record key and partition path
    final RowData rowData2 = insertRow(TestConfigurations.ROW_TYPE, null, StringData.fromString("Danny"), 23,
        null, StringData.fromString("par1"));
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData2));
    assertThat(keyGen1.getPartitionPath(rowData2), is("1970010100"));
    // empty record key and partition path
    final RowData rowData3 = insertRow(StringData.fromString(""), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString("par1"));
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData3));
    assertThat(keyGen1.getPartitionPath(rowData3), is("1970010100"));

    // hive style partitioning
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    final RowDataKeyGen keyGen2 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen2.getPartitionPath(rowData1), is("ts=1970010102"));
    assertThat(keyGen2.getPartitionPath(rowData2), is("ts=1970010100"));
    assertThat(keyGen2.getPartitionPath(rowData3), is("ts=1970010100"));

    // TimestampType.DATE_STRING case, we use another string type `partition` column instead of `ts`
    conf = TestConfigurations.getDefaultConf("path1");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    conf.setString(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), TimestampBasedAvroKeyGenerator.class.getName());
    conf.setString(TIMESTAMP_TYPE_FIELD.key(), TimestampBasedAvroKeyGenerator.TimestampType.DATE_STRING.name());
    conf.setString(TIMESTAMP_INPUT_DATE_FORMAT.key(), "yyyy-MM-dd HH:mm:ss");
    conf.setString(TIMESTAMP_OUTPUT_DATE_FORMAT.key(), "yyyy-MM-dd");
    final RowData rowData4 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(7200000), StringData.fromString("2004-02-29 01:02:03"));
    final RowDataKeyGen keyGen3 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);
    assertThat(keyGen3.getPartitionPath(rowData4), is("2004-02-29"));
  }

  @ParameterizedTest
  @ValueSource(strings = {FlinkOptions.PARTITION_FORMAT_DASHED_DAY, FlinkOptions.PARTITION_FORMAT_DAY})
  void testDateBasedKeyGenerator(String partitionFormat) {
    boolean dashed = partitionFormat.equals(FlinkOptions.PARTITION_FORMAT_DASHED_DAY);
    Configuration conf = TestConfigurations.getDefaultConf("path1", TestConfigurations.ROW_DATA_TYPE_DATE);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "dt");
    conf.set(FlinkOptions.PARTITION_FORMAT, partitionFormat);
    HoodieTableFactory.setupTimestampKeygenOptions(conf, DataTypes.DATE());
    final RowData rowData1 = insertRow(TestConfigurations.ROW_TYPE_DATE,
        StringData.fromString("id1"), StringData.fromString("Danny"), 23, 1);
    final RowDataKeyGen keyGen1 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE_DATE);

    assertThat(keyGen1.getRecordKey(rowData1), is("id1"));
    String expectedPartition1 = dashed ? "1970-01-02" : "19700102";
    assertThat(keyGen1.getPartitionPath(rowData1), is(expectedPartition1));

    // null record key and partition path
    final RowData rowData2 = insertRow(TestConfigurations.ROW_TYPE_DATE, null, StringData.fromString("Danny"), 23, null);
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData2));
    String expectedPartition2 = dashed ? "1970-01-02" : "19700102";
    assertThat(keyGen1.getPartitionPath(rowData2), is(expectedPartition2));

    // empty record key
    String expectedPartition3 = dashed ? "1970-01-03" : "19700103";
    final RowData rowData3 = insertRow(TestConfigurations.ROW_TYPE_DATE, StringData.fromString(""), StringData.fromString("Danny"), 23, 2);
    assertThrows(HoodieKeyException.class, () -> keyGen1.getRecordKey(rowData3));
    assertThat(keyGen1.getPartitionPath(rowData3), is(expectedPartition3));

    // hive style partitioning
    conf.set(FlinkOptions.HIVE_STYLE_PARTITIONING, true);
    final RowDataKeyGen keyGen2 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE_DATE);
    assertThat(keyGen2.getPartitionPath(rowData1), is("dt=" + expectedPartition1));
    assertThat(keyGen2.getPartitionPath(rowData2), is("dt=" + expectedPartition2));
    assertThat(keyGen2.getPartitionPath(rowData3), is("dt=" + expectedPartition3));
  }

  @Test
  void testPrimaryKeylessWrite() {
    Configuration conf = TestConfigurations.getDefaultConf("path1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "");
    final RowData rowData1 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString("par1"));
    final int taskId = 3;
    final String instantTime = "000001";
    final RowDataKeyGen keyGen1 = RowDataKeyGens.instance(conf, TestConfigurations.ROW_TYPE, taskId, instantTime);
    assertThat(keyGen1.getRecordKey(rowData1), is(instantTime + "_" + taskId + "_0"));

    // null record key and partition path
    final RowData rowData2 = insertRow(TestConfigurations.ROW_TYPE, null, StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), null);
    assertThat(keyGen1.getRecordKey(rowData2), is(instantTime + "_" + taskId + "_1"));

    // empty record key and partition path
    final RowData rowData3 = insertRow(StringData.fromString(""), StringData.fromString("Danny"), 23,
        TimestampData.fromEpochMillis(1), StringData.fromString(""));
    assertThat(keyGen1.getRecordKey(rowData3), is(instantTime + "_" + taskId + "_2"));
  }

  @Test
  void testRecordKeyContainsTimestamp() {
    Configuration conf = TestConfigurations.getDefaultConf("path1");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid,ts");
    conf.setString(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "true");
    Timestamp ts = new Timestamp(1675841687000L);
    final RowData rowData1 = insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
        TimestampData.fromTimestamp(ts), StringData.fromString("par1"));
    final RowDataKeyGen keyGen1 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);

    assertThat(keyGen1.getRecordKey(rowData1), is("uuid:id1,ts:" + ts.toLocalDateTime().toString()));

    conf.setString(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), "false");
    final RowDataKeyGen keyGen2 = RowDataKeyGen.instance(conf, TestConfigurations.ROW_TYPE);

    assertThat(keyGen2.getRecordKey(rowData1), is("uuid:id1,ts:1675841687000"));

  }
}
