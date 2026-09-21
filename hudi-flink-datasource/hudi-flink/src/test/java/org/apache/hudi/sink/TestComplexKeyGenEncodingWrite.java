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

package org.apache.hudi.sink;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Flink writes to a single-field ComplexKeyGenerator table that predates {@code hoodie.table.complex.keygenerator.encoding}:
 * the job supplies the matching encoding before keying its records, and the write records it.
 */
public class TestComplexKeyGenEncodingWrite {

  @TempDir
  File tempFile;

  private Configuration complexKeygenConf() {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition,name");
    return conf;
  }

  @ParameterizedTest
  @EnumSource(ComplexKeyGenEncoding.class)
  void testWriteToLegacyTableWithoutRecordedEncoding(ComplexKeyGenEncoding encoding) throws Exception {
    // a version 8 table written with the given encoding by a release that did not record it
    Configuration legacyConf = complexKeygenConf();
    legacyConf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.EIGHT.versionCode());
    legacyConf.setString(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), encoding.name());
    StreamerUtil.initTableIfNotExists(legacyConf);
    TestData.writeData(TestData.DATA_SET_INSERT, legacyConf);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(legacyConf);
    HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
        Collections.singleton(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    checkRecordKeys(encoding, TestData.DATA_SET_INSERT);

    // a job at the current version: the matching encoding is supplied in the job conf ahead of the upgrade, so the
    // records it keys match the existing ones and the upgrade records the same encoding on the table
    Configuration conf = complexKeygenConf();
    conf.setString(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), encoding.name());
    OptionsInference.setupComplexKeygenEncoding(conf);
    assertEquals(encoding.name(), conf.getString(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), null));
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    metaClient = StreamerUtil.createMetaClient(conf);
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig().getTableVersion());
    assertEquals(Option.of(encoding), metaClient.getTableConfig().getComplexKeyGenEncoding());
    checkRecordKeys(encoding, TestData.DATA_SET_INSERT, TestData.DATA_SET_UPDATE_INSERT);
  }

  /** A table created by the job records FIELD_PREFIXED and keys its records the same way, whatever the legacy write config says. */
  @Test
  void testNewTableAtOlderVersionKeysFollowRecordedEncoding() throws Exception {
    Configuration conf = complexKeygenConf();
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.EIGHT.versionCode());
    conf.setString(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), "true");
    OptionsInference.setupComplexKeygenEncoding(conf);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    assertEquals(Option.of(ComplexKeyGenEncoding.FIELD_PREFIXED), metaClient.getTableConfig().getComplexKeyGenEncoding());
    checkRecordKeys(ComplexKeyGenEncoding.FIELD_PREFIXED, TestData.DATA_SET_INSERT);

    Configuration conf2 = complexKeygenConf();
    OptionsInference.setupComplexKeygenEncoding(conf2);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf2);
    checkRecordKeys(ComplexKeyGenEncoding.FIELD_PREFIXED, TestData.DATA_SET_INSERT, TestData.DATA_SET_UPDATE_INSERT);
  }

  /** Every key of the given rows appears exactly once in its partition, stored with the given encoding. */
  @SafeVarargs
  private final void checkRecordKeys(ComplexKeyGenEncoding encoding, List<RowData>... dataSets) throws Exception {
    String prefix = encoding.encodesFieldName() ? "uuid:" : "";
    Map<String, TreeSet<String>> keysByPartition = new TreeMap<>();
    Stream.of(dataSets).flatMap(List::stream).forEach(row ->
        keysByPartition.computeIfAbsent(row.getString(4) + "/" + row.getString(1), p -> new TreeSet<>()).add(prefix + row.getString(0)));
    Map<String, String> expected = keysByPartition.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    int topLevelPartitions = (int) Stream.of(dataSets).flatMap(List::stream)
        .map(row -> row.getString(4).toString()).distinct().count();
    TestData.checkWrittenData(tempFile, expected, topLevelPartitions,
        record -> record.get("_hoodie_record_key").toString());
  }
}
