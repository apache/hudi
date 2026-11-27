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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that bulk insert writes the bloom filter and min/max record key footers to base files only when
 * {@code hoodie.parquet.bloom.filter.enabled} is true or the index type needs them, on both the row-writer
 * and the record-based write paths.
 */
@Tag("functional")
class TestBulkInsertBloomFilterFooter extends SparkClientFunctionalTestHarness {

  private static final StructType SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("key", DataTypes.StringType, false),
      DataTypes.createStructField("partition", DataTypes.StringType, false),
      DataTypes.createStructField("value", DataTypes.StringType, false)
  });

  private static Stream<Arguments> bloomFilterFooterCases() {
    List<Arguments> cases = new ArrayList<>();
    for (boolean rowWriter : new boolean[] {true, false}) {
      cases.add(Arguments.of(rowWriter, null, HoodieIndex.IndexType.SIMPLE, true));
      cases.add(Arguments.of(rowWriter, "false", HoodieIndex.IndexType.SIMPLE, false));
      cases.add(Arguments.of(rowWriter, "false", HoodieIndex.IndexType.BLOOM, true));
      cases.add(Arguments.of(rowWriter, "false", HoodieIndex.IndexType.GLOBAL_BLOOM, true));
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "rowWriter={0}, bloomFilterEnabled={1}, index={2}")
  @MethodSource("bloomFilterFooterCases")
  void testBloomFilterFooterFollowsConfig(boolean rowWriter, String bloomFilterEnabled,
                                          HoodieIndex.IndexType indexType, boolean expectBloomFilter) throws IOException {
    Map<String, String> options = new HashMap<>();
    options.put(HoodieTableConfig.NAME.key(), "test_bloom_filter_footer");
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition");
    options.put(DataSourceWriteOptions.ENABLE_ROW_WRITER().key(), String.valueOf(rowWriter));
    options.put(HoodieIndexConfig.INDEX_TYPE.key(), indexType.name());
    options.put(HoodieMetadataConfig.ENABLE.key(), "false");
    if (bloomFilterEnabled != null) {
      options.put(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), bloomFilterEnabled);
    }

    List<Row> rows = IntStream.range(0, 20)
        .mapToObj(i -> RowFactory.create("key" + i, "p" + (i % 2), "value" + i))
        .collect(Collectors.toList());
    spark().createDataset(rows, SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(SCHEMA))
        .write()
        .format("hudi")
        .options(options)
        .mode(SaveMode.Overwrite)
        .save(basePath());

    List<StoragePath> baseFiles = new ArrayList<>();
    for (String partition : new String[] {"p0", "p1"}) {
      for (StoragePathInfo pathInfo : hoodieStorage().listDirectEntries(new StoragePath(basePath(), partition))) {
        if (pathInfo.getPath().getName().endsWith(".parquet")) {
          baseFiles.add(pathInfo.getPath());
        }
      }
    }
    assertFalse(baseFiles.isEmpty(), "bulk insert must write base files");
    for (StoragePath baseFile : baseFiles) {
      Map<String, String> footer = new ParquetUtils().readFooter(hoodieStorage(), false, baseFile,
          HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, HOODIE_MIN_RECORD_KEY_FOOTER, HOODIE_MAX_RECORD_KEY_FOOTER);
      assertEquals(expectBloomFilter, footer.containsKey(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY),
          "bloom filter footer presence in " + baseFile);
      assertEquals(expectBloomFilter, footer.containsKey(HOODIE_MIN_RECORD_KEY_FOOTER),
          "min record key footer presence in " + baseFile);
      assertEquals(expectBloomFilter, footer.containsKey(HOODIE_MAX_RECORD_KEY_FOOTER),
          "max record key footer presence in " + baseFile);
    }
    assertEquals(rows.size(), spark().read().format("hudi").load(basePath()).count());
  }
}
