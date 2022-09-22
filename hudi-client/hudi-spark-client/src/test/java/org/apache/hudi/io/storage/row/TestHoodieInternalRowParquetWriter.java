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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link HoodieInternalRowParquetWriter}.
 */
public class TestHoodieInternalRowParquetWriter extends HoodieClientTestHarness {

  private static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieInternalRowParquetWriter");
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void endToEndTest(boolean parquetWriteLegacyFormatEnabled) throws Exception {
    HoodieWriteConfig.Builder writeConfigBuilder =
        SparkDatasetTestUtils.getConfigBuilder(basePath, timelineServicePort);
    for (int i = 0; i < 5; i++) {
      // init write support and parquet config
      HoodieRowParquetWriteSupport writeSupport = getWriteSupport(writeConfigBuilder, hadoopConf, parquetWriteLegacyFormatEnabled);
      HoodieWriteConfig cfg = writeConfigBuilder.build();
      HoodieParquetConfig<HoodieRowParquetWriteSupport> parquetConfig = new HoodieParquetConfig<>(writeSupport,
          CompressionCodecName.SNAPPY, cfg.getParquetBlockSize(), cfg.getParquetPageSize(), cfg.getParquetMaxFileSize(),
          writeSupport.getHadoopConf(), cfg.getParquetCompressionRatio(), cfg.parquetDictionaryEnabled());

      // prepare path
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(basePath + "/" + fileId);
      String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
      metaClient.getFs().mkdirs(new Path(basePath));

      // init writer
      HoodieInternalRowParquetWriter writer = new HoodieInternalRowParquetWriter(filePath, parquetConfig);

      // generate input
      int size = 10 + RANDOM.nextInt(100);
      // Generate inputs
      Dataset<Row> inputRows = SparkDatasetTestUtils.getRandomRows(sqlContext, size, partitionPath, false);
      List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(inputRows, SparkDatasetTestUtils.ENCODER);

      // issue writes
      for (InternalRow internalRow : internalRows) {
        writer.write(internalRow);
      }

      // close the writer
      writer.close();

      // verify rows
      Dataset<Row> result = sqlContext.read().parquet(basePath);
      assertEquals(0, inputRows.except(result).count());
    }
  }

  private HoodieRowParquetWriteSupport getWriteSupport(HoodieWriteConfig.Builder writeConfigBuilder, Configuration hadoopConf, boolean parquetWriteLegacyFormatEnabled) {
    writeConfigBuilder.withStorageConfig(HoodieStorageConfig.newBuilder().parquetWriteLegacyFormat(String.valueOf(parquetWriteLegacyFormatEnabled)).build());
    HoodieWriteConfig writeConfig = writeConfigBuilder.build();
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        writeConfig.getBloomFilterNumEntries(),
        writeConfig.getBloomFilterFPP(),
        writeConfig.getDynamicBloomFilterMaxNumEntries(),
        writeConfig.getBloomFilterType());
    return new HoodieRowParquetWriteSupport(hadoopConf, SparkDatasetTestUtils.STRUCT_TYPE, Option.of(filter), writeConfig.getStorageConfig());
  }
}
