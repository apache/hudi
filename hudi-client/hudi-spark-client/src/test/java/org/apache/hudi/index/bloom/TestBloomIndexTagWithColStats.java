/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.index.bloom;

import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.functional.TestHoodieMetadataBase;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestBloomIndexTagWithColStats extends TestHoodieMetadataBase {

  private static final Schema SCHEMA = getSchemaFromResource(TestBloomIndexTagWithColStats.class, "/exampleSchema.avsc", true);

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private void init(Properties props) throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    initMetaClient(props);
    writeClient = getHoodieWriteClient(makeConfig());
  }

  private HoodieWriteConfig makeConfig() {
    // For the bloom index to use column stats and bloom filters from metadata table,
    // the following configs must be set to true:
    // "hoodie.bloom.index.use.metadata"
    // "hoodie.metadata.enable" (by default is true)
    // "hoodie.metadata.index.column.stats.enable"
    // "hoodie.metadata.index.bloom.filter.enable"
    return HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.BLOOM)
            .bloomIndexPruneByRanges(true)
            .bloomIndexTreebasedFilter(true)
            .bloomIndexBucketizedChecking(true)
            .bloomIndexKeysPerBucket(2)
            .bloomIndexUseMetadata(true)
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withMetadataIndexBloomFilter(true)
            .withMetadataIndexColumnStats(true)
            .build())
        .withSchema(SCHEMA.toString())
        .build();
  }

  @Test
  public void testSimpleKeyGenerator() throws Exception {
    Properties props = new Properties();
    props.setProperty("hoodie.table.recordkey.fields", "_row_key");
    init(props);

    TypedProperties keyGenProperties = new TypedProperties();
    keyGenProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    keyGenProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "time");
    SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(keyGenProperties);

    testTagLocationOnPartitionedTable(keyGenerator);
  }

  @Test
  public void testComplexGeneratorWithMultiKeysSinglePartitionField() throws Exception {
    Properties props = new Properties();
    props.setProperty("hoodie.table.recordkey.fields", "_row_key,number");
    init(props);

    TypedProperties keyGenProperties = new TypedProperties();
    keyGenProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key,number");
    keyGenProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "time");
    ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(keyGenProperties);

    testTagLocationOnPartitionedTable(keyGenerator);
  }

  @Test
  public void testComplexGeneratorWithSingleKeyMultiPartitionFields() throws Exception {
    Properties props = new Properties();
    props.setProperty("hoodie.table.recordkey.fields", "_row_key");
    init(props);

    TypedProperties keyGenProperties = new TypedProperties();
    keyGenProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    keyGenProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "time,number");
    ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(keyGenProperties);

    testTagLocationOnPartitionedTable(keyGenerator);
  }

  private void testTagLocationOnPartitionedTable(KeyGenerator keyGenerator) throws Exception {
    GenericRecord genericRecord = generateGenericRecord("1", "2020", 1);
    HoodieRecord record =
        new HoodieAvroRecord(keyGenerator.getKey(genericRecord), new HoodieAvroPayload(Option.of(genericRecord)));
    JavaRDD<HoodieRecord> recordRDD = jsc.parallelize(Arrays.asList(record));

    HoodieWriteConfig config = makeConfig();
    HoodieSparkTable hoodieTable = HoodieSparkTable.create(config, context, metaClient);

    HoodieBloomIndex bloomIndex = new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
    JavaRDD<HoodieRecord> taggedRecordRDD = tagLocation(bloomIndex, recordRDD, hoodieTable);

    // Should not find any files
    assertFalse(taggedRecordRDD.first().isCurrentLocationKnown());

    WriteClientTestUtils.startCommitWithTime(writeClient, "001");
    JavaRDD<WriteStatus> status = writeClient.upsert(taggedRecordRDD, "001");
    String fileId = status.first().getFileId();

    metaClient = HoodieTableMetaClient.reload(metaClient);
    taggedRecordRDD = tagLocation(bloomIndex, recordRDD, HoodieSparkTable.create(config, context, metaClient));

    assertEquals(taggedRecordRDD.first().getCurrentLocation().getFileId(), fileId);
  }

  private GenericRecord generateGenericRecord(String rowKey, String time, int number) {
    GenericRecord rec = new GenericData.Record(SCHEMA);
    rec.put("_row_key", rowKey);
    rec.put("time", time);
    rec.put("number", number);
    return rec;
  }
}
