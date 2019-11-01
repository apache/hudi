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

package org.apache.hudi.bench.generator;///*
// *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *           http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// *
// */
//
//package org.apache.hudi.bench.generator;
//
//import static junit.framework.TestCase.assertEquals;
//import static junit.framework.TestCase.assertFalse;
//import static org.mockito.Mockito.when;
//
//import org.apache.hudi.bench.configuration.DeltaConfig.Config;
//import org.apache.hudi.bench.input.config.DFSDeltaConfig;
//import org.apache.hudi.bench.input.config.DeltaConfig;
//import org.apache.hudi.common.SerializableConfiguration;
//import org.apache.hudi.common.util.FSUtils;
//import org.apache.hudi.bench.job.operation.Operation;
//import org.apache.hudi.bench.input.DeltaInputFormat;
//import org.apache.hudi.bench.input.DeltaOutputType;
//import org.apache.hudi.bench.input.reader.DFSAvroDeltaInputReader;
//import org.apache.hudi.bench.utils.TestUtils;
//import org.apache.hudi.utilities.UtilitiesTestBase;
//import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//import org.apache.hudi.common.util.Option;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.spark.api.java.JavaRDD;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.mockito.Mockito;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//
//// NOTE : Need the following to ensure that local objects are mocked using PowerMockito but this clashes with
//// HDFSMiniCluster setup by reloading some classes leading to incorrect path/permissions issue
//// @RunWith(PowerMockRunner.class)
//// @PowerMockIgnore({"org.apache.apache._", "com.sun.*"})
//@PrepareForTest({DFSAvroDeltaInputReader.class})
//public class TestWorkloadGenerator extends UtilitiesTestBase {
//
//  private FilebasedSchemaProvider schemaProvider;
//
//  @BeforeClass
//  public static void initClass() throws Exception {
//    UtilitiesTestBase.initClass();
//  }
//
//  @AfterClass
//  public static void cleanupClass() throws Exception {
//    UtilitiesTestBase.cleanupClass();
//  }
//
//  @Before
//  public void setup() throws Exception {
//    super.setup();
//    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS("hudi-bench-config/source.avsc"),
//        jsc);
//  }
//
//  @After
//  public void teardown() throws Exception {
//    super.teardown();
//  }
//
//  @Test
//  @Ignore
//  public void testInsertWorkloadGenerator() throws Exception {
//    LazyRecordGeneratorIterator mockLazyRecordGeneratorIterator = Mockito.mock(LazyRecordGeneratorIterator.class);
//    when(mockLazyRecordGeneratorIterator.hasNext()).thenReturn(true, true, true, true, true, false);
//    PowerMockito.whenNew(LazyRecordGeneratorIterator.class)
//        .withArguments(new FlexibleSchemaRecordGenerationIterator(5, schemaProvider.getSourceSchema()
//            .toString())).thenReturn(mockLazyRecordGeneratorIterator);
//    // Perform upserts of all 5 records
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 5, 0, 1, 1024, 1, Option.empty(), false);
//    DeltaConfig mockSinkConfig = Mockito.mock(DeltaConfig.class);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(mockSinkConfig, );
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator.generateInserts(jsc, dataGenerationConfig, schemaProvider
//        .getSourceSchema().toString(), Collections.emptyList());
//    assertEquals(inputRDD.count(), 5);
//  }
//
//  @Test
//  public void testGetPartitionToCountMap() {
//    DFSDeltaConfig mockSinkConfig = Mockito.mock(DFSDeltaConfig.class);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(mockSinkConfig, );
//    Long actualNumRecords = 5L;
//    JavaRDD<GenericRecord> rdd = TestUtils.makeRDD(jsc, actualNumRecords.intValue());
//    // Test for 1 spark partition
//    int numPartitions = rdd.getNumPartitions();
//    Map<Integer, Long> map = workloadGenerator.getPartitionToCountMap(rdd);
//    // Total num spark partitions should be 1
//    assertEquals(map.size(), numPartitions);
//    // Total records in 1 num spark partition should be 5
//    Long totalRecord = map.get(0);
//    assertEquals(totalRecord, actualNumRecords);
//    // Test for 2 spark partitions
//    rdd = rdd.repartition(2);
//    numPartitions = rdd.getNumPartitions();
//    map = workloadGenerator.getPartitionToCountMap(rdd);
//    // Total num spark partitions should be 2
//    assertEquals(map.size(), numPartitions);
//    // Total records in 2 num spark partitions should be 5
//    totalRecord = map.get(0) + map.get(1);
//    assertEquals(totalRecord, actualNumRecords);
//  }
//
//  @Test
//  public void testGetAdjustedPartitionsCount() {
//    DFSDeltaConfig mockSinkConfig = Mockito.mock(DFSDeltaConfig.class);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(mockSinkConfig, );
//    // Test for 1 spark partition
//    Map<Integer, Long> partitionCountMap = new HashMap<>();
//    long totalInitRecords = 10L;
//    partitionCountMap.put(0, totalInitRecords);
//    Map<Integer, Long> adjustedPartitionsCount = workloadGenerator.getAdjustedPartitionsCount(partitionCountMap, 5);
//    assertEquals(adjustedPartitionsCount.size(), 1);
//    Long newCount = adjustedPartitionsCount.get(0);
//    Long totalNewRecordsExpected = 5L;
//    assertEquals(newCount, totalNewRecordsExpected);
//    // Test for 3 spark partitions
//    partitionCountMap = new HashMap<>();
//    partitionCountMap.put(0, 3L);
//    partitionCountMap.put(1, 4L);
//    partitionCountMap.put(2, 3L);
//    adjustedPartitionsCount = workloadGenerator.getAdjustedPartitionsCount(partitionCountMap, 5);
//    // Should remove 1 partition completely since all partitions have count less than five
//    assertEquals(adjustedPartitionsCount.size(), 2);
//    // Should decrement 1 partition to remove total of 5 records
//    Map.Entry<Integer, Long> partitionToAdjust = adjustedPartitionsCount.entrySet().iterator().next();
//    newCount = partitionToAdjust.getValue();
//    Long originalValueForPartition = partitionCountMap.get(partitionToAdjust.getKey());
//    assertFalse(newCount == originalValueForPartition);
//  }
//
//  @Test
//  public void testAdjustRDDToGenerateExactNumUpdates() {
//    DFSDeltaConfig mockSinkConfig = Mockito.mock(DFSDeltaConfig.class);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(mockSinkConfig, );
//    long totalRecordsRequired = 10;
//    JavaRDD<GenericRecord> updates = TestUtils.makeRDD(jsc, 5);
//    // Test flow to generate more updates
//    JavaRDD<GenericRecord> adjustedRDD = workloadGenerator.adjustRDDToGenerateExactNumUpdates(updates, jsc,
//        totalRecordsRequired);
//    assertEquals(adjustedRDD.count(), totalRecordsRequired);
//    totalRecordsRequired = 100;
//    adjustedRDD = workloadGenerator.adjustRDDToGenerateExactNumUpdates(updates, jsc, totalRecordsRequired);
//    assertEquals(adjustedRDD.count(), totalRecordsRequired);
//    // Test flow to generate less updates
//    totalRecordsRequired = 3;
//    adjustedRDD = workloadGenerator.adjustRDDToGenerateExactNumUpdates(updates, jsc,
//        totalRecordsRequired);
//    assertEquals(adjustedRDD.count(), totalRecordsRequired);
//  }
//
//  @Test
//  public void testDFSAvroWorkloadGeneratorSimple() throws IOException {
//    // Perform inserts of 1000 records
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 1024 * 1024L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 1000, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    assertEquals(inputRDD.count(), 1000);
//    // Write 1 file for all these inserts
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 1);
//    // Perform upserts of all 1000 records
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 1000, 1, 1024, 1, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Collections.emptyList(), schemaProvider.getSourceSchema().toString());
//    assertEquals(inputRDD.count(), 1000);
//    // Write 1 file for all these upserts
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 1);
//    FileSystem fs = FSUtils.getFs(dfsBasePath, sinkConfig.getConfiguration());
//    FileStatus[] fileStatuses = fs.globStatus(new Path(dfsBasePath + "/*/*.avro"));
//    // 2 files should be present, 1 for inserts and 1 for upserts
//    assertEquals(fileStatuses.length, 2);
//  }
//
//  @Test
//  public void testGenerateUpdatesFromGreaterNumExistingInserts() throws IOException {
//    // Perform inserts of 1000 records
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 1024 * 1024L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 1000, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 1);
//    // Perform upserts of 500 records
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 1000, 1, 1024, 1, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Collections.emptyList(), schemaProvider.getSourceSchema().toString());
//    // We should be able to generate 500 updates from the previously inserted 1000 records
//    assertEquals(inputRDD.count(), 1000);
//  }
//
//  @Test
//  public void testGenerateUpdatesFromSmallerNumExistingInserts() throws IOException {
//    // Perform inserts of 1000 records
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 1024 * 1024L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 500, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    // Ensure that 500 inserts were written to 1 file
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 1);
//    // Perform upserts of 1000 records
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 1000, 1, 1024, 1, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Collections.emptyList(), schemaProvider.getSourceSchema().toString());
//    // We should be able to generate 1000 updates from the previously inserted 500 records
//    assertEquals(inputRDD.count(), 1000);
//  }
//
//  @Test
//  public void testGenerateUpdatesFromMuchSmallerNumExistingInserts() throws IOException {
//    // Perform inserts of 1000 records
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 1024 * 1024L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 1000, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    // Ensure that 1000 inserts were written to 1 file
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 1);
//    // Perform upserts of 10000 records
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 10000, 1, 1024, 1, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Collections.emptyList(), schemaProvider.getSourceSchema().toString());
//    // We should be able to generate 10000 updates from the previously inserted 1000 records
//    assertEquals(inputRDD.count(), 10000);
//  }
//
//  @Test
//  public void testAdjustUpdatesFromGreaterNumUpdates() throws IOException {
//    // Perform inserts of 1000 records into 3 files
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 10240L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 1000, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 3);
//    // Perform upsert of 450 records. This will force us to readAvro min (3) files with more than 450 records readAvro
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 450, 1, 1024, 1, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Collections.emptyList(), schemaProvider.getSourceSchema().toString());
//    // We should be able to generate 450 updates from the previously inserted 1000 records
//    assertEquals(inputRDD.count(), 450);
//  }
//
//  @Test
//  public void testUpdateGeneratorForNoPartitions() throws IOException {
//    // Perform inserts of 1000 records into 3 files
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 10240L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 1000, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 3);
//    // Perform upsert of 450 records. This will force us to readAvro min (3) files with more than 450 records readAvro
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 450, 1, 1024, 0, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Collections.emptyList(), schemaProvider.getSourceSchema().toString());
//    assertEquals(inputRDD.getNumPartitions(), 1);
//    // We should be able to generate 150 updates from the previously inserted 1000 records
//    assertEquals(inputRDD.count(), 450);
//
//  }
//
//  @Test
//  public void testUpdateGeneratorForInvalidPartitionFieldNames() throws IOException {
//    // Perform inserts of 1000 records into 3 files
//    DeltaConfig sinkConfig = new DFSDeltaConfig(DeltaOutputType.DFS, DeltaInputFormat.AVRO,
//        new SerializableConfiguration(jsc.hadoopConfiguration()), dfsBasePath, schemaProvider.getSourceSchema()
//        .toString(), 10240L);
//    DeltaGenerator workloadGenerator = new DeltaGenerator(sinkConfig, );
//    Config dataGenerationConfig =
//        new Config(Operation.INSERT, 1000, 0, 1, 1024, 1, Option.empty(), false);
//    JavaRDD<GenericRecord> inputRDD = workloadGenerator
//        .generateInserts(jsc, dataGenerationConfig, schemaProvider.getSourceSchema().toString(), Collections
//            .emptyList());
//    assertEquals(workloadGenerator.writeRecords(inputRDD, 1).collect().size(), 3);
//    // Perform upsert of 450 records. This will force us to readAvro min (3) files with more than 450 records readAvro
//    dataGenerationConfig =
//        new Config(Operation.UPSERT, 0, 450, 1, 1024, 0, Option.empty(), false);
//    inputRDD = workloadGenerator
//        .generateUpdates(jsc, sparkSession, dataGenerationConfig,
//            Arrays.asList("_row_key"), Arrays.asList("not_there"), schemaProvider.getSourceSchema().toString());
//    assertEquals(inputRDD.getNumPartitions(), 1);
//    // We should be able to generate 450 updates from the previously inserted 1000 records
//    assertEquals(inputRDD.count(), 450);
//
//  }
//
//}
