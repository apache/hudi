/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.index.SparkIndexerSupport;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkVectorIndexBootstrap extends SparkClientFunctionalTestHarness {

  @Test
  void testGetVectorIndexRecordsFromLocalVectorTable() throws Exception {
    String tablePath = java.net.URI.create(basePath()).getPath();
    String indexName = HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX + "vec_idx";
    int dim = 4;
    Properties tableProps = getPropertiesForKeyGen(true);
    tableProps.put("hoodie.datasource.write.precombine.field", "ts");
    tableProps.put(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), vectorWriteSchemaJson(dim));

    HoodieTableMetaClient metaClient = getHoodieMetaClient(storageConf(), tablePath, tableProps, COPY_ON_WRITE);

    HoodieWriteConfig writeConfig = getConfigBuilder(true)
        .withPath(tablePath)
        .withSchema(vectorWriteSchemaJson(dim))
        .withProperties(tableProps)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();

    String instantTime;
    try (SparkRDDWriteClient client = getHoodieWriteClient(writeConfig)) {
      instantTime = client.startCommit();
      List<HoodieRecord> inserts = buildVectorRecords(dim);
      List<WriteStatus> statuses = client.insert(jsc().parallelize(inserts, 1), instantTime).collect();
      assertNoWriteErrors(statuses);
      client.commit(instantTime, jsc().parallelize(statuses), Option.empty(), metaClient.getCommitActionType(), new HashMap<>());
    }

    metaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf())
        .setBasePath(tablePath)
        .build();

    HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
        .withIndexName(indexName)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX)
        .withIndexFunction("ivfflat")
        .withSourceFields(Arrays.asList("embedding"))
        .withIndexOptions(vectorIndexOptions(dim))
        .withVersion(HoodieIndexVersion.getCurrentVersion(HoodieTableVersion.current(), MetadataPartitionType.VECTOR_INDEX))
        .build();

    HoodieSchema tableSchema = new TableSchemaResolver(metaClient).getTableSchema();
    List<FileSliceAndPartition> fileSlices = collectLatestFileSlices(metaClient, writeConfig);
    SparkIndexerSupport indexerSupport = new SparkIndexerSupport(context(), writeConfig);
    List<HoodieRecord> records = HoodieJavaRDD.getJavaRDD(
        indexerSupport.generateVectorIndexRecords(
            indexDefinition, metaClient, fileSlices, tableSchema, 1, instantTime)).collect();
    List<String> recordKeys = records.stream()
        .map(record -> record.getKey().getRecordKey())
        .collect(Collectors.toList());
    List<Integer> families = recordKeys.stream().map(TestSparkVectorIndexBootstrap::family).collect(Collectors.toList());

    assertTrue(recordKeys.size() >= 4, "Generated vector index key families: " + families);
    assertTrue(families.contains(VectorIndexMetadataKey.FAMILY_CENTROIDS),
        "Expected a centroids record. Families: " + families);
    assertTrue(families.contains(VectorIndexMetadataKey.FAMILY_QUANTIZER),
        "Expected a quantizer record. Families: " + families);
    assertTrue(families.contains(VectorIndexMetadataKey.FAMILY_CLUSTER_STATS),
        "Expected a cluster-stats record. Families: " + families);
    assertTrue(families.contains(VectorIndexMetadataKey.FAMILY_POSTING),
        "Expected a posting record. Families: " + families);
    assertTrue(recordKeys.stream().anyMatch(TestSparkVectorIndexBootstrap::isManifestGenerationOne),
        "Expected generation-1 manifest key in fresh vector bootstrap. Families: " + families);
  }

  private static int family(String key) {
    byte[] bytes = VectorIndexMetadataKey.decode(key);
    return bytes.length == 0 ? -1 : Byte.toUnsignedInt(bytes[0]);
  }

  private List<FileSliceAndPartition> collectLatestFileSlices(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig) {
    String latestInstant = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
        .map(instant -> instant.requestedTime()).orElse("000");
    List<String> partitions = FSUtils.getAllPartitionPaths(context(), metaClient, writeConfig.getMetadataConfig());
    List<FileSliceAndPartition> fileSlices = new ArrayList<>();
    try (HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context(), metaClient, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants())) {
      fsView.loadAllPartitions();
      for (String partition : partitions) {
        fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant)
            .forEach(fileSlice -> fileSlices.add(FileSliceAndPartition.of(partition, fileSlice)));
      }
    }
    return fileSlices;
  }

  private static boolean isManifestGenerationOne(String key) {
    byte[] keyBytes = VectorIndexMetadataKey.decode(key);
    return keyBytes.length >= 5
        && Byte.toUnsignedInt(keyBytes[0]) == VectorIndexMetadataKey.FAMILY_MANIFEST
        && readUnsignedInt(keyBytes, 1) == 1;
  }

  private static int readUnsignedInt(byte[] bytes, int offset) {
    return (Byte.toUnsignedInt(bytes[offset]) << 24)
        | (Byte.toUnsignedInt(bytes[offset + 1]) << 16)
        | (Byte.toUnsignedInt(bytes[offset + 2]) << 8)
        | Byte.toUnsignedInt(bytes[offset + 3]);
  }

  private static List<HoodieRecord> buildVectorRecords(int dim) {
    Schema schema = new Schema.Parser().parse(vectorWriteSchemaJson(dim));
    return Arrays.asList(
        createRecord(schema, "id1", "p1", 1L, new float[] {1.0f, 0.0f, 0.0f, 0.0f}),
        createRecord(schema, "id2", "p1", 2L, new float[] {0.0f, 1.0f, 0.0f, 0.0f}),
        createRecord(schema, "id3", "p2", 3L, new float[] {0.0f, 0.0f, 1.0f, 0.0f}),
        createRecord(schema, "id4", "p2", 4L, new float[] {0.0f, 0.0f, 0.0f, 1.0f})
    );
  }

  private static HoodieRecord createRecord(Schema schema, String id, String partition, long ts, float[] values) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("_row_key", id);
    record.put("partition_path", partition);
    record.put("ts", ts);
    record.put("embedding", new GenericData.Fixed(schema.getField("embedding").schema(), toVectorBytes(values)));
    return new HoodieAvroRecord(new HoodieKey(id, partition), new org.apache.hudi.common.model.HoodieAvroPayload(Option.of(record)));
  }

  private static byte[] toVectorBytes(float[] values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES).order(org.apache.hudi.common.schema.HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    for (float value : values) {
      buffer.putFloat(value);
    }
    return buffer.array();
  }

  private static Map<String, String> vectorIndexOptions(int dim) {
    Map<String, String> opts = new HashMap<>();
    opts.put("vector.dimension", Integer.toString(dim));
    opts.put("vector.num_clusters", "2");
    opts.put("vector.metric", "l2");
    opts.put("vector.max_iter", "5");
    opts.put("vector.quantizer", "IVF_RABITQ");
    return opts;
  }

  private static String vectorWriteSchemaJson(int dim) {
    return "{"
        + "\"type\":\"record\","
        + "\"name\":\"vector_bootstrap_record\","
        + "\"namespace\":\"org.apache.hudi.metadata\","
        + "\"fields\":["
        + "{\"name\":\"_row_key\",\"type\":\"string\"},"
        + "{\"name\":\"partition_path\",\"type\":\"string\"},"
        + "{\"name\":\"ts\",\"type\":\"long\"},"
        + "{\"name\":\"embedding\",\"type\":{"
        + "\"type\":\"fixed\","
        + "\"name\":\"vector_float_" + dim + "\","
        + "\"size\":" + (dim * 4) + ","
        + "\"logicalType\":\"vector\","
        + "\"dimension\":" + dim + ","
        + "\"elementType\":\"FLOAT\","
        + "\"storageBacking\":\"FIXED_BYTES\""
        + "}}"
        + "]"
        + "}";
  }
}
