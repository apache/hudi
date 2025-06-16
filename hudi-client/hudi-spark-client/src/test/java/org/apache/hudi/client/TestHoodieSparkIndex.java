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

package org.apache.hudi.client;

import org.apache.hudi.index.HoodieSparkIndexClient;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.index.expression.ExpressionIndexSparkFunctions.IDENTITY_FUNCTION;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX_PREFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSparkIndex extends HoodieClientTestBase {

  @Test
  public void testIndexCreateAndDrop() throws IOException {
    HoodieWriteConfig cfg = getConfigBuilder().build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0x17AB);

    initMetaClient(HoodieTableType.COPY_ON_WRITE);

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      String commitTime1 = "001";
      WriteClientTestUtils.startCommitWithTime(client, commitTime1);
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime1, 200);
      JavaRDD<HoodieRecord> writeRecords1 = jsc.parallelize(records1, 1);
      List<WriteStatus> statuses1 = client.upsert(writeRecords1, commitTime1).collect();
      assertNoWriteErrors(statuses1);
    }

    HoodieSparkIndexClient sparkIndexClient = new HoodieSparkIndexClient(cfg, context);

    // add 4 indices(2 SI and 2 EI)
    String indexNamePrefix = "index";
    String fieldNamePrefix = "field";
    for (int i = 0; i < 5; i++) {
      String indexName = indexNamePrefix + "_" + i;
      String fieldName = fieldNamePrefix + "_" + i;
      HoodieIndexDefinition indexDefinition = getIndexDefinition(indexName,
          i % 2 == 0 ? PARTITION_NAME_SECONDARY_INDEX : PARTITION_NAME_EXPRESSION_INDEX, fieldName);
      HoodieIndexUtils.register(metaClient, indexDefinition);
      readAndValidateIndexDefn(indexDefinition);
    }

    // add col stats index.
    HoodieIndexDefinition colStatsIndexDefinition = getIndexDefinition(PARTITION_NAME_COLUMN_STATS,
        PARTITION_NAME_COLUMN_STATS, fieldNamePrefix + "_5");
    HoodieIndexUtils.register(metaClient, colStatsIndexDefinition);
    readAndValidateIndexDefn(colStatsIndexDefinition);

    // drop one among sec index and expression index.
    metaClient.deleteIndexDefinition(getIndexFullName(indexNamePrefix + "_1", PARTITION_NAME_EXPRESSION_INDEX));
    metaClient.deleteIndexDefinition(getIndexFullName(indexNamePrefix + "_2", PARTITION_NAME_SECONDARY_INDEX));

    //validate rest.
    HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.builder().setBasePath(metaClient.getBasePath()).setConf(metaClient.getStorageConf())
        .build();
    HoodieIndexDefinition indexDefinition = getIndexDefinition(indexNamePrefix + "_0", PARTITION_NAME_SECONDARY_INDEX, fieldNamePrefix + "_0");
    readAndValidateIndexDefn(indexDefinition, newMetaClient);
    indexDefinition = getIndexDefinition(indexNamePrefix + "_3", PARTITION_NAME_EXPRESSION_INDEX, fieldNamePrefix + "_3");
    readAndValidateIndexDefn(indexDefinition, newMetaClient);
    indexDefinition = getIndexDefinition(indexNamePrefix + "_4", PARTITION_NAME_SECONDARY_INDEX, fieldNamePrefix + "_4");
    readAndValidateIndexDefn(indexDefinition, newMetaClient);
    readAndValidateIndexDefn(colStatsIndexDefinition);

    // update col stats w/ new set of cols
    List<String> colsToIndex = IntStream.range(0, 10).mapToObj(number -> fieldNamePrefix + "_" + number).collect(Collectors.toList());
    colStatsIndexDefinition = getIndexDefinition(PARTITION_NAME_COLUMN_STATS,
        PARTITION_NAME_COLUMN_STATS, PARTITION_NAME_COLUMN_STATS, colsToIndex, Collections.EMPTY_MAP);
    HoodieIndexUtils.register(metaClient, colStatsIndexDefinition);
    readAndValidateIndexDefn(colStatsIndexDefinition);

    // drop col stats
    metaClient.deleteIndexDefinition(colStatsIndexDefinition.getIndexName());
    readAndValidateIndexDefnNotPresent(colStatsIndexDefinition, HoodieTableMetaClient.builder().setBasePath(metaClient.getBasePath()).setConf(metaClient.getStorageConf())
        .build());
  }

  private void readAndValidateIndexDefn(HoodieIndexDefinition expectedIndexDefn) {
    HoodieTableMetaClient newMetaClient = HoodieTableMetaClient.builder().setBasePath(metaClient.getBasePath()).setConf(metaClient.getStorageConf())
        .build();
    readAndValidateIndexDefn(expectedIndexDefn, newMetaClient);
  }

  private void readAndValidateIndexDefn(HoodieIndexDefinition expectedIndexDefn, HoodieTableMetaClient metaClient) {
    assertTrue(metaClient.getIndexMetadata().isPresent());
    assertTrue(!metaClient.getIndexMetadata().get().getIndexDefinitions().isEmpty());
    assertTrue(metaClient.getIndexMetadata().get().getIndexDefinitions().containsKey(expectedIndexDefn.getIndexName()));
    assertEquals(expectedIndexDefn, metaClient.getIndexMetadata().get().getIndexDefinitions().get(expectedIndexDefn.getIndexName()));
  }

  private void readAndValidateIndexDefnNotPresent(HoodieIndexDefinition expectedIndexDefn, HoodieTableMetaClient metaClient) {
    assertTrue(metaClient.getIndexMetadata().isPresent());
    assertTrue(!metaClient.getIndexMetadata().get().getIndexDefinitions().isEmpty());
    assertTrue(!metaClient.getIndexMetadata().get().getIndexDefinitions().containsKey(expectedIndexDefn.getIndexName()));
  }

  private HoodieIndexDefinition getIndexDefinition(String indexName, String indexType, String sourceField) {
    return getIndexDefinition(indexName, indexType, IDENTITY_FUNCTION, Collections.singletonList(sourceField), Collections.emptyMap());
  }

  private HoodieIndexDefinition getIndexDefinition(String indexName, String indexType, String indexFunc, List<String> sourceFields,
                                                   Map<String, String> indexOptions) {
    String fullIndexName = getIndexFullName(indexName, indexType);
    return HoodieIndexDefinition.newBuilder()
        .withIndexName(fullIndexName)
        .withIndexType(indexType)
        .withIndexFunction(indexFunc)
        .withSourceFields(sourceFields)
        .withIndexOptions(indexOptions)
        .build();
  }

  private String getIndexFullName(String indexName, String indexType) {
    if (indexName.equals(PARTITION_NAME_COLUMN_STATS)) {
      return PARTITION_NAME_COLUMN_STATS;
    }
    return indexType.equals(PARTITION_NAME_SECONDARY_INDEX)
        ? PARTITION_NAME_SECONDARY_INDEX_PREFIX + indexName
        : PARTITION_NAME_EXPRESSION_INDEX_PREFIX + indexName;
  }
}
