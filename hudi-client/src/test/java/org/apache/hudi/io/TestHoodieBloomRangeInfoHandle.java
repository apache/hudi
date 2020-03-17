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

package org.apache.hudi.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieJsonPayload;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.bloom.BloomIndexFileInfo;
import org.apache.hudi.table.HoodieTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestHoodieBloomRangeInfoHandle extends HoodieClientTestHarness {

  private String schemaStr;
  private Schema schema;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieBloomRangeInfoHandle");
    initPath();
    initFileSystem();
    // We have some records to be tagged (two different partitions)
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupSparkContexts();
    cleanupFileSystem();
    cleanupMetaClient();
  }

  @Test
  public void testBloomRangeInfoHandle() throws Exception {

    final String partitionPath = "2020/05/01";

    // 1. generate records
    List<HoodieRecord> records =
        IntStream.range(0, 10000).mapToObj(i -> {
          String rowKey = UUID.randomUUID().toString();

          ObjectNode jsonNode = new ObjectMapper().createObjectNode();
          jsonNode.put("_row_key", rowKey);
          jsonNode.put("time", String.valueOf(Instant.now().toEpochMilli()));
          jsonNode.put("number", i);

          try {
            return new HoodieRecord(new HoodieKey(rowKey, partitionPath), new HoodieJsonPayload(jsonNode.toString()));
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage());
          }
        }).collect(Collectors.toList());

    // 2. write records to file
    String filename =
            HoodieClientTestUtils.writeParquetFile(basePath, partitionPath, records, schema, null, true);

    // 3. create hoodie table
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).build();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieTable table = HoodieTable.create(metaClient, config, jsc.hadoopConfiguration());

    // 4. create handle
    HoodieBloomRangeInfoHandle handle = new HoodieBloomRangeInfoHandle(config, table, Pair.of(partitionPath, FSUtils.getFileId(filename)));
    BloomFilter bloomFilter = handle.getBloomFilter();
    BloomIndexFileInfo rangeInfo = handle.getRangeInfo();

    // 5. checks
    records.forEach(record -> {
      // check bloomFilter
      assertTrue(bloomFilter.mightContain(record.getRecordKey()));

      // check rangeInfo
      assertTrue(rangeInfo.isKeyInRange(record.getRecordKey()));
    });
  }
}
