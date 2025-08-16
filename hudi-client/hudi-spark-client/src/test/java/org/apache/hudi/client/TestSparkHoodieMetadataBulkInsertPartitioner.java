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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.metadata.DefaultMetadataTableFileGroupIndexParser;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieMetadataBulkInsertPartitioner;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSparkHoodieMetadataBulkInsertPartitioner extends SparkClientFunctionalTestHarness {
  @Test
  public void testPartitioner() {
    List<HoodieRecord> records = new ArrayList<>();

    BiConsumer<String, Integer> initRecords = (fileGroupId, count) -> {
      while (count-- > 0) {
        HoodieRecord r = HoodieMetadataPayload.createPartitionListRecord(Collections.EMPTY_LIST);
        r.unseal();
        r.setCurrentLocation(new HoodieRecordLocation("001", fileGroupId));
        r.seal();
        records.add(r);
      }
    };

    // Assume there are 5 fileGroups in MDT partition FILES.
    initRecords.accept(MetadataPartitionType.FILES.getFileIdPrefix() + "000", 3);
    initRecords.accept(MetadataPartitionType.FILES.getFileIdPrefix() + "001", 5);
    initRecords.accept(MetadataPartitionType.FILES.getFileIdPrefix() + "002", 7);
    // Intentionally skipping fileGroup 003
    initRecords.accept(MetadataPartitionType.FILES.getFileIdPrefix() + "004", 9);
    // repeated fileGroups
    initRecords.accept(MetadataPartitionType.FILES.getFileIdPrefix() + "002", 11);

    SparkHoodieMetadataBulkInsertPartitioner partitioner = new SparkHoodieMetadataBulkInsertPartitioner(new DefaultMetadataTableFileGroupIndexParser(5));
    JavaRDD<HoodieRecord> partitionedRecords = partitioner.repartitionRecords(jsc().parallelize(records, records.size()), 0);

    // Only 5 partitions should be there corresponding to 5 unique fileGroups in MDT
    assertEquals(5, partitionedRecords.getNumPartitions(), "Only 5 partitions should be there corresponding to 3 unique fileGroups in MDT");

    // Records must be sorted as we are writing to HFile
    assertTrue(partitioner.arePartitionRecordsSorted(), "Must be sorted");

    // Each partition should only have records for a single fileGroup
    partitionedRecords.foreachPartition(recordIterator -> {
      HoodieRecordLocation location = null;
      while (recordIterator.hasNext()) {
        HoodieRecord record = recordIterator.next();
        HoodieRecordLocation recordLocation = record.getCurrentLocation();
        if (location == null) {
          location = recordLocation;
        } else {
          assertEquals(recordLocation, location, "Records should have the same location in a partition");
        }
      }
    });

    // Record count should match
    assertEquals(records.size(), partitionedRecords.count(), "Record count should match");

    // Number of records in each partition should be correct
    Map<String, Integer> recordsPerFileGroup = partitionedRecords.mapToPair(r -> new Tuple2<>(r.getCurrentLocation().getFileId(), 1))
        .reduceByKey(Integer::sum)
        .collectAsMap();
    assertEquals(3, recordsPerFileGroup.get(MetadataPartitionType.FILES.getFileIdPrefix() + "000"), "Number of records in each partition should be correct");
    assertEquals(5, recordsPerFileGroup.get(MetadataPartitionType.FILES.getFileIdPrefix() + "001"), "Number of records in each partition should be correct");
    assertEquals(7 + 11, recordsPerFileGroup.get(MetadataPartitionType.FILES.getFileIdPrefix() + "002"), "Number of records in each partition should be correct");
    assertEquals(9, recordsPerFileGroup.get(MetadataPartitionType.FILES.getFileIdPrefix() + "004"), "Number of records in each partition should be correct");
    assertEquals(-1, recordsPerFileGroup.getOrDefault(MetadataPartitionType.FILES.getFileIdPrefix() + "003", -1), "No records in skipped file group");

    // fileIDPrefixes should match the name of the MDT fileGroups
    Set<String> fileIDPrefixes = IntStream.of(0, 1, 2, 4).mapToObj(partitioner::getFileIdPfx).collect(Collectors.toSet());
    assertEquals(fileIDPrefixes, recordsPerFileGroup.keySet(), "fileIDPrefixes should match the name of the MDT fileGroups");
  }
}
