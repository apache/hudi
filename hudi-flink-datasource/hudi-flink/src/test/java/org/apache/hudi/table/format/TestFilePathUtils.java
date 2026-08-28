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

package org.apache.hudi.table.format;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.storage.StoragePath;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link FilePathUtils}.
 */
class TestFilePathUtils {

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  void testGenerateAndUnescapePartitionPath() {
    LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
    partitionSpec.put("region", "us/west");
    partitionSpec.put("day", "2026-07-28");

    assertEquals(
        "region=us%2Fwest/day=2026-07-28/",
        FilePathUtils.generatePartitionPath(partitionSpec, true, true));
    assertEquals(
        "us%2Fwest/2026-07-28",
        FilePathUtils.generatePartitionPath(partitionSpec, false, false));
    assertEquals("", FilePathUtils.generatePartitionPath(new LinkedHashMap<>(), true, true));

    assertEquals("a/b=c%invalid", FilePathUtils.unescapePathName("a%2Fb%3Dc%invalid"));
    assertEquals("trailing%", FilePathUtils.unescapePathName("trailing%"));
    assertThrows(
        TableException.class,
        () -> FilePathUtils.generatePartitionPath(
            new LinkedHashMap<>(Collections.singletonMap("region", "")), true, false));
  }

  @Test
  void testExtractPartitionKeyValues() {
    assertEquals(
        linkedMap("region", "us/west", "day", "2026-07-28"),
        FilePathUtils.extractPartitionKeyValues(
            new Path("/table/region=us%2Fwest/day=2026-07-28"),
            true,
            new String[] {"region", "day"}));
    assertEquals(
        linkedMap("region", "us/west", "day", "2026-07-28"),
        FilePathUtils.extractPartitionKeyValues(
            new Path("/table/us%2Fwest/2026-07-28"),
            false,
            new String[] {"region", "day"}));
    assertTrue(FilePathUtils.extractPartitionKeyValues(
        new Path("/table"), true, new String[0]).isEmpty());
  }

  @Test
  void testGeneratePartitionSpecs() {
    List<String> fieldNames = Arrays.asList("region", "day", "event_time");
    List<DataType> fieldTypes = Arrays.asList(
        DataTypes.STRING(), DataTypes.INT(), DataTypes.TIMESTAMP(3));

    assertEquals(
        objectMap("region", "us", "day", 28),
        FilePathUtils.generatePartitionSpecs(
            "/table/region=us/day=28/event_time=2026-07-28%2010%3A15/data.parquet",
            fieldNames,
            fieldTypes,
            FlinkOptions.PARTITION_DEFAULT_NAME.defaultValue(),
            "region,day,event_time",
            true));

    assertTrue(FilePathUtils.generatePartitionSpecs(
        "/table/data.parquet",
        fieldNames,
        fieldTypes,
        FlinkOptions.PARTITION_DEFAULT_NAME.defaultValue(),
        FlinkOptions.PARTITION_PATH_FIELD.defaultValue(),
        true).isEmpty());
  }

  @Test
  void testRecursivePartitionDiscoveryAndHiddenPaths() throws IOException {
    Files.createDirectories(tempDir.resolve("region=us/day=28"));
    Files.createDirectories(tempDir.resolve("region=eu/day=29"));
    Files.createDirectories(tempDir.resolve("_temporary/day=30"));
    Files.createDirectories(tempDir.resolve(".hidden/day=31"));
    Files.createDirectories(tempDir.resolve(".file.log.1/day=32"));

    Path root = new Path(tempDir.toUri());
    FileSystem fs = root.getFileSystem(new org.apache.hadoop.conf.Configuration());
    FileStatus[] statuses = FilePathUtils.getFileStatusRecursively(root, 2, fs);
    assertEquals(3, statuses.length);

    List<Tuple2<LinkedHashMap<String, String>, Path>> partitions =
        FilePathUtils.searchPartKeyValueAndPaths(
            fs, root, true, new String[] {"region", "day"});
    assertEquals(3, partitions.size());
    assertTrue(partitions.stream().anyMatch(
        tuple -> "us".equals(tuple.f0.get("region")) && "28".equals(tuple.f0.get("day"))));

    assertEquals(0, FilePathUtils.getFileStatusRecursively(
        new Path(root, "missing"), 1, fs).length);
  }

  @Test
  void testGetPartitionsResolvesDefaultValue() throws IOException {
    String defaultPartition = FlinkOptions.PARTITION_DEFAULT_NAME.defaultValue();
    Files.createDirectories(tempDir.resolve("region=" + defaultPartition));
    Files.createDirectories(tempDir.resolve("region=us"));

    List<Map<String, String>> partitions = FilePathUtils.getPartitions(
        new Path(tempDir.toUri()),
        new org.apache.hadoop.conf.Configuration(),
        Collections.singletonList("region"),
        defaultPartition,
        true);

    assertEquals(2, partitions.size());
    assertTrue(partitions.stream().anyMatch(partition -> partition.containsKey("region")
        && partition.get("region") == null));
    assertTrue(partitions.stream().anyMatch(partition -> "us".equals(partition.get("region"))));
  }

  @Test
  void testValidateAndConvertPartitionPaths() {
    Map<String, String> unordered = new LinkedHashMap<>();
    unordered.put("day", "28");
    unordered.put("region", "us");

    assertEquals(
        linkedMap("region", "us", "day", "28"),
        FilePathUtils.validateAndReorderPartitions(
            unordered, Arrays.asList("region", "day")));
    assertEquals(
        unordered,
        FilePathUtils.validateAndReorderPartitions(unordered, Collections.emptyList()));
    assertThrows(
        TableException.class,
        () -> FilePathUtils.validateAndReorderPartitions(
            Collections.singletonMap("region", "us"), Arrays.asList("region", "day")));

    List<Map<String, String>> partitionPaths =
        Collections.singletonList(unordered);
    assertArrayEquals(
        new Path[] {new Path("/table/region=us/day=28/")},
        FilePathUtils.partitionPath2ReadPath(
            new Path("/table"), Arrays.asList("region", "day"), partitionPaths, true));
    assertEquals(
        Collections.singleton("us/28"),
        FilePathUtils.toRelativePartitionPaths(
            Arrays.asList("region", "day"), partitionPaths, false));
  }

  @Test
  void testReadPathsAndPathConversions() throws IOException {
    Path root = new Path(tempDir.toUri());
    Configuration flinkConf = new Configuration();
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

    assertArrayEquals(
        new Path[] {root},
        FilePathUtils.getReadPaths(
            root, flinkConf, hadoopConf, Collections.emptyList()));

    Path[] hadoopPaths = {new Path("/table/a"), new Path("/table/b")};
    org.apache.flink.core.fs.Path[] flinkPaths = FilePathUtils.toFlinkPaths(hadoopPaths);
    assertEquals(hadoopPaths[0].toUri(), flinkPaths[0].toUri());
    assertEquals(
        new StoragePath("/table/c").toUri(),
        FilePathUtils.toFlinkPath(new StoragePath("/table/c")).toUri());
  }

  @Test
  void testExtractPartitionConfiguration() {
    Configuration conf = new Configuration();
    assertArrayEquals(new String[0], FilePathUtils.extractPartitionKeys(conf));
    assertArrayEquals(new String[0], FilePathUtils.extractHivePartitionFields(conf));

    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "region,day");
    assertArrayEquals(
        new String[] {"region", "day"}, FilePathUtils.extractPartitionKeys(conf));
    assertArrayEquals(
        new String[] {"region", "day"}, FilePathUtils.extractHivePartitionFields(conf));

    conf.set(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS, "country,date");
    assertArrayEquals(
        new String[] {"country", "date"}, FilePathUtils.extractHivePartitionFields(conf));
    assertTrue(FilePathUtils.isHiveStylePartitioning("region=us"));
    assertFalse(FilePathUtils.isHiveStylePartitioning("us"));
    assertFalse(FilePathUtils.isHiveStylePartitioning("region=us/day=28"));
  }

  private static LinkedHashMap<String, String> linkedMap(String... entries) {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();
    for (int index = 0; index < entries.length; index += 2) {
      result.put(entries[index], entries[index + 1]);
    }
    return result;
  }

  private static LinkedHashMap<String, Object> objectMap(Object... entries) {
    LinkedHashMap<String, Object> result = new LinkedHashMap<>();
    for (int index = 0; index < entries.length; index += 2) {
      result.put((String) entries[index], entries[index + 1]);
    }
    return result;
  }
}
