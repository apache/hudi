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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.createBaseFile;
import static org.apache.hudi.utilities.config.DFSPathSelectorConfig.ROOT_INPUT_PATH;
import static org.apache.hudi.utilities.config.DatePartitionPathSelectorConfig.PARTITIONS_LIST_PARALLELISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDFSPathSelectorCommonMethods extends HoodieSparkClientTestHarness {

  TypedProperties props;
  Path inputPath;

  @BeforeEach
  void setUp() {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    props = new TypedProperties();
    props.setProperty(ROOT_INPUT_PATH.key(), basePath);
    props.setProperty(PARTITIONS_LIST_PARALLELISM.key(), "1");
    inputPath = new Path(basePath);
  }

  @AfterEach
  public void teardown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(classes = {DFSPathSelector.class, DatePartitionPathSelector.class})
  public void listEligibleFilesShouldIgnoreCertainPrefixes(Class<?> clazz) throws Exception {
    DFSPathSelector selector = (DFSPathSelector) ReflectionUtils.loadClass(clazz.getName(), props, storageConf.unwrap());
    createBaseFile(basePath, "p1", "000", "foo1", 1);
    createBaseFile(basePath, "p1", "000", ".foo2", 1);
    createBaseFile(basePath, "p1", "000", "_foo3", 1);

    List<FileStatus> eligibleFiles = selector.listEligibleFiles(
        (FileSystem) storage.getFileSystem(), inputPath, 0);
    assertEquals(1, eligibleFiles.size());
    assertTrue(eligibleFiles.get(0).getPath().getName().startsWith("foo1"));
  }

  @ParameterizedTest
  @ValueSource(classes = {DFSPathSelector.class, DatePartitionPathSelector.class})
  public void listEligibleFilesShouldIgnore0LengthFiles(Class<?> clazz) throws Exception {
    DFSPathSelector selector = (DFSPathSelector) ReflectionUtils.loadClass(clazz.getName(), props, storageConf.unwrap());
    createBaseFile(basePath, "p1", "000", "foo1", 1);
    createBaseFile(basePath, "p1", "000", "foo2", 0);
    createBaseFile(basePath, "p1", "000", "foo3", 0);

    List<FileStatus> eligibleFiles = selector.listEligibleFiles(
        (FileSystem) storage.getFileSystem(), inputPath, 0);
    assertEquals(1, eligibleFiles.size());
    assertTrue(eligibleFiles.get(0).getPath().getName().startsWith("foo1"));
  }

  @ParameterizedTest
  @ValueSource(classes = {DFSPathSelector.class, DatePartitionPathSelector.class})
  public void listEligibleFilesShouldIgnoreFilesEarlierThanCheckpointTime(Class<?> clazz) throws Exception {
    DFSPathSelector selector = (DFSPathSelector) ReflectionUtils.loadClass(clazz.getName(), props, storageConf.unwrap());
    createBaseFile(basePath, "p1", "000", "foo1", 1);
    createBaseFile(basePath, "p1", "000", "foo2", 1);
    createBaseFile(basePath, "p1", "000", "foo3", 1);

    List<FileStatus> eligibleFiles = selector.listEligibleFiles(
        (FileSystem) storage.getFileSystem(), inputPath, Long.MAX_VALUE);
    assertEquals(0, eligibleFiles.size());
  }

  @ParameterizedTest
  @ValueSource(classes = {DFSPathSelector.class, DatePartitionPathSelector.class})
  public void getNextFilePathsAndMaxModificationTimeShouldRespectSourceLimit(Class<?> clazz) throws Exception {
    DFSPathSelector selector = (DFSPathSelector) ReflectionUtils.loadClass(clazz.getName(), props, storageConf.unwrap());
    createBaseFile(basePath, "p1", "000", "foo1", 10, 1000);
    createBaseFile(basePath, "p1", "000", "foo2", 10, 2000);
    createBaseFile(basePath, "p1", "000", "foo3", 10, 3000);
    createBaseFile(basePath, "p1", "000", "foo4", 10, 4000);
    createBaseFile(basePath, "p1", "000", "foo5", 10, 5000);
    Pair<Option<String>, Checkpoint> nextFilePathsAndCheckpoint = selector
        .getNextFilePathsAndMaxModificationTime(jsc, Option.empty(), 30);
    List<String> fileNames = Arrays
        .stream(nextFilePathsAndCheckpoint.getLeft().get().split(","))
        .map(p -> Paths.get(p).toFile().getName())
        .sorted().collect(Collectors.toList());
    assertEquals(2, fileNames.size());
    assertTrue(fileNames.get(0).startsWith("foo1"));
    assertTrue(fileNames.get(1).startsWith("foo2"));
    String checkpointStr1stRead = nextFilePathsAndCheckpoint.getRight().getCheckpointKey();
    assertEquals(2000L, Long.parseLong(checkpointStr1stRead), "should read up to foo2 (inclusive)");
  }

  @ParameterizedTest
  @ValueSource(classes = {DFSPathSelector.class, DatePartitionPathSelector.class})
  public void getNextFilePathsAndMaxModificationTimeShouldIgnoreSourceLimitIfSameModTimeFilesPresent(Class<?> clazz) throws Exception {
    DFSPathSelector selector = (DFSPathSelector) ReflectionUtils.loadClass(clazz.getName(), props, storageConf.unwrap());
    createBaseFile(basePath, "p1", "000", "foo1", 10, 1000);
    createBaseFile(basePath, "p1", "000", "foo2", 10, 1000);
    createBaseFile(basePath, "p1", "000", "foo3", 10, 1000);
    createBaseFile(basePath, "p1", "000", "foo4", 10, 2000);
    createBaseFile(basePath, "p1", "000", "foo5", 10, 2000);
    Pair<Option<String>, Checkpoint> nextFilePathsAndCheckpoint = selector
        .getNextFilePathsAndMaxModificationTime(jsc, Option.empty(), 20);
    List<String> fileNames1stRead = Arrays
        .stream(nextFilePathsAndCheckpoint.getLeft().get().split(","))
        .map(p -> Paths.get(p).toFile().getName())
        .sorted().collect(Collectors.toList());
    assertEquals(3, fileNames1stRead.size());
    assertTrue(fileNames1stRead.get(0).startsWith("foo1"));
    assertTrue(fileNames1stRead.get(1).startsWith("foo2"));
    assertTrue(fileNames1stRead.get(2).startsWith("foo3"));
    String checkpointStr1stRead = nextFilePathsAndCheckpoint.getRight().getCheckpointKey();
    assertEquals(1000L, Long.parseLong(checkpointStr1stRead), "should read up to foo3 (inclusive)");

    nextFilePathsAndCheckpoint = selector
        .getNextFilePathsAndMaxModificationTime(jsc, Option.of(new StreamerCheckpointV2(checkpointStr1stRead)), 20);
    List<String> fileNames2ndRead = Arrays
        .stream(nextFilePathsAndCheckpoint.getLeft().get().split(","))
        .map(p -> Paths.get(p).toFile().getName())
        .sorted().collect(Collectors.toList());
    assertEquals(2, fileNames2ndRead.size());
    assertTrue(fileNames2ndRead.get(0).startsWith("foo4"));
    assertTrue(fileNames2ndRead.get(1).startsWith("foo5"));
    String checkpointStr2ndRead = nextFilePathsAndCheckpoint.getRight().getCheckpointKey();
    assertEquals(2000L, Long.parseLong(checkpointStr2ndRead), "should read up to foo5 (inclusive)");
  }
}
