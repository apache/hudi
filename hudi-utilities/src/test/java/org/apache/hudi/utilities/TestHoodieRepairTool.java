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

package org.apache.hudi.utilities;

import org.apache.hudi.HoodieTestCommitGenerator;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.testutils.providers.SparkProvider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.HoodieTestCommitGenerator.getBaseFilename;
import static org.apache.hudi.HoodieTestCommitGenerator.getLogFilename;
import static org.apache.hudi.HoodieTestCommitGenerator.initCommitInfoForRepairTests;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieRepairTool implements SparkProvider {
  @TempDir
  public static java.nio.file.Path tempDir;
  // Instant time -> List<Pair<relativePartitionPath, fileId>>
  private static final Map<String, List<Pair<String, String>>> BASE_FILE_INFO = new HashMap<>();
  private static final Map<String, List<Pair<String, String>>> LOG_FILE_INFO = new HashMap<>();
  // instant time -> partitionPathToFileIdAndNameMap
  private static final Map<String, Map<String, List<Pair<String, String>>>> INSTANT_INFO_MAP = new HashMap<>();
  private static final List<String> ALL_FILE_ABSOLUTE_PATH_LIST = new ArrayList<>();
  // Relative paths to base path
  private static final List<String> DANGLING_DATA_FILE_LIST = new ArrayList<>();
  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient HoodieSparkEngineContext context;
  private static java.nio.file.Path backupTempDir;
  private static String basePath;
  private static HoodieTableMetaClient metaClient;
  private boolean initialized = false;

  @BeforeAll
  static void initFileInfo() throws IOException {
    initCommitInfoForRepairTests(BASE_FILE_INFO, LOG_FILE_INFO);
    metaClient =
        HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), HoodieTableType.COPY_ON_WRITE);
    basePath = metaClient.getBasePath();
    SecureRandom random = new SecureRandom(
        Long.valueOf(System.currentTimeMillis()).toString().getBytes(StandardCharsets.UTF_8));
    long randomLong = random.nextLong();
    backupTempDir = Paths.get("/tmp/test_backup_" + randomLong);
    HoodieTestCommitGenerator.setupTimelineInFS(
        basePath, BASE_FILE_INFO, LOG_FILE_INFO, INSTANT_INFO_MAP);
    ALL_FILE_ABSOLUTE_PATH_LIST.addAll(INSTANT_INFO_MAP.entrySet().stream()
        .flatMap(e -> e.getValue().entrySet().stream()
            .flatMap(partition -> partition.getValue().stream()
                .map(fileInfo -> new Path(new Path(basePath, partition.getKey()), fileInfo.getValue()).toString())
                .collect(Collectors.toList())
                .stream())
            .collect(Collectors.toList())
            .stream()
        )
        .collect(Collectors.toList()));
    setupDanglingDataFileList();
  }

  @BeforeEach
  public void initWithCleanState() throws IOException {
    cleanUp();
    initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      SparkRDDWriteClient.registerClasses(sparkConf);
      HoodieReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
    }
  }

  @AfterEach
  public void cleanUp() throws IOException {
    cleanUpDanglingDataFilesInFS();
    cleanUpBackupTempDir();
  }

  @AfterAll
  public static synchronized void resetSpark() {
    if (spark != null) {
      spark.close();
      spark = null;
    }
  }

  private static void cleanUpDanglingDataFilesInFS() {
    FileSystem fs = metaClient.getFs();
    DANGLING_DATA_FILE_LIST.forEach(
        relativeFilePath -> {
          Path path = new Path(basePath, relativeFilePath);
          try {
            if (fs.exists(path)) {
              fs.delete(path, false);
            }
          } catch (IOException e) {
            throw new HoodieIOException("Unable to delete file: " + path);
          }
        }
    );
  }

  private static void cleanUpBackupTempDir() throws IOException {
    FileSystem fs = metaClient.getFs();
    fs.delete(new Path(backupTempDir.toAbsolutePath().toString()), true);
  }

  private static void setupDanglingDataFileList() {
    DANGLING_DATA_FILE_LIST.add(
        new Path("2022/01/01",
            getBaseFilename("000", UUID.randomUUID().toString())).toString());
    DANGLING_DATA_FILE_LIST.add(
        new Path("2022/01/06",
            getLogFilename("001", UUID.randomUUID().toString())).toString());
  }

  public static Stream<Arguments> configPathParams() {
    Object[][] data = new Object[][] {
        {null, basePath, -1}, {basePath + "/backup", basePath, -1},
        {"/tmp/backup", basePath, 0}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("configPathParams")
  public void testCheckBackupPathAgainstBasePath(
      String backupPath, String basePath, int expectedResult) {
    HoodieRepairTool.Config config = new HoodieRepairTool.Config();
    config.backupPath = backupPath;
    config.basePath = basePath;
    HoodieRepairTool tool = new HoodieRepairTool(jsc, config);
    assertEquals(expectedResult, tool.checkBackupPathAgainstBasePath());
  }

  public static Stream<Arguments> configPathParamsWithFS() throws IOException {
    SecureRandom random = new SecureRandom();
    long randomLong = random.nextLong();
    String emptyBackupPath = "/tmp/empty_backup_" + randomLong;
    FSUtils.createPathIfNotExists(metaClient.getFs(), new Path(emptyBackupPath));
    String nonEmptyBackupPath = "/tmp/nonempty_backup_" + randomLong;
    FSUtils.createPathIfNotExists(metaClient.getFs(), new Path(nonEmptyBackupPath));
    FSUtils.createPathIfNotExists(metaClient.getFs(), new Path(nonEmptyBackupPath, ".hoodie"));
    Object[][] data = new Object[][] {
        {null, basePath, 0}, {"/tmp/backup", basePath, 0},
        {emptyBackupPath, basePath, 0}, {basePath + "/backup", basePath, -1},
        {nonEmptyBackupPath, basePath, -1},
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("configPathParamsWithFS")
  public void testCheckBackupPathForRepair(
      String backupPath, String basePath, int expectedResult) throws IOException {
    HoodieRepairTool.Config config = new HoodieRepairTool.Config();
    config.backupPath = backupPath;
    config.basePath = basePath;
    HoodieRepairTool tool = new HoodieRepairTool(jsc, config);
    assertEquals(expectedResult, tool.checkBackupPathForRepair());
    if (backupPath == null) {
      // Backup path should be created if not provided
      assertNotNull(config.backupPath);
    }
  }

  @Test
  public void testRepairWithIntactInstants() throws IOException {

    testRepairToolWithMode(
        Option.empty(), Option.empty(), HoodieRepairTool.Mode.REPAIR.toString(),
        backupTempDir.toAbsolutePath().toString(), true,
        ALL_FILE_ABSOLUTE_PATH_LIST, Collections.emptyList());
  }

  @Test
  public void testRepairWithBrokenInstants() throws IOException {
    List<String> tableDanglingFileList = createDanglingDataFilesInFS(basePath);
    String backupPath = backupTempDir.toAbsolutePath().toString();
    List<String> backupDanglingFileList = DANGLING_DATA_FILE_LIST.stream()
        .map(filePath -> new Path(backupPath, filePath).toString())
        .collect(Collectors.toList());
    List<String> existingFileList = new ArrayList<>(ALL_FILE_ABSOLUTE_PATH_LIST);
    existingFileList.addAll(backupDanglingFileList);

    testRepairToolWithMode(
        Option.empty(), Option.empty(), HoodieRepairTool.Mode.REPAIR.toString(),
        backupPath, true,
        existingFileList, tableDanglingFileList);
  }

  @Test
  public void testRepairWithOneBrokenInstant() throws IOException {
    List<String> tableDanglingFileList = createDanglingDataFilesInFS(basePath);
    String backupPath = backupTempDir.toAbsolutePath().toString();
    List<String> backupDanglingFileList = DANGLING_DATA_FILE_LIST
        .subList(1, 2).stream()
        .map(filePath -> new Path(backupPath, filePath).toString())
        .collect(Collectors.toList());
    List<String> existingFileList = new ArrayList<>(ALL_FILE_ABSOLUTE_PATH_LIST);
    existingFileList.addAll(backupDanglingFileList);
    existingFileList.addAll(tableDanglingFileList.subList(0, 1));

    testRepairToolWithMode(
        Option.of("001"), Option.empty(), HoodieRepairTool.Mode.REPAIR.toString(),
        backupPath, true,
        existingFileList, tableDanglingFileList.subList(1, 2));
  }

  @Test
  public void testDryRunWithBrokenInstants() throws IOException {
    List<String> tableDanglingFileList = createDanglingDataFilesInFS(basePath);
    String backupPath = backupTempDir.toAbsolutePath().toString();
    List<String> backupDanglingFileList = DANGLING_DATA_FILE_LIST.stream()
        .map(filePath -> new Path(backupPath, filePath).toString())
        .collect(Collectors.toList());
    List<String> existingFileList = new ArrayList<>(ALL_FILE_ABSOLUTE_PATH_LIST);
    existingFileList.addAll(tableDanglingFileList);

    testRepairToolWithMode(
        Option.empty(), Option.empty(), HoodieRepairTool.Mode.DRY_RUN.toString(),
        backupPath, true,
        existingFileList, backupDanglingFileList);
  }

  @Test
  public void testDryRunWithOneBrokenInstant() throws IOException {
    List<String> tableDanglingFileList = createDanglingDataFilesInFS(basePath);
    String backupPath = backupTempDir.toAbsolutePath().toString();
    List<String> backupDanglingFileList = DANGLING_DATA_FILE_LIST.stream()
        .map(filePath -> new Path(backupPath, filePath).toString())
        .collect(Collectors.toList());
    List<String> existingFileList = new ArrayList<>(ALL_FILE_ABSOLUTE_PATH_LIST);
    existingFileList.addAll(tableDanglingFileList);

    testRepairToolWithMode(
        Option.of("001"), Option.empty(), HoodieRepairTool.Mode.DRY_RUN.toString(),
        backupPath, true,
        existingFileList, backupDanglingFileList);
  }

  @Test
  public void testUndoWithNonExistentBackupPath() throws IOException {
    String backupPath = backupTempDir.toAbsolutePath().toString();
    metaClient.getFs().delete(new Path(backupPath), true);

    testRepairToolWithMode(
        Option.empty(), Option.empty(), HoodieRepairTool.Mode.UNDO.toString(),
        backupPath, false,
        ALL_FILE_ABSOLUTE_PATH_LIST, Collections.emptyList());
  }

  @Test
  public void testUndoWithExistingBackupPath() throws IOException {
    String backupPath = backupTempDir.toAbsolutePath().toString();
    List<String> backupDanglingFileList = createDanglingDataFilesInFS(backupPath);
    List<String> restoreDanglingFileList = DANGLING_DATA_FILE_LIST.stream()
        .map(filePath -> new Path(basePath, filePath).toString())
        .collect(Collectors.toList());
    List<String> existingFileList = new ArrayList<>(ALL_FILE_ABSOLUTE_PATH_LIST);
    existingFileList.addAll(backupDanglingFileList);
    existingFileList.addAll(restoreDanglingFileList);

    testRepairToolWithMode(
        Option.empty(), Option.empty(), HoodieRepairTool.Mode.UNDO.toString(),
        backupPath, true,
        existingFileList, Collections.emptyList());
    // Second run should fail
    testRepairToolWithMode(
        Option.empty(), Option.empty(), HoodieRepairTool.Mode.UNDO.toString(),
        backupPath, false,
        existingFileList, Collections.emptyList());
  }

  private void testRepairToolWithMode(
      Option<String> startingInstantOption, Option<String> endingInstantOption,
      String runningMode, String backupPath, boolean isRunSuccessful,
      List<String> existFilePathList, List<String> nonExistFilePathList) throws IOException {
    HoodieRepairTool.Config config = new HoodieRepairTool.Config();
    config.backupPath = backupPath;
    config.basePath = basePath;
    config.assumeDatePartitioning = true;
    if (startingInstantOption.isPresent()) {
      config.startingInstantTime = startingInstantOption.get();
    }
    if (endingInstantOption.isPresent()) {
      config.endingInstantTime = endingInstantOption.get();
    }
    config.runningMode = runningMode;
    HoodieRepairTool tool = new HoodieRepairTool(jsc, config);
    assertEquals(isRunSuccessful, tool.run());
    verifyFilesInFS(existFilePathList, nonExistFilePathList);
  }

  private void verifyFilesInFS(
      List<String> existFilePathList, List<String> nonExistFilePathList) throws IOException {
    FileSystem fs = metaClient.getFs();

    for (String filePath : existFilePathList) {
      assertTrue(fs.exists(new Path(filePath)),
          String.format("File %s should exist but it's not in the file system", filePath));
    }

    for (String filePath : nonExistFilePathList) {
      assertFalse(fs.exists(new Path(filePath)),
          String.format("File %s should not exist but it's in the file system", filePath));
    }
  }

  private List<String> createDanglingDataFilesInFS(String parentPath) {
    FileSystem fs = metaClient.getFs();
    return DANGLING_DATA_FILE_LIST.stream().map(relativeFilePath -> {
      Path path = new Path(parentPath, relativeFilePath);
      try {
        fs.mkdirs(path.getParent());
        if (!fs.exists(path)) {
          fs.create(path, false);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return path.toString();
    })
        .collect(Collectors.toList());
  }

  @Override
  public HoodieEngineContext context() {
    return context;
  }

  @Override
  public SparkSession spark() {
    return spark;
  }

  @Override
  public SQLContext sqlContext() {
    return sqlContext;
  }

  @Override
  public JavaSparkContext jsc() {
    return jsc;
  }
}
