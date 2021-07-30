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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.utilities.sources.helpers.DFSPathSelector.Config.ROOT_INPUT_PATH_PROP;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.CURRENT_DATE;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DATE_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.DATE_PARTITION_DEPTH;
import static org.apache.hudi.utilities.sources.helpers.DatePartitionPathSelector.Config.LOOKBACK_DAYS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDatePartitionPathSelector extends HoodieClientTestHarness {

  private transient HoodieSparkEngineContext context = null;
  static List<LocalDate> totalDates;

  @BeforeAll
  public static void initClass() {
    String s = "2020-07-21";
    String e = "2020-07-25";
    LocalDate start = LocalDate.parse(s);
    LocalDate end = LocalDate.parse(e);
    totalDates = new ArrayList<>();
    while (!start.isAfter(end)) {
      totalDates.add(start);
      start = start.plusDays(1);
    }
  }

  @BeforeEach
  public void setup() {
    initSparkContexts();
    initPath();
    initFileSystem();
    context = new HoodieSparkEngineContext(jsc);
  }

  @AfterEach
  public void teardown() throws Exception {
    cleanupResources();
  }

  /*
   * Create Date partitions with some files under each of the leaf Dirs.
   */
  public List<Path> createDatePartitionsWithFiles(List<Path> leafDirs, boolean hiveStyle, String dateFormat)
      throws IOException {
    List<Path> allFiles = new ArrayList<>();
    for (Path path : leafDirs) {
      List<Path> datePartitions = generateDatePartitionsUnder(path, hiveStyle, dateFormat);
      for (Path datePartition : datePartitions) {
        allFiles.addAll(createRandomFilesUnder(datePartition));
      }
    }
    return allFiles;
  }

  /**
   * Create all parent level dirs before the date partitions.
   *
   * @param root Current parent dir. Initially this points to table basepath.
   * @param dirs List o sub dirs to be created under root.
   * @param depth Depth of partitions before date partitions.
   * @param leafDirs Collect list of leaf dirs. These will be the immediate parents of date based partitions.
   * @throws IOException
   */
  public void createParentDirsBeforeDatePartitions(Path root, List<String> dirs, int depth, List<Path> leafDirs)
      throws IOException {
    if (depth <= 0) {
      leafDirs.add(root);
      return;
    }
    for (String s : dirs) {
      Path subdir = new Path(root, s);
      fs.mkdirs(subdir);
      createParentDirsBeforeDatePartitions(subdir, generateRandomStrings(), depth - 1, leafDirs);
    }
  }

  /*
   * Random string generation util used for generating file names or file contents.
   */
  private List<String> generateRandomStrings() {
    List<String> subDirs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      subDirs.add(UUID.randomUUID().toString());
    }
    return subDirs;
  }

  /*
   * Generate date based partitions under a parent dir with or without hivestyle formatting.
   */
  private List<Path> generateDatePartitionsUnder(Path parent, boolean hiveStyle, String dateFormat) throws IOException {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
    List<Path> datePartitions = new ArrayList<>();
    String prefix = (hiveStyle ? "dt=" : "");
    for (int i = 0; i < 5; i++) {
      Path child = new Path(parent, prefix + formatter.format(totalDates.get(i)));
      fs.mkdirs(child);
      datePartitions.add(child);
    }
    return datePartitions;
  }

  /*
   * Creates random files under the given directory.
   */
  private List<Path> createRandomFilesUnder(Path path) throws IOException {
    List<Path> resultFiles = new ArrayList<>();
    List<String> fileNames = generateRandomStrings();
    for (String fileName : fileNames) {
      List<String> fileContent = generateRandomStrings();
      String[] lines = new String[fileContent.size()];
      lines = fileContent.toArray(lines);
      Path file = new Path(path, fileName);
      UtilitiesTestBase.Helpers.saveStringsToDFS(lines, fs, file.toString());
      resultFiles.add(file);
    }
    return resultFiles;
  }

  private static TypedProperties getProps(
      String basePath, String dateFormat, int datePartitionDepth, int numDaysToList, String currentDate) {
    TypedProperties properties = new TypedProperties();
    properties.put(ROOT_INPUT_PATH_PROP, basePath);
    properties.put(DATE_FORMAT, dateFormat);
    properties.put(DATE_PARTITION_DEPTH, "" + datePartitionDepth);
    properties.put(LOOKBACK_DAYS, "" + numDaysToList);
    properties.put(CURRENT_DATE, currentDate);
    return properties;
  }

  /*
   * Return test params => (table basepath, date partition's depth,
   * num of prev days to list, current date, is date partition formatted in hive style?,
   * expected number of paths after pruning)
   */
  private static Stream<Arguments> configParams() {
    Object[][] data =
        new Object[][] {
          {"table1", "yyyyMMdd", 0, 2, "2020-07-25", true, 1},
          {"table2", "yyyyMMdd", 0, 2, "2020-07-25", false, 1},
          {"table3", "yyyyMMMdd", 1, 3, "2020-07-25", true, 4},
          {"table4", "yyyyMMMdd", 1, 3, "2020-07-25", false, 4},
          {"table5", "yyyy-MM-dd", 2, 1, "2020-07-25", true, 10},
          {"table6", "yyyy-MM-dd", 2, 1, "2020-07-25", false, 10},
          {"table7", "yyyy-MMM-dd", 3, 2, "2020-07-25", true, 75},
          {"table8", "yyyy-MMM-dd", 3, 2, "2020-07-25", false, 75}
        };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("configParams")
  public void testPruneDatePartitionPaths(
      String tableName,
      String dateFormat,
      int datePartitionDepth,
      int numPrevDaysToList,
      String currentDate,
      boolean isHiveStylePartition,
      int expectedNumFiles)
      throws IOException {
    TypedProperties props = getProps(basePath + "/" + tableName, dateFormat, datePartitionDepth, numPrevDaysToList, currentDate);
    DatePartitionPathSelector pathSelector = new DatePartitionPathSelector(props, jsc.hadoopConfiguration());

    Path root = new Path(props.getString(ROOT_INPUT_PATH_PROP));
    int totalDepthBeforeDatePartitions = props.getInteger(DATE_PARTITION_DEPTH) - 1;

    // Create parent dir
    List<Path> leafDirs = new ArrayList<>();
    createParentDirsBeforeDatePartitions(root, generateRandomStrings(), totalDepthBeforeDatePartitions, leafDirs);
    createDatePartitionsWithFiles(leafDirs, isHiveStylePartition, dateFormat);

    List<String> paths = pathSelector.pruneDatePartitionPaths(context, fs, root.toString(), LocalDate.parse(currentDate));
    assertEquals(expectedNumFiles, paths.size());
  }
}
