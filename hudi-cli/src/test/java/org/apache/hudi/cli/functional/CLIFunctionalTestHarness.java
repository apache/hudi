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

package org.apache.hudi.cli.functional;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.HoodieSparkKryoRegistrar$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CLIFunctionalTestHarness implements SparkProvider {

  protected static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();

  // Box drawing characters of a rendered table, kept as escapes so that the source stays ASCII.
  private static final char TABLE_ROW_START = '\u2551'; // double vertical, starts and ends a row
  private static final char TABLE_HEADER_DIVIDER = '\u2560'; // double vertical and right, under the header
  private static final char TABLE_ROW_DIVIDER = '\u255F'; // double vertical and single right, between rows
  private static final String TABLE_CELL_SEPARATORS = "[\u2551\u2502]"; // double and single vertical
  protected static final String EMPTY_TABLE_CELL = "(empty)";

  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  protected static transient TimelineService timelineService;
  protected static transient HoodieSparkEngineContext context;
  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
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

  @Override
  public HoodieSparkEngineContext context() {
    return context;
  }

  public String tableName() {
    return tableName("_test_table");
  }

  public String tableName(String suffix) {
    return getClass().getSimpleName() + suffix;
  }

  public String tablePath(String tableName) {
    return Paths.get(basePath(), tableName).toString();
  }

  public StorageConfiguration<Configuration> storageConf() {
    return HadoopFSUtils.getStorageConfWithCopy(jsc().hadoopConfiguration());
  }

  @BeforeEach
  public synchronized void runBeforeEach() {
    initialized = spark != null;
    if (!initialized) {
      SparkConf sparkConf = conf();
      HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
      SparkRDDReadClient.addHoodieSupport(sparkConf);
      spark = SparkSession.builder().config(sparkConf).getOrCreate();
      sqlContext = spark.sqlContext();
      jsc = new JavaSparkContext(spark.sparkContext());
      context = new HoodieSparkEngineContext(jsc);
      timelineService = HoodieClientTestUtils.initTimelineService(
          context, basePath(), incrementTimelineServicePortToUse());
      timelineServicePort = timelineService.getServerPort();
    }
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() throws IOException {
    if (spark != null) {
      spark.close();
      spark = null;
    }
    if (timelineService != null) {
      timelineService.close();
    }
  }

  /**
   * Helper to prepare string for matching.
   *
   * @param str Input string.
   * @return pruned string with non word characters removed.
   */
  protected static String removeNonWordAndStripSpace(String str) {
    return str.replaceAll("[\\s]+", ",").replaceAll("[\\W]+", ",");
  }

  /**
   * Splits a table rendered by {@link org.apache.hudi.cli.HoodiePrintHelper} into its data rows,
   * each row being the list of its trimmed cell values. Cells spanning several rendered lines are
   * joined back into a single cell, separated by a space. The header and an empty table yield no
   * rows.
   *
   * @param rendered Rendered table.
   * @return One list of cell values per data row.
   */
  protected static List<List<String>> renderedRows(String rendered) {
    List<List<String>> rows = new ArrayList<>();
    boolean inData = false;
    boolean startOfRow = false;
    for (String line : rendered.split("\n")) {
      if (line.isEmpty()) {
        continue;
      }
      char first = line.charAt(0);
      if (first == TABLE_HEADER_DIVIDER || first == TABLE_ROW_DIVIDER) {
        inData = true;
        startOfRow = true;
        continue;
      }
      if (first != TABLE_ROW_START || !inData) {
        continue;
      }
      List<String> cells = renderedCells(line);
      if (cells.size() == 1 && EMPTY_TABLE_CELL.equals(cells.get(0))) {
        continue;
      }
      if (startOfRow) {
        rows.add(cells);
        startOfRow = false;
      } else {
        List<String> previous = rows.get(rows.size() - 1);
        for (int i = 0; i < cells.size(); i++) {
          if (!cells.get(i).isEmpty()) {
            previous.set(i, (previous.get(i) + " " + cells.get(i)).trim());
          }
        }
      }
    }
    return rows;
  }

  /**
   * Splits a single rendered line into its trimmed cell values.
   *
   * @param line One line of a rendered table.
   * @return The cell values of that line.
   */
  protected static List<String> renderedCells(String line) {
    String[] parts = line.split(TABLE_CELL_SEPARATORS, -1);
    List<String> cells = new ArrayList<>();
    for (int i = 1; i < parts.length - 1; i++) {
      cells.add(parts[i].trim());
    }
    return cells;
  }

  protected int incrementTimelineServicePortToUse() {
    // Increment the timeline service port for each individual test
    // to avoid port reuse causing failures
    timelineServicePort = (timelineServicePort + 1 - 1024) % (65536 - 1024) + 1024;
    return timelineServicePort;
  }
}
