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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector;
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.translator.DecodedBootstrapPartitionPathTranslator;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.apache.hudi.config.HoodieBootstrapConfig.DATA_QUERIES_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests different layouts for bootstrap base path
 */
@Tag("functional")
public class TestBootstrapRead extends HoodieSparkClientTestBase {

  @TempDir
  public java.nio.file.Path tmpFolder;
  protected String bootstrapBasePath = null;
  protected String bootstrapTargetPath = null;
  protected String hudiBasePath = null;

  protected static int nInserts = 100;
  protected static int nUpdates = 20;
  protected static String[] dashPartitionPaths = {"2016-03-15", "2015-03-16", "2015-03-17"};
  protected static String[] slashPartitionPaths = {"2016/03/15", "2015/03/16", "2015/03/17"};
  protected String bootstrapType;
  protected Boolean dashPartitions;
  protected String tableType;
  protected Integer nPartitions;

  protected String[] partitionCols;
  protected static String[] dropColumns = {"_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",  "_hoodie_file_name", "city_to_state"};

  @BeforeEach
  public void setUp() throws Exception {
    bootstrapBasePath = tmpFolder.toAbsolutePath() + "/bootstrapBasePath";
    hudiBasePath = tmpFolder.toAbsolutePath() + "/hudiBasePath";
    bootstrapTargetPath = tmpFolder.toAbsolutePath() + "/bootstrapTargetPath";
    initSparkContexts();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupClients();
    cleanupTestDataGenerator();
  }

  private static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    String[] bootstrapType = {"full", "metadata", "mixed"};
    Boolean[] dashPartitions = {true,false};
    String[] tableType = {"COPY_ON_WRITE", "MERGE_ON_READ"};
    Integer[] nPartitions = {0, 1, 2};
    for (String tt : tableType) {
      for (Boolean dash : dashPartitions) {
        for (String bt : bootstrapType) {
          for (Integer n : nPartitions) {
            //can't be mixed bootstrap if it's nonpartitioned
            //don't need to test slash partitions if it's nonpartitioned
            if ((!bt.equals("mixed") && dash) || n > 0) {
              b.add(Arguments.of(bt, dash, tt, n));
            }
          }
        }
      }
    }
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void runTests(String bootstrapType,Boolean dashPartitions, String tableType, Integer nPartitions) {
    this.bootstrapType = bootstrapType;
    this.dashPartitions = dashPartitions;
    this.tableType = tableType;
    this.nPartitions = nPartitions;
    setupDirs();

    //do bootstrap
    Map<String, String> options = setBootstrapOptions();
    Dataset<Row> bootstrapDf = sparkSession.emptyDataFrame();
    bootstrapDf.write().format("hudi")
        .options(options)
        .mode(SaveMode.Overwrite)
        .save(bootstrapTargetPath);
    compareTables();

    //do upserts
    options = basicOptions();
    doUpdate(options, "001");
    compareTables();

    doUpdate(options, "002");
    compareTables();
  }

  private Map<String, String> basicOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType);
    options.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    if (nPartitions == 0) {
      options.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), NonpartitionedKeyGenerator.class.getName());
    } else {
      options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), String.join(",", partitionCols));
      if (nPartitions == 1) {
        options.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), SimpleKeyGenerator.class.getName());
      } else {
        options.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), ComplexKeyGenerator.class.getName());
      }
    }
    options.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    if (tableType.equals("MERGE_ON_READ")) {
      options.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
    }
    options.put(HoodieWriteConfig.TBL_NAME.key(), "test");
    return options;
  }

  private Map<String, String> setBootstrapOptions() {
    Map<String, String> options = basicOptions();
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL());
    options.put(HoodieBootstrapConfig.BASE_PATH.key(), bootstrapBasePath);
    if (!dashPartitions) {
      options.put(HoodieBootstrapConfig.PARTITION_PATH_TRANSLATOR_CLASS_NAME.key(), DecodedBootstrapPartitionPathTranslator.class.getName());
    }
    switch (bootstrapType) {
      case "metadata":
        options.put(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key(), MetadataOnlyBootstrapModeSelector.class.getName());
        break;
      case "full":
        options.put(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key(), FullRecordBootstrapModeSelector.class.getName());
        break;
      case "mixed":
        options.put(HoodieBootstrapConfig.MODE_SELECTOR_CLASS_NAME.key(), BootstrapRegexModeSelector.class.getName());
        String regexPattern;
        if (dashPartitions) {
          regexPattern = "partition_path=2015-03-1[5-7]";
        } else {
          regexPattern = "partition_path=2015%2F03%2F1[5-7]";
        }
        if (nPartitions > 1) {
          regexPattern = regexPattern + "\\/.*";
        }
        options.put(HoodieBootstrapConfig.PARTITION_SELECTOR_REGEX_PATTERN.key(), regexPattern);
        break;
      default:
        throw new RuntimeException();
    }
    return options;
  }

  protected void doUpdate(Map<String,String> options, String instantTime) {
    Dataset<Row> updates = generateTestUpdates(instantTime);
    String nCompactCommits = "3";
    updates.write().format("hudi")
        .options(options)
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), nCompactCommits)
        .mode(SaveMode.Append)
        .save(hudiBasePath);
    if (bootstrapType.equals("mixed")) {
      //mixed tables have a commit for each of the metadata and full bootstrap modes
      //so to align with the regular hudi table, we need to compact after 4 commits instead of 3
      nCompactCommits = "4";
    }
    updates.write().format("hudi")
        .options(options)
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), nCompactCommits)
        .mode(SaveMode.Append)
        .save(bootstrapTargetPath);
  }

  protected void compareTables() {
    Map<String,String> readOpts = new HashMap<>();
    if (tableType.equals("MERGE_ON_READ")) {
      //Bootstrap MOR currently only has read optimized queries implemented
      readOpts.put(DataSourceReadOptions.QUERY_TYPE().key(),DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL());
    }
    Dataset<Row> hudiDf = sparkSession.read().options(readOpts).format("hudi").load(hudiBasePath);
    Dataset<Row> bootstrapDf = sparkSession.read().format("hudi").load(bootstrapTargetPath);
    Dataset<Row> fastBootstrapDf = sparkSession.read().format("hudi").option(DATA_QUERIES_ONLY.key(), "true").load(bootstrapTargetPath);
    if (nPartitions == 0) {
      compareDf(fastBootstrapDf.drop("city_to_state"), bootstrapDf.drop(dropColumns).drop("_hoodie_partition_path"));
      compareDf(hudiDf.drop(dropColumns), bootstrapDf.drop(dropColumns));
      return;
    }
    compareDf(hudiDf.drop(dropColumns).drop(partitionCols), bootstrapDf.drop(dropColumns).drop(partitionCols));
    compareDf(fastBootstrapDf.drop("city_to_state").drop(partitionCols), bootstrapDf.drop(dropColumns).drop("_hoodie_partition_path").drop(partitionCols));
    compareDf(hudiDf.select("_row_key",partitionCols), bootstrapDf.select("_row_key",partitionCols));
    compareDf(fastBootstrapDf.select("_row_key",partitionCols), bootstrapDf.select("_row_key",partitionCols));
  }

  protected void compareDf(Dataset<Row> df1, Dataset<Row> df2) {
    assertEquals(0, df1.except(df2).count());
    assertEquals(0, df2.except(df1).count());
  }

  protected void setupDirs()  {
    dataGen = new HoodieTestDataGenerator(dashPartitions ? dashPartitionPaths : slashPartitionPaths);
    Dataset<Row> inserts = generateTestInserts();
    if (nPartitions > 0) {
      partitionCols = new String[nPartitions];
      partitionCols[0] = "partition_path";
      for (int i = 1; i < partitionCols.length; i++) {
        partitionCols[i] = "partpath" + (i + 1);
      }
      inserts.write().partitionBy(partitionCols).save(bootstrapBasePath);
    } else {
      inserts.write().save(bootstrapBasePath);
    }

    inserts.write().format("hudi")
        .options(basicOptions())
        .mode(SaveMode.Overwrite)
        .save(hudiBasePath);
  }

  public Dataset<Row> generateTestInserts() {
    List<String> records = dataGen.generateInserts("000", nInserts).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return addPartitionColumns(sparkSession.read().json(rdd), nPartitions);
  }

  public Dataset<Row> generateTestUpdates(String instantTime) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, nUpdates).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      return addPartitionColumns(sparkSession.read().json(rdd), nPartitions);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Dataset<Row> addPartitionColumns(Dataset<Row> df, Integer nPartitions) {
    if (nPartitions < 2) {
      return df;
    }
    for (int i = 2; i <= nPartitions; i++) {
      df = applyPartition(df, i);
    }
    return df;
  }

  private static Dataset<Row> applyPartition(Dataset<Row> df, Integer n) {
    return df.withColumn("partpath" + n,
        functions.md5(functions.concat_ws("," + n + ",",
            df.col("partition_path"),
            functions.hash(df.col("_row_key")).mod(n))));
  }
}
