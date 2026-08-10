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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector;
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.translator.DecodedBootstrapPartitionPathTranslator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordToString;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class TestBootstrapReadBase extends HoodieSparkClientTestBase {

  @TempDir
  public java.nio.file.Path tmpFolder;
  protected String bootstrapBasePath = null;
  protected String bootstrapTargetPath = null;
  protected String hudiBasePath = null;

  protected int nInserts = 100;
  protected int nUpdates = 20;
  protected static String[] dashPartitionPaths = {"2016-03-14","2016-03-15", "2015-03-16", "2015-03-17"};
  protected static String[] slashPartitionPaths = {"2016/03/15", "2015/03/16", "2015/03/17"};
  protected String bootstrapType;
  protected Boolean dashPartitions;
  protected HoodieTableType tableType;
  protected Integer nPartitions;

  protected String[] partitionCols;
  protected static String[] dropColumns = {"_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",  "_hoodie_file_name", "city_to_state"};

  @BeforeEach
  public void setUp() throws Exception {
    String randomUuid = UUID.randomUUID().toString();
    bootstrapBasePath = tmpFolder.toAbsolutePath() + "/" + randomUuid + "/bootstrapBasePath";
    hudiBasePath = tmpFolder.toAbsolutePath() + "/" + randomUuid + "/hudiBasePath";
    bootstrapTargetPath = tmpFolder.toAbsolutePath() + "/" + randomUuid + "/bootstrapTargetPath";
    initSparkContexts(this.getClass().getSimpleName() + randomUuid);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupClients();
    cleanupTestDataGenerator();
  }

  protected Map<String, String> basicOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceWriteOptions.TABLE_TYPE().key(), tableType.name());
    options.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
    options.put(HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key(), "10");
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
    options.put(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp");
    if (tableType.equals(MERGE_ON_READ)) {
      options.put(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
    }
    options.put(HoodieWriteConfig.TBL_NAME.key(), "test");
    return options;
  }

  protected Map<String, String> setBootstrapOptions() {
    Map<String, String> options = basicOptions();
    options.put(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL());
    options.put(HoodieBootstrapConfig.BASE_PATH.key(), bootstrapBasePath);
    options.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "false");
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
    Dataset<Row> updates = generateTestUpdates(instantTime, nUpdates);
    doUpsert(options, updates);
  }

  protected void doInsert(Map<String,String> options, String instantTime) {
    Dataset<Row> inserts = generateTestInserts(instantTime, nUpdates);
    doUpsert(options, inserts);
  }

  protected void doDelete(Map<String,String> options, String instantTime) {
    Dataset<Row> deletes = generateTestDeletes(instantTime, nUpdates);
    doUpsert(options, deletes);
  }

  protected void doUpsert(Map<String,String> options, Dataset<Row> df) {
    String nCompactCommits = "3";
    df.write().format("hudi")
        .options(options)
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), nCompactCommits)
        .mode(SaveMode.Append)
        .save(hudiBasePath);
    if (bootstrapType.equals("mixed")) {
      // mixed tables have a commit for each of the metadata and full bootstrap modes
      // so to align with the regular hudi table, we need to compact after 4 commits instead of 3
      nCompactCommits = "4";
    }
    df.write().format("hudi")
        .options(options)
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), nCompactCommits)
        .mode(SaveMode.Append)
        .save(bootstrapTargetPath);
  }

  protected void compareTables() {
    Dataset<Row> hudiDf = sparkSession.read().format("hudi").load(hudiBasePath);
    Dataset<Row> bootstrapDf = sparkSession.read().format("hudi").load(bootstrapTargetPath);
    if (nPartitions == 0) {
      compareDf(hudiDf.drop(dropColumns), bootstrapDf.drop(dropColumns));
      return;
    }
    compareDf(hudiDf.drop(dropColumns).drop(partitionCols), bootstrapDf.drop(dropColumns).drop(partitionCols));
    compareDf(hudiDf.select("_row_key",partitionCols), bootstrapDf.select("_row_key",partitionCols));
  }

  protected void verifyMetaColOnlyRead(Integer iteration) {
    Dataset<Row> hudiDf = sparkSession.read().format("hudi").load(hudiBasePath).select("_hoodie_commit_time", "_hoodie_record_key");
    Dataset<Row> bootstrapDf = sparkSession.read().format("hudi").load(bootstrapTargetPath).select("_hoodie_commit_time", "_hoodie_record_key");
    hudiDf.show(100,false);
    bootstrapDf.show(100,false);
    if (iteration > 0) {
      assertEquals(sparkSession.sql("select * from hudi_iteration_" + (iteration - 1)).intersect(hudiDf).count(),
          sparkSession.sql("select * from bootstrap_iteration_" + (iteration - 1)).intersect(bootstrapDf).count());
    }
    hudiDf.createOrReplaceTempView("hudi_iteration_" + iteration);
    bootstrapDf.createOrReplaceTempView("bootstrap_iteration_" + iteration);
  }

  protected void compareDf(Dataset<Row> df1, Dataset<Row> df2) {
    Dataset<Row> difference1 = df1.except(df2);
    long diff1Count = difference1.count();
    if (diff1Count > 0) {
      difference1.show(100, false);
    }
    Dataset<Row> difference2 = df2.except(df1);
    long diff2Count = difference2.count();
    if (diff2Count > 0) {
      difference2.show(100, false);
    }
    assertEquals(0, diff1Count);
    assertEquals(0, diff2Count);
  }

  protected void setupDirs()  {
    dataGen = new HoodieTestDataGenerator(dashPartitions ? dashPartitionPaths : slashPartitionPaths);
    Dataset<Row> inserts = generateTestInserts("000", nInserts);
    if (dashPartitions) {
      //test adding a partition to the table
      inserts = inserts.filter("partition_path != '2016-03-14'");
    }
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

  protected Dataset<Row> makeDeleteDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateUniqueDeleteRecords(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> generateTestDeletes(String instantTime, Integer n) {
    return addPartitionColumns(makeDeleteDf(instantTime, n), nPartitions);
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> generateTestInserts(String instantTime, Integer n) {
    return addPartitionColumns(makeInsertDf(instantTime, n), nPartitions);
  }

  protected Dataset<Row> makeUpdateDf(String instantTime, Integer n) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, n).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      return sparkSession.read().json(rdd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Dataset<Row> generateTestUpdates(String instantTime, Integer n) {
    return addPartitionColumns(makeUpdateDf(instantTime, n), nPartitions);
  }

  protected static Dataset<Row> addPartitionColumns(Dataset<Row> df, Integer nPartitions) {
    if (nPartitions < 2) {
      return df;
    }
    for (int i = 2; i <= nPartitions; i++) {
      df = applyPartition(df, i);
    }
    return df;
  }

  protected static Dataset<Row> applyPartition(Dataset<Row> df, Integer n) {
    return df.withColumn("partpath" + n,
        functions.md5(functions.concat_ws("," + n + ",",
            df.col("partition_path"),
            functions.hash(df.col("_row_key")).mod(n))));
  }
}
