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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_ENABLED;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;

@Slf4j
public class HoodieJavaGenerateApp {
  @Parameter(names = {"--table-path", "-p"}, description = "Path for Hoodie sample table")
  private String tablePath = "file:///tmp/hoodie/sample-table";

  @Parameter(names = {"--table-name", "-n"}, description = "Table name for Hoodie sample table")
  private String tableName = "hoodie_test";

  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
  private String tableType = HoodieTableType.COPY_ON_WRITE.name();

  @Parameter(names = {"--hive-sync", "-hs"}, description = "Enable syncing to hive")
  private Boolean enableHiveSync = false;

  @Parameter(names = {"--hive-db", "-hd"}, description = "Hive database")
  private String hiveDB = "default";

  @Parameter(names = {"--hive-table", "-ht"}, description = "Hive table")
  private String hiveTable = "hoodie_sample_test";

  @Parameter(names = {"--hive-user", "-hu"}, description = "Hive username")
  private String hiveUser = "hive";

  @Parameter(names = {"--hive-password", "-hp"}, description = "Hive password")
  private String hivePass = "hive";

  @Parameter(names = {"--hive-url", "-hl"}, description = "Hive JDBC URL")
  private String hiveJdbcUrl = "jdbc:hive2://localhost:10000";

  @Parameter(names = {"--non-partitioned", "-np"}, description = "Use non-partitioned Table")
  private Boolean nonPartitionedTable = false;

  @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
  private Boolean useMultiPartitionKeys = false;

  @Parameter(names = {"--commit-type", "-ct"}, description = "How may commits will run")
  private String commitType = "overwrite";

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public static void main(String[] args) throws Exception {
    HoodieJavaGenerateApp cli = new HoodieJavaGenerateApp();
    JCommander cmd = new JCommander(cli, null, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    try (SparkSession spark = cli.getOrCreateSparkSession()) {
      cli.insert(spark);
    }
  }

  private SparkSession getOrCreateSparkSession() {
    SparkSession spark = SparkSession.builder()
        .config(getSparkConfForTest("Hoodie Spark APP"))
        .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");
    return spark;
  }

  private HoodieTestDataGenerator getDataGenerate() {
    // Generator of some records to be loaded in.
    if (nonPartitionedTable) {
      // All data goes to base-path
      return new HoodieTestDataGenerator(new String[]{""});
    } else {
      return new HoodieTestDataGenerator();
    }
  }

  /**
   * Setup configs for syncing to hive.
   */
  private DataFrameWriter<Row> updateHiveSyncConfig(DataFrameWriter<Row> writer) {
    if (enableHiveSync) {
      log.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(META_SYNC_TABLE_NAME.key(), hiveTable)
          .option(META_SYNC_DATABASE_NAME.key(), hiveDB)
          .option(HIVE_URL.key(), hiveJdbcUrl)
          .option(HIVE_USER.key(), hiveUser)
          .option(HIVE_PASS.key(), hivePass)
          .option(HIVE_SYNC_ENABLED.key(), "true");
      if (nonPartitionedTable) {
        writer = writer
            .option(HiveSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
                NonPartitionedExtractor.class.getCanonicalName())
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "");
      } else if (useMultiPartitionKeys) {
        writer = writer.option(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "year,month,day").option(
            HiveSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
            MultiPartKeysValueExtractor.class.getCanonicalName());
      } else {
        writer = writer.option(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "dateStr").option(
            HiveSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
            SlashEncodedDayPartitionValueExtractor.class.getCanonicalName());
      }
    }
    return writer;
  }

  private void insert(SparkSession spark) throws IOException {
    HoodieTestDataGenerator dataGen = getDataGenerate();

    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());

    // Generate some input..
    String instantTime = InProcessTimeGenerator.createNewInstantTime();
    List<HoodieRecord> recordsSoFar = new ArrayList<>(dataGen.generateInserts(instantTime/* ignore */, 100));
    List<String> records1 = recordsToStrings(recordsSoFar);
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    // Save as hoodie dataset (copy on write)
    // specify the hoodie source
    DataFrameWriter<Row> writer = inputDF1.write().format("org.apache.hudi")
        // any hoodie client config can be passed like this
        .option("hoodie.insert.shuffle.parallelism", "2")
        // full list in HoodieWriteConfig & its package
        .option("hoodie.upsert.shuffle.parallelism", "2")
        // Hoodie Table Type
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType)
        // insert
        .option(DataSourceWriteOptions.OPERATION().key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())
        // This is the record key
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key")
        // this is the partition to place it into
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition")
        // use to combine duplicate records in input/with disk val
        .option(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp")
        // Used by hive sync and queries
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .mode(commitType);

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    log.info("Commit at instant time :" + commitInstantTime1);
  }
}
