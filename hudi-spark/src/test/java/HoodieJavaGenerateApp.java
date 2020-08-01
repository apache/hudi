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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;

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

  private static final Logger LOG = LogManager.getLogger(HoodieJavaGenerateApp.class);

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
    // Spark session setup..
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
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
      LOG.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), hiveTable)
          .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), hiveDB)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hiveJdbcUrl)
          .option(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hiveUser)
          .option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hivePass)
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
      if (nonPartitionedTable) {
        writer = writer
            .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
                NonPartitionedExtractor.class.getCanonicalName())
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "");
      } else if (useMultiPartitionKeys) {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "year,month,day").option(
            DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            MultiPartKeysValueExtractor.class.getCanonicalName());
      } else {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "dateStr");
      }
    }
    return writer;
  }

  private void insert(SparkSession spark) throws IOException {
    HoodieTestDataGenerator dataGen = getDataGenerate();

    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());

    // Generate some input..
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
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
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
        // insert
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL())
        // This is the record key
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        // this is the partition to place it into
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        // use to combine duplicate records in input/with disk val
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        // Used by hive sync and queries
        .option(HoodieWriteConfig.TABLE_NAME, tableName)
        // Add Key Extractor
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
            nonPartitionedTable ? NonpartitionedKeyGenerator.class.getCanonicalName()
                : SimpleKeyGenerator.class.getCanonicalName())
        .mode(commitType);

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    LOG.info("Commit at instant time :" + commitInstantTime1);
  }
}
