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

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings;
import static org.apache.hudi.common.testutils.Transformations.randomSelectAsHoodieKeys;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_ENABLED;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;

/**
 * Sample program that writes & reads hoodie tables via the Spark datasource.
 */
@Slf4j
public class HoodieJavaApp {

  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
  private String tablePath = "file:///tmp/hoodie/sample-table";

  @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
  private String tableName = "hoodie_test";

  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
  private String tableType = HoodieTableType.COPY_ON_WRITE.name();

  @Parameter(names = {"--hive-sync", "-hv"}, description = "Enable syncing to hive")
  private Boolean enableHiveSync = false;

  @Parameter(names = {"--hive-db", "-hd"}, description = "hive database")
  private String hiveDB = "default";

  @Parameter(names = {"--hive-table", "-ht"}, description = "hive table")
  private String hiveTable = "hoodie_sample_test";

  @Parameter(names = {"--hive-user", "-hu"}, description = "hive username")
  private String hiveUser = "hive";

  @Parameter(names = {"--hive-password", "-hp"}, description = "hive password")
  private String hivePass = "hive";

  @Parameter(names = {"--hive-url", "-hl"}, description = "hive JDBC URL")
  private String hiveJdbcUrl = "jdbc:hive2://localhost:10000";

  @Parameter(names = {"--non-partitioned", "-np"}, description = "Use non-partitioned Table")
  private Boolean nonPartitionedTable = false;

  @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
  private Boolean useMultiPartitionKeys = false;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public static void main(String[] args) throws Exception {
    HoodieJavaApp cli = new HoodieJavaApp();
    JCommander cmd = new JCommander(cli, null, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    cli.run();
  }

  public void run() throws Exception {
    SparkSession spark = SparkSession.builder()
        .config(getSparkConfForTest("Hoodie Spark APP"))
        .getOrCreate();

    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = null;
    if (nonPartitionedTable) {
      // All data goes to base-path
      dataGen = new HoodieTestDataGenerator(new String[]{""});
    } else {
      dataGen = new HoodieTestDataGenerator();
    }

    // Explicitly clear up the hoodie table path if it exists.
    fs.delete(new Path(tablePath), true);

    /**
     * Commit with only inserts
     */
    // Generate some input..
    List<HoodieRecord> recordsSoFar = new ArrayList<>(dataGen.generateInserts("001"/* ignore */, 100));
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
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key(), "false")
        .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), "true")
        .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), "true")
        .option(DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES().key(), DataSourceWriteOptions.SPARK_SQL_OPTIMIZED_WRITES().defaultValue())
        // This will remove any existing data at path below, and create a
        .mode(SaveMode.Overwrite);

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    log.info("First commit at instant time :" + commitInstantTime1);

    /**
     * Commit that updates records
     */
    List<HoodieRecord> recordsToBeUpdated = dataGen.generateUpdates("002"/* ignore */, 100);
    recordsSoFar.addAll(recordsToBeUpdated);
    List<String> records2 = recordsToStrings(recordsToBeUpdated);
    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));
    writer = inputDF2.write().format("org.apache.hudi").option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType) // Hoodie Table Type
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition")
        .option(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key(), "false")
        .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), "true")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName).mode(SaveMode.Append);

    updateHiveSyncConfig(writer);
    writer.save(tablePath);
    String commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    log.info("Second commit at instant time :" + commitInstantTime2);

    /**
     * Commit that Deletes some records
     */
    List<String> deletes = randomSelectAsHoodieKeys(recordsSoFar, 20).stream()
        .map(hr -> "{\"_row_key\":\"" + hr.getRecordKey() + "\",\"partition\":\"" + hr.getPartitionPath() + "\"}")
        .collect(Collectors.toList());
    Dataset<Row> inputDF3 = spark.read().json(jssc.parallelize(deletes, 2));
    writer = inputDF3.write().format("org.apache.hudi").option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .option("hoodie.delete.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType) // Hoodie Table Type
        .option(DataSourceWriteOptions.OPERATION().key(), "delete")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition")
        .option(HoodieTableConfig.ORDERING_FIELDS.key(), "timestamp")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key(), "false")
        .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), "true")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName).mode(SaveMode.Append);

    updateHiveSyncConfig(writer);
    writer.save(tablePath);
    String commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    log.info("Third commit at instant time :" + commitInstantTime3);

    /**
     * Read & do some queries
     */
    Dataset<Row> snapshotQueryDF = spark.read().format("org.apache.hudi").load(tablePath);
    snapshotQueryDF.registerTempTable("hoodie_ro");
    spark.sql("describe hoodie_ro").show();
    // all trips whose fare amount was greater than 2.
    spark.sql("select fare.amount, begin_lon, begin_lat, timestamp from hoodie_ro where fare.amount > 2.0").show();

    if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) {
      /**
       * Consume incrementally, only changes in commit 2 above. Currently only supported for COPY_ON_WRITE TABLE
       */
      Dataset<Row> incQueryDF = spark.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
          // Only changes in write 2 above
          .option(DataSourceReadOptions.START_COMMIT().key(), commitInstantTime1)
          // For incremental view, pass in the root/base path of dataset
          .load(tablePath);

      log.info("You will only see records from : " + commitInstantTime2);
      incQueryDF.groupBy(incQueryDF.col("_hoodie_commit_time")).count().show();
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
}
