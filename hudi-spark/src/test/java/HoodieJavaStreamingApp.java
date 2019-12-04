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
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.ProcessingTime;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Sample program that writes & reads hoodie datasets via the Spark datasource streaming.
 */
public class HoodieJavaStreamingApp {

  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
  private String tablePath = "file:///tmp/hoodie/streaming/sample-table";

  @Parameter(names = {"--streaming-source-path", "-ssp"}, description = "path for streaming source file folder")
  private String streamingSourcePath = "file:///tmp/hoodie/streaming/source";

  @Parameter(names = {"--streaming-checkpointing-path", "-scp"},
      description = "path for streaming checking pointing folder")
  private String streamingCheckpointingPath = "file:///tmp/hoodie/streaming/checkpoint";

  @Parameter(names = {"--streaming-duration-in-ms", "-sdm"},
      description = "time in millisecond for the streaming duration")
  private Long streamingDurationInMs = 15000L;

  @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
  private String tableName = "hoodie_test";

  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
  private String tableType = HoodieTableType.MERGE_ON_READ.name();

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

  @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
  private Boolean useMultiPartitionKeys = false;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;


  private static Logger logger = LogManager.getLogger(HoodieJavaStreamingApp.class);

  public static void main(String[] args) throws Exception {
    HoodieJavaStreamingApp cli = new HoodieJavaStreamingApp();
    JCommander cmd = new JCommander(cli, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    cli.run();
  }

  /**
   *
   * @throws Exception
   */
  public void run() throws Exception {
    // Spark session setup..
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark Streaming APP")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());

    // folder path clean up and creation, preparing the environment
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(streamingSourcePath), true);
    fs.delete(new Path(streamingCheckpointingPath), true);
    fs.delete(new Path(tablePath), true);
    fs.mkdirs(new Path(streamingSourcePath));

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    List<String> records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 100));
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    List<String> records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("002", 100));

    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));

    // setup the input for streaming
    Dataset<Row> streamingInput = spark.readStream().schema(inputDF1.schema()).json(streamingSourcePath);


    // start streaming and showing
    ExecutorService executor = Executors.newFixedThreadPool(2);

    // thread for spark strucutured streaming
    Future<Void> streamFuture = executor.submit(() -> {
      logger.info("===== Streaming Starting =====");
      stream(streamingInput);
      logger.info("===== Streaming Ends =====");
      return null;
    });

    // thread for adding data to the streaming source and showing results over time
    Future<Void> showFuture = executor.submit(() -> {
      logger.info("===== Showing Starting =====");
      show(spark, fs, inputDF1, inputDF2);
      logger.info("===== Showing Ends =====");
      return null;
    });

    // let the threads run
    streamFuture.get();
    showFuture.get();

    executor.shutdown();
  }

  /**
   * Adding data to the streaming source and showing results over time.
   * 
   * @param spark
   * @param fs
   * @param inputDF1
   * @param inputDF2
   * @throws Exception
   */
  public void show(SparkSession spark, FileSystem fs, Dataset<Row> inputDF1, Dataset<Row> inputDF2) throws Exception {
    inputDF1.write().mode(SaveMode.Append).json(streamingSourcePath);
    // wait for spark streaming to process one microbatch
    Thread.sleep(3000);
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    logger.info("First commit at instant time :" + commitInstantTime1);

    inputDF2.write().mode(SaveMode.Append).json(streamingSourcePath);
    // wait for spark streaming to process one microbatch
    Thread.sleep(3000);
    String commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    logger.info("Second commit at instant time :" + commitInstantTime2);

    /**
     * Read & do some queries
     */
    Dataset<Row> hoodieROViewDF = spark.read().format("org.apache.hudi")
        // pass any path glob, can include hoodie & non-hoodie
        // datasets
        .load(tablePath + "/*/*/*/*");
    hoodieROViewDF.registerTempTable("hoodie_ro");
    spark.sql("describe hoodie_ro").show();
    // all trips whose fare was greater than 2.
    spark.sql("select fare, begin_lon, begin_lat, timestamp from hoodie_ro where fare > 2.0").show();

    if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) {
      /**
       * Consume incrementally, only changes in commit 2 above. Currently only supported for COPY_ON_WRITE TABLE
       */
      Dataset<Row> hoodieIncViewDF = spark.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(), DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
          // Only changes in write 2 above
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), commitInstantTime1)
          // For incremental view, pass in the root/base path of dataset
          .load(tablePath);

      logger.info("You will only see records from : " + commitInstantTime2);
      hoodieIncViewDF.groupBy(hoodieIncViewDF.col("_hoodie_commit_time")).count().show();
    }
  }

  /**
   * Hoodie spark streaming job.
   * 
   * @param streamingInput
   * @throws Exception
   */
  public void stream(Dataset<Row> streamingInput) throws Exception {

    DataStreamWriter<Row> writer = streamingInput.writeStream().format("org.apache.hudi")
        .option("hoodie.insert.shuffle.parallelism", "2").option("hoodie.upsert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), tableType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        .option(HoodieWriteConfig.TABLE_NAME, tableName).option("checkpointLocation", streamingCheckpointingPath)
        .outputMode(OutputMode.Append());

    updateHiveSyncConfig(writer);
    writer.trigger(new ProcessingTime(500)).start(tablePath).awaitTermination(streamingDurationInMs);
  }

  /**
   * Setup configs for syncing to hive.
   * 
   * @param writer
   * @return
   */
  private DataStreamWriter<Row> updateHiveSyncConfig(DataStreamWriter<Row> writer) {
    if (enableHiveSync) {
      logger.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), hiveTable)
          .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), hiveDB)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hiveJdbcUrl)
          .option(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hiveUser)
          .option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hivePass)
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
      if (useMultiPartitionKeys) {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "year,month,day").option(
            DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            MultiPartKeysValueExtractor.class.getCanonicalName());
      } else {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "dateStr");
      }
    }
    return writer;
  }
}
