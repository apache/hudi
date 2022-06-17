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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.model.partextractor.MultiPartKeysValueExtractor;
import org.apache.hudi.sync.common.model.partextractor.SlashEncodedDayPartitionValueExtractor;

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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;

/**
 * Sample program that writes & reads hoodie tables via the Spark datasource streaming.
 */
public class HoodieJavaStreamingApp {

  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
  private String tablePath = "/tmp/hoodie/streaming/sample-table";

  @Parameter(names = {"--streaming-source-path", "-ssp"}, description = "path for streaming source file folder")
  private String streamingSourcePath = "/tmp/hoodie/streaming/source";

  @Parameter(names = {"--streaming-checkpointing-path", "-scp"},
      description = "path for streaming checking pointing folder")
  private String streamingCheckpointingPath = "/tmp/hoodie/streaming/checkpoint";

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


  private static final Logger LOG = LogManager.getLogger(HoodieJavaStreamingApp.class);

  public static void main(String[] args) throws Exception {
    HoodieJavaStreamingApp cli = new HoodieJavaStreamingApp();
    JCommander cmd = new JCommander(cli, null, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    int errStatus = 0;
    try {
      cli.run();
    } catch (Exception ex) {
      LOG.error("Got error running app ", ex);
      errStatus = -1;
    } finally {
      System.exit(errStatus);
    }
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

    List<String> records1 = recordsToStrings(dataGen.generateInserts("001", 100));
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    List<String> records2 = recordsToStrings(dataGen.generateUpdatesForAllRecords("002"));
    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));


    String ckptPath = streamingCheckpointingPath + "/stream1";
    String srcPath = streamingSourcePath + "/stream1";
    fs.mkdirs(new Path(ckptPath));
    fs.mkdirs(new Path(srcPath));

    // setup the input for streaming
    Dataset<Row> streamingInput = spark.readStream().schema(inputDF1.schema()).json(srcPath + "/*");

    // start streaming and showing
    ExecutorService executor = Executors.newFixedThreadPool(2);
    int numInitialCommits = 0;

    // thread for spark structured streaming
    try {
      Future<Void> streamFuture = executor.submit(() -> {
        LOG.info("===== Streaming Starting =====");
        stream(streamingInput, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL(), ckptPath);
        LOG.info("===== Streaming Ends =====");
        return null;
      });

      // thread for adding data to the streaming source and showing results over time
      Future<Integer> showFuture = executor.submit(() -> {
        LOG.info("===== Showing Starting =====");
        int numCommits = addInputAndValidateIngestion(spark, fs,  srcPath,0, 100, inputDF1, inputDF2, true);
        LOG.info("===== Showing Ends =====");
        return numCommits;
      });

      // let the threads run
      streamFuture.get();
      numInitialCommits = showFuture.get();
    } finally {
      executor.shutdownNow();
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jssc.hadoopConfiguration()).setBasePath(tablePath).build();
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      // Ensure we have successfully completed one compaction commit
      ValidationUtils.checkArgument(metaClient.getActiveTimeline().getCommitTimeline().getInstants().count() == 1);
    } else {
      ValidationUtils.checkArgument(metaClient.getActiveTimeline().getCommitTimeline().getInstants().count() >= 1);
    }

    // Deletes Stream
    // Need to restart application to ensure spark does not assume there are multiple streams active.
    spark.close();
    SparkSession newSpark = SparkSession.builder().appName("Hoodie Spark Streaming APP")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    jssc = new JavaSparkContext(newSpark.sparkContext());
    String ckptPath2 = streamingCheckpointingPath + "/stream2";
    String srcPath2 = srcPath + "/stream2";
    fs.mkdirs(new Path(ckptPath2));
    fs.mkdirs(new Path(srcPath2));
    Dataset<Row> delStreamingInput = newSpark.readStream().schema(inputDF1.schema()).json(srcPath2 + "/*");
    List<String> deletes = recordsToStrings(dataGen.generateUniqueUpdates("002", 20));
    Dataset<Row> inputDF3 = newSpark.read().json(jssc.parallelize(deletes, 2));
    executor = Executors.newFixedThreadPool(2);

    // thread for spark structured streaming
    try {
      Future<Void> streamFuture = executor.submit(() -> {
        LOG.info("===== Streaming Starting =====");
        stream(delStreamingInput, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL(), ckptPath2);
        LOG.info("===== Streaming Ends =====");
        return null;
      });

      final int numCommits = numInitialCommits;
      // thread for adding data to the streaming source and showing results over time
      Future<Void> showFuture = executor.submit(() -> {
        LOG.info("===== Showing Starting =====");
        addInputAndValidateIngestion(newSpark, fs, srcPath2, numCommits, 80, inputDF3, null, false);
        LOG.info("===== Showing Ends =====");
        return null;
      });

      // let the threads run
      streamFuture.get();
      showFuture.get();
    } finally {
      executor.shutdown();
    }
  }

  private void waitTillNCommits(FileSystem fs, int numCommits, int timeoutSecs, int sleepSecsAfterEachRun)
      throws InterruptedException {
    long beginTime = System.currentTimeMillis();
    long currTime = beginTime;
    long timeoutMsecs = timeoutSecs * 1000;

    while ((currTime - beginTime) < timeoutMsecs) {
      try {
        HoodieTimeline timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, tablePath);
        LOG.info("Timeline :" + timeline.getInstants().collect(Collectors.toList()));
        if (timeline.countInstants() >= numCommits) {
          return;
        }
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(fs.getConf()).setBasePath(tablePath).setLoadActiveTimelineOnLoad(true).build();
        System.out.println("Instants :" + metaClient.getActiveTimeline().getInstants().collect(Collectors.toList()));
      } catch (TableNotFoundException te) {
        LOG.info("Got table not found exception. Retrying");
      } finally {
        Thread.sleep(sleepSecsAfterEachRun * 1000);
        currTime = System.currentTimeMillis();
      }
    }
    throw new IllegalStateException("Timedout waiting for " + numCommits + " commits to appear in " + tablePath);
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
  public int addInputAndValidateIngestion(SparkSession spark, FileSystem fs, String srcPath,
      int initialCommits, int expRecords,
      Dataset<Row> inputDF1, Dataset<Row> inputDF2, boolean instantTimeValidation) throws Exception {
    // Ensure, we always write only one file. This is very important to ensure a single batch is reliably read
    // atomically by one iteration of spark streaming.
    inputDF1.coalesce(1).write().mode(SaveMode.Append).json(srcPath);

    int numExpCommits = initialCommits + 1;
    // wait for spark streaming to process one microbatch
    waitTillNCommits(fs, numExpCommits, 180, 3);
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    LOG.info("First commit at instant time :" + commitInstantTime1);

    String commitInstantTime2 = commitInstantTime1;
    if (null != inputDF2) {
      numExpCommits += 1;
      inputDF2.write().mode(SaveMode.Append).json(srcPath);
      // wait for spark streaming to process one microbatch
      Thread.sleep(3000);
      waitTillNCommits(fs, numExpCommits, 180, 3);
      commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
      LOG.info("Second commit at instant time :" + commitInstantTime2);
    }

    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      numExpCommits += 1;
      // Wait for compaction to also finish and track latest timestamp as commit timestamp
      waitTillNCommits(fs, numExpCommits, 180, 3);
      commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
      LOG.info("Compaction commit at instant time :" + commitInstantTime2);
    }

    /**
     * Read & do some queries
     */
    Dataset<Row> hoodieROViewDF = spark.read().format("hudi")
        // pass any path glob, can include hoodie & non-hoodie
        // datasets
        .load(tablePath + "/*/*/*/*");
    hoodieROViewDF.registerTempTable("hoodie_ro");
    spark.sql("describe hoodie_ro").show();
    // all trips whose fare amount was greater than 2.
    spark.sql("select fare.amount, begin_lon, begin_lat, timestamp from hoodie_ro where fare.amount > 2.0").show();

    if (instantTimeValidation) {
      System.out.println("Showing all records. Latest Instant Time =" + commitInstantTime2);
      spark.sql("select * from hoodie_ro").show(200, false);
      long numRecordsAtInstant2 =
          spark.sql("select * from hoodie_ro where _hoodie_commit_time = " + commitInstantTime2).count();
      ValidationUtils.checkArgument(numRecordsAtInstant2 == expRecords,
          "Expecting " + expRecords + " records, Got " + numRecordsAtInstant2);
    }

    long numRecords = spark.sql("select * from hoodie_ro").count();
    ValidationUtils.checkArgument(numRecords == expRecords,
        "Expecting " + expRecords + " records, Got " + numRecords);

    if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) {
      /**
       * Consume incrementally, only changes in commit 2 above. Currently only supported for COPY_ON_WRITE TABLE
       */
      Dataset<Row> hoodieIncViewDF = spark.read().format("hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
          // Only changes in write 2 above
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), commitInstantTime1)
          // For incremental view, pass in the root/base path of dataset
          .load(tablePath);

      LOG.info("You will only see records from : " + commitInstantTime2);
      hoodieIncViewDF.groupBy(hoodieIncViewDF.col("_hoodie_commit_time")).count().show();
    }
    return numExpCommits;
  }

  /**
   * Hoodie spark streaming job.
   * 
   * @param streamingInput
   * @throws Exception
   */
  public void stream(Dataset<Row> streamingInput, String operationType, String checkpointLocation) throws Exception {

    DataStreamWriter<Row> writer = streamingInput.writeStream().format("org.apache.hudi")
        .option("hoodie.insert.shuffle.parallelism", "2").option("hoodie.upsert.shuffle.parallelism", "2")
        .option("hoodie.delete.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.OPERATION().key(), operationType)
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE().key(), "true")
        .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE().key(), "true")
        .option(HoodieCompactionConfig.PRESERVE_COMMIT_METADATA.key(), "false")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName).option("checkpointLocation", checkpointLocation)
        .outputMode(OutputMode.Append());

    updateHiveSyncConfig(writer);
    StreamingQuery query = writer.trigger(Trigger.ProcessingTime(500)).start(tablePath);
    query.awaitTermination(streamingDurationInMs);
  }

  /**
   * Setup configs for syncing to hive.
   * 
   * @param writer
   * @return
   */
  private DataStreamWriter<Row> updateHiveSyncConfig(DataStreamWriter<Row> writer) {
    if (enableHiveSync) {
      LOG.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(HiveSyncConfig.META_SYNC_TABLE_NAME.key(), hiveTable)
          .option(HiveSyncConfig.META_SYNC_DATABASE_NAME.key(), hiveDB)
          .option(HiveSyncConfig.HIVE_URL.key(), hiveJdbcUrl)
          .option(HiveSyncConfig.HIVE_USER.key(), hiveUser)
          .option(HiveSyncConfig.HIVE_PASS.key(), hivePass)
          .option(HiveSyncConfig.HIVE_SYNC_ENABLED.key(), "true");
      if (useMultiPartitionKeys) {
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
