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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;

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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;

public class HoodieDropPartitionTest {
  private static final Logger LOG = LogManager.getLogger(HoodieDropPartitionTest.class);
  private String tablePath = "/tmp/hoodie/streaming/drop-part-table";
  private String streamingSourcePath = "/tmp/hoodie/drop-part/source";
  private String streamingCheckpointingPath = "/tmp/hoodie/drop-part/checkpoint";
  private Long streamingDurationInMs = 15000L;
  private String tableName = "drop_part_test";
  private String tableType = HoodieTableType.MERGE_ON_READ.name();

  @Test
  public void dropPartitionTest() {
    HoodieDropPartitionTest cli = new HoodieDropPartitionTest();
    int errStatus = 0;
    try {
      cli.run();
    } catch (Exception ex) {
      LOG.error("Got error ", ex);
      errStatus = -1;
    } finally {
      System.exit(errStatus);
    }
  }

  private void stream(Dataset<Row> streamingInput, String operationType, String checkpointLocation) throws Exception {
    DataStreamWriter<Row> writer = streamingInput.writeStream()
        .format("org.apache.hudi")
        .option("hoodie.insert.shuffle.parallelism", "1")
        .option("hoodie.upsert.shuffle.parallelism", "1")
        .option("hoodie.delete.shuffle.parallelism", "1")
        .option(DataSourceWriteOptions.OPERATION().key(), operationType)
        .option(DataSourceWriteOptions.TABLE_TYPE().key(), tableType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1")
        .option("hoodie.compact.inline", "true")
        .option(HoodieMetadataConfig.ENABLE.key(), false)
        .option("hoodie.datasource.write.drop.partition.columns", false)
        .option("hoodie.index.type", "BUCKET")
        .option("hoodie.bucket.index.hash.field", "_row_key")
        .option("hoodie.bucket.index.num.buckets", "1")
        .option("hoodie.storage.layout.partitioner.class", "org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner")
        .option("hoodie.storage.layout.type", "BUCKET")
        .option(HoodieCompactionConfig.PRESERVE_COMMIT_METADATA.key(), "false")
        .option(DataSourceWriteOptions.STREAMING_IGNORE_FAILED_BATCH().key(), "true")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
        .option("checkpointLocation", checkpointLocation)
        .outputMode(OutputMode.Append());

    StreamingQuery query = writer.trigger(Trigger.ProcessingTime(500)).start(tablePath);
    query.awaitTermination(streamingDurationInMs);
  }

  private void waitCommits(int numCommits, JavaSparkContext jssc) throws InterruptedException {
    while (true) {
      try {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
            .setConf(jssc.hadoopConfiguration()).setBasePath(tablePath).build();
        int completeInstants = metaClient.getActiveTimeline().reload()
            .getTimelineOfActions(CollectionUtils.createSet(HoodieActiveTimeline.DELTA_COMMIT_ACTION))
            .filterCompletedInstants().countInstants();
        if (completeInstants >= numCommits) {
          return;
        }
      } catch (TableNotFoundException te) {
        LOG.info("table not found exception");
      } finally {
        Thread.sleep(10000);
      }
    }
  }

  private void run() throws Exception {
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark Streaming APP")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(streamingSourcePath), true);
    fs.delete(new Path(streamingCheckpointingPath), true);
    fs.delete(new Path(tablePath), true);
    fs.mkdirs(new Path(streamingSourcePath));

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    List<String> records1 = recordsToStrings(dataGen.generateInserts("001", 40));
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 1));

    List<String> records2 = recordsToStrings(dataGen.generateInserts("001", 40));
    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 1));

    List<String> records3 = recordsToStrings(dataGen.generateInserts("003", 40));
    Dataset<Row> inputDF3 = spark.read().json(jssc.parallelize(records3, 1));

    List<Dataset<Row>> dfList = CollectionUtils.createImmutableList(inputDF1, inputDF2, inputDF3);

    String ckptPath = streamingCheckpointingPath + "/stream1";
    String srcPath = streamingSourcePath + "/stream1";
    fs.mkdirs(new Path(ckptPath));
    fs.mkdirs(new Path(srcPath));

    Dataset<Row> streamingInput = spark.readStream().schema(inputDF1.schema()).json(srcPath + "/*");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    int numInitialCommits;

    // thread for spark structured streaming
    try {
      Future<Void> streamFuture = executor.submit(() -> {
        stream(streamingInput, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL(), ckptPath);
        return null;
      });

      // thread for adding data to the streaming source
      Future<Integer> showFuture = executor.submit(() -> addInput(srcPath, dfList, jssc));
      streamFuture.get();
      numInitialCommits = showFuture.get();
      LOG.info("numCommits : " + numInitialCommits);

      long totalRecordsProduced = inputDF1.count() + inputDF2.count() + inputDF3.count();
      Dataset<Row> hoodieROViewDF = spark.read().format("hudi")
          .load(tablePath + "/*/*/*/*");
      hoodieROViewDF.registerTempTable("drop_part_test");
      long totalRecordsWritten = spark.sql("select * from drop_part_test").count();
      ValidationUtils.checkArgument(totalRecordsWritten == totalRecordsProduced,
          "Expecting " + totalRecordsProduced + " records, Got " + totalRecordsWritten);
    } finally {
      fs.delete(new Path(streamingSourcePath), true);
      fs.delete(new Path(streamingCheckpointingPath), true);
      fs.delete(new Path(tablePath), true);
      executor.shutdownNow();
    }
  }

  private int addInput(String srcPath, List<Dataset<Row>> datasetList, JavaSparkContext jssc) throws Exception {
    int numExpCommits = 1;
    for (Dataset<Row> rowDataset : datasetList) {
      rowDataset.coalesce(1).write().mode(SaveMode.Append).json(srcPath);
      waitCommits(numExpCommits, jssc);
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(jssc.hadoopConfiguration()).setBasePath(tablePath).build();
      waitCompactionCompleted(metaClient);
      numExpCommits += 1;
    }
    return numExpCommits;
  }

  private void waitCompactionCompleted(HoodieTableMetaClient metaClient) throws InterruptedException {
    if (metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().lastInstant().isPresent()) {
      Thread.sleep(10000);
      waitCompactionCompleted(metaClient);
    }
  }
}
