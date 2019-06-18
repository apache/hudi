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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieAvroPayload;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex.IndexType;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Driver program that uses the Hoodie client with synthetic workload, and performs basic operations. <p>
 */
public class HoodieClientExample {

  private static Logger logger = LogManager.getLogger(HoodieClientExample.class);
  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;
  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
  private String tablePath = "file:///tmp/hoodie/sample-table";
  @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
  private String tableName = "hoodie_rt";
  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
  private String tableType = HoodieTableType.COPY_ON_WRITE.name();

  public static void main(String[] args) throws Exception {
    HoodieClientExample cli = new HoodieClientExample();
    JCommander cmd = new JCommander(cli, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    cli.run();
  }


  public void run() throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("hoodie-client-example");
    sparkConf.setMaster("local[1]");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryoserializer.buffer.max", "512m");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // initialize the table, if not done already
    Path path = new Path(tablePath);
    FileSystem fs = FSUtils.getFs(tablePath, jsc.hadoopConfiguration());
    if (!fs.exists(path)) {
      HoodieTableMetaClient
          .initTableType(jsc.hadoopConfiguration(), tablePath, HoodieTableType.valueOf(tableType), tableName,
              HoodieAvroPayload.class.getName());
    }

    // Create the write client to write some records in
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
        .forTable(tableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.BLOOM).build())
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().archiveCommitsWith(2, 3).build()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = client.startCommit();
    logger.info("Starting commit " + newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<HoodieRecord> writeRecords = jsc.<HoodieRecord>parallelize(records, 1);
    client.upsert(writeRecords, newCommitTime);

    /**
     * Write 2 (updates)
     */
    newCommitTime = client.startCommit();
    logger.info("Starting commit " + newCommitTime);
    records.addAll(dataGen.generateUpdates(newCommitTime, 100));
    writeRecords = jsc.<HoodieRecord>parallelize(records, 1);
    client.upsert(writeRecords, newCommitTime);

    /**
     * Schedule a compaction and also perform compaction on a MOR dataset
     */
    if (HoodieTableType.valueOf(tableType) == HoodieTableType.MERGE_ON_READ) {
      Optional<String> instant = client.scheduleCompaction(Optional.empty());
      JavaRDD<WriteStatus> writeStatues = client.compact(instant.get());
      client.commitCompaction(instant.get(), writeStatues, Optional.empty());
    }
  }

}
