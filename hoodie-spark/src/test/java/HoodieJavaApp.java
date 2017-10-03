/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *           http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hoodie.DataSourceReadOptions;
import com.uber.hoodie.DataSourceWriteOptions;
import com.uber.hoodie.HoodieDataSourceHelpers;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.config.HoodieWriteConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
/**
 * Sample program that writes & reads hoodie datasets via the Spark datasource
 */
public class HoodieJavaApp {

    @Parameter(names={"--table-path", "-p"}, description = "path for Hoodie sample table")
    private String tablePath = "file:///tmp/hoodie/sample-table";

    @Parameter(names={"--table-name", "-n"}, description = "table name for Hoodie sample table")
    private String tableName =  "hoodie_test";

    @Parameter(names={"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
    private String tableType =  HoodieTableType.COPY_ON_WRITE.name();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    private static Logger logger = LogManager.getLogger(HoodieJavaApp.class);

    public static void main(String[] args) throws Exception {
        HoodieJavaApp cli = new HoodieJavaApp();
        JCommander cmd = new JCommander(cli, args);

        if (cli.help) {
            cmd.usage();
            System.exit(1);
        }
        cli.run();
    }

    public void run() throws Exception {

        // Spark session setup..
        SparkSession spark = SparkSession.builder()
                .appName("Hoodie Spark APP")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[1]")
                .getOrCreate();
        JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
        FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());

        // Generator of some records to be loaded in.
        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

        /**
         * Commit with only inserts
         */
        // Generate some input..
        List<String> records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001"/* ignore */, 100));
        Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

        // Save as hoodie dataset (copy on write)
        inputDF1.write()
                .format("com.uber.hoodie") // specify the hoodie source
                .option("hoodie.insert.shuffle.parallelism", "2") // any hoodie client config can be passed like this
                .option("hoodie.upsert.shuffle.parallelism", "2") // full list in HoodieWriteConfig & its package
                .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL()) // insert
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key") // This is the record key
                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition") // this is the partition to place it into
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp") // use to combine duplicate records in input/with disk val
                .option(HoodieWriteConfig.TABLE_NAME, tableName) // Used by hive sync and queries
                .mode(SaveMode.Overwrite) // This will remove any existing data at path below, and create a new dataset if needed
                .save(tablePath); // ultimately where the dataset will be placed
        String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
        logger.info("First commit at instant time :" + commitInstantTime1);

        /**
         * Commit that updates records
         */
        List<String> records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("002"/* ignore */, 100));
        Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));
        inputDF2.write()
                .format("com.uber.hoodie")
                .option("hoodie.insert.shuffle.parallelism", "2")
                .option("hoodie.upsert.shuffle.parallelism", "2")
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
                .option(HoodieWriteConfig.TABLE_NAME, tableName)
                .mode(SaveMode.Append)
                .save(tablePath);
        String commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
        logger.info("Second commit at instant time :" + commitInstantTime1);

        /**
         * Read & do some queries
         */
        Dataset<Row> hoodieROViewDF = spark.read()
                .format("com.uber.hoodie")
                // pass any path glob, can include hoodie & non-hoodie datasets
                .load(tablePath + "/*/*/*/*");
        hoodieROViewDF.registerTempTable("hoodie_ro");
        spark.sql("describe hoodie_ro").show();
        // all trips whose fare was greater than 2.
        spark.sql("select fare, begin_lon, begin_lat, timestamp from hoodie_ro where fare > 2.0").show();


        /**
         * Consume incrementally, only changes in commit 2 above.
         */
        Dataset<Row> hoodieIncViewDF = spark.read().format("com.uber.hoodie")
                .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(), DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), commitInstantTime1) // Only changes in write 2 above
                .load(tablePath); // For incremental view, pass in the root/base path of dataset

        logger.info("You will only see records from : " + commitInstantTime2);
        hoodieIncViewDF.groupBy(hoodieIncViewDF.col("_hoodie_commit_time")).count().show();
    }
}
