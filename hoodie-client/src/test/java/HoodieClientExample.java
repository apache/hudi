/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.index.HoodieIndex;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Properties;

/**
 * Driver program that uses the Hoodie client with synthetic workload, and performs basic
 * operations. <p>
 */
public class HoodieClientExample {


    private static Logger logger = LogManager.getLogger(HoodieClientExample.class);

    public static void main(String[] args) throws Exception {
        String tablePath = args.length == 1 ? args[0] : "file:///tmp/hoodie/sample-table";

        HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

        SparkConf sparkConf = new SparkConf().setAppName("hoodie-client-example");
        sparkConf.setMaster("local[1]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.max", "512m");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // generate some records to be loaded in.
        HoodieWriteConfig cfg =
            HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .forTable("sample-table").withIndexConfig(
                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .build();
        Properties properties = new Properties();
        properties.put(HoodieWriteConfig.TABLE_NAME, "sample-table");
        HoodieTableMetaClient
            .initializePathAsHoodieDataset(FSUtils.getFs(), tablePath,
                properties);
        HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

        /**
         * Write 1 (only inserts)
         */
        String newCommitTime = "001";
        logger.info("Starting commit " + newCommitTime);

        List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
        JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

        client.upsert(writeRecords, newCommitTime);

        /**
         * Write 2 (updates)
         */
        newCommitTime = "002";
        logger.info("Starting commit " + newCommitTime);
        records.addAll(dataGen.generateUpdates(newCommitTime, 100));

        writeRecords = jsc.parallelize(records, 1);
        client.upsert(writeRecords, newCommitTime);
    }
}
