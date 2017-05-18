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


import org.apache.spark.sql.SparkSession;

/**
 * Examples to do Spark SQL on Hoodie dataset.
 */
public class HoodieSparkSQLExample {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("Hoodie SparkSQL")
                .config("hive.metastore.uris","thrift://localhost:10000")
                .config("spark.sql.hive.convertMetastoreParquet", false)
                .enableHiveSupport()
                .master("local[2]")
                .getOrCreate();

        spark.sql("describe hoodie_rt").show();
        spark.sql("select * from hoodie_rt").show();
        spark.sql("select end_lon as e1, driver, rider as r1, datestr, driver, datestr, rider, _hoodie_record_key from hoodie_rt").show();
        spark.sql("select fare, begin_lon, begin_lat, timestamp from hoodie_rt where fare > 2.0").show();
        spark.sql("select count(*) as cnt, _hoodie_file_name as file from hoodie_rt group by _hoodie_file_name").show();
    }
}
