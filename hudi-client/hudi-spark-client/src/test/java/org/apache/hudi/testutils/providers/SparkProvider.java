/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.testutils.providers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.Map;

public interface SparkProvider extends org.apache.hudi.testutils.providers.HoodieEngineContextProvider {

  SparkSession spark();

  SQLContext sqlContext();

  JavaSparkContext jsc();

  default SparkConf conf(Map<String, String> overwritingConfigs) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.app.name", getClass().getName());
    sparkConf.set("spark.master", "local[8]");
    sparkConf.set("spark.default.parallelism", "4");
    sparkConf.set("spark.sql.shuffle.partitions", "4");
    sparkConf.set("spark.driver.maxResultSize", "2g");
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
    overwritingConfigs.forEach(sparkConf::set);
    return sparkConf;
  }

  default SparkConf conf() {
    return conf(Collections.emptyMap());
  }
}
