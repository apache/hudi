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

package com.uber.hoodie.cli.utils;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.cli.commands.SparkMain;
import com.uber.hoodie.common.util.FSUtils;
import java.io.File;
import java.net.URISyntaxException;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;

public class SparkUtil {

  public static Logger logger = Logger.getLogger(SparkUtil.class);
  public static final String DEFUALT_SPARK_MASTER = "yarn-client";

  /**
   * TODO: Need to fix a bunch of hardcoded stuff here eg: history server, spark distro
   */
  public static SparkLauncher initLauncher(String propertiesFile) throws URISyntaxException {
    String currentJar = new File(
        SparkUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath())
        .getAbsolutePath();
    SparkLauncher sparkLauncher =
        new SparkLauncher().setAppResource(currentJar)
            .setMainClass(SparkMain.class.getName())
            .setPropertiesFile(propertiesFile);
    File libDirectory = new File(new File(currentJar).getParent(), "lib");
    for (String library : libDirectory.list()) {
      sparkLauncher.addJar(new File(libDirectory, library).getAbsolutePath());
    }
    return sparkLauncher;
  }

  public static JavaSparkContext initJavaSparkConf(String name) {
    SparkConf sparkConf = new SparkConf().setAppName(name);
    sparkConf.setMaster(DEFUALT_SPARK_MASTER);
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.driver.maxResultSize", "2g");
    sparkConf.set("spark.eventLog.overwrite", "true");
    sparkConf.set("spark.eventLog.enabled", "true");

    // Configure hadoop conf
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    sparkConf = HoodieWriteClient.registerClasses(sparkConf);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    jsc.hadoopConfiguration().setBoolean("parquet.enable.summary-metadata", false);
    FSUtils.prepareHadoopConf(jsc.hadoopConfiguration());
    return jsc;
  }
}
