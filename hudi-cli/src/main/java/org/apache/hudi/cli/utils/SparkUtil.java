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

package org.apache.hudi.cli.utils;

import org.apache.hudi.cli.HoodieCliSparkConfig;
import org.apache.hudi.cli.commands.SparkEnvCommand;
import org.apache.hudi.cli.commands.SparkMain;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

/**
 * Utility functions dealing with Spark.
 */
public class SparkUtil {

  private static final String DEFAULT_SPARK_MASTER = "yarn";

  /**
   * TODO: Need to fix a bunch of hardcoded stuff here eg: history server, spark distro.
   */
  public static SparkLauncher initLauncher(String propertiesFile) throws URISyntaxException {
    String currentJar = new File(SparkUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath())
        .getAbsolutePath();
    Map<String, String> env = SparkEnvCommand.env;
    SparkLauncher sparkLauncher =
        new SparkLauncher(env).setAppResource(currentJar).setMainClass(SparkMain.class.getName());

    if (!StringUtils.isNullOrEmpty(propertiesFile)) {
      sparkLauncher.setPropertiesFile(propertiesFile);
    }
    File libDirectory = new File(new File(currentJar).getParent(), "lib");
    for (String library : Objects.requireNonNull(libDirectory.list())) {
      sparkLauncher.addJar(new File(libDirectory, library).getAbsolutePath());
    }
    return sparkLauncher;
  }

  public static JavaSparkContext initJavaSparkConf(String name) {
    return initJavaSparkConf(name, Option.empty(), Option.empty());
  }

  public static JavaSparkContext initJavaSparkConf(String name, Option<String> master,
      Option<String> executorMemory) {
    SparkConf sparkConf = new SparkConf().setAppName(name);

    String defMaster = master.orElse(sparkConf.getenv(HoodieCliSparkConfig.CLI_SPARK_MASTER));
    if ((null == defMaster) || (defMaster.isEmpty())) {
      sparkConf.setMaster(DEFAULT_SPARK_MASTER);
    } else {
      sparkConf.setMaster(defMaster);
    }

    sparkConf.set(HoodieCliSparkConfig.CLI_SERIALIZER, "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set(HoodieCliSparkConfig.CLI_DRIVER_MAX_RESULT_SIZE, "2g");
    sparkConf.set(HoodieCliSparkConfig.CLI_EVENT_LOG_OVERWRITE, "true");
    sparkConf.set(HoodieCliSparkConfig.CLI_EVENT_LOG_ENABLED, "true");
    if (executorMemory.isPresent()) {
      sparkConf.set(HoodieCliSparkConfig.CLI_EXECUTOR_MEMORY, executorMemory.get());
    }

    // Configure hadoop conf
    sparkConf.set(HoodieCliSparkConfig.CLI_MAPRED_OUTPUT_COMPRESS, "true");
    sparkConf.set(HoodieCliSparkConfig.CLI_MAPRED_OUTPUT_COMPRESSION_CODEC, "true");
    sparkConf.set(HoodieCliSparkConfig.CLI_MAPRED_OUTPUT_COMPRESSION_CODEC, "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set(HoodieCliSparkConfig.CLI_MAPRED_OUTPUT_COMPRESSION_TYPE, "BLOCK");

    SparkRDDWriteClient.registerClasses(sparkConf);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    jsc.hadoopConfiguration().setBoolean(HoodieCliSparkConfig.CLI_PARQUET_ENABLE_SUMMARY_METADATA, false);
    FSUtils.prepareHadoopConf(jsc.hadoopConfiguration());
    return jsc;
  }
}
