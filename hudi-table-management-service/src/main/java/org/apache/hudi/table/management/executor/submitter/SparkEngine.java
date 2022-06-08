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

package org.apache.hudi.table.management.executor.submitter;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.table.management.common.EnvConstant;
import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.exception.HoodieTableManagementException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.cli.utils.SparkUtil.initLauncher;

public class SparkEngine extends ExecutionEngine {

  private static final Logger LOG = LoggerFactory.getLogger(SparkEngine.class);

  private String jobName;
  private Instance instance;
  private String mainClass;

  public SparkEngine(String jobName, Instance instance, String mainClass) {
    this.jobName = jobName;
    this.instance = instance;
    this.mainClass = mainClass;
  }

  @Override
  protected String getCommand() throws IOException {
    String format = "%s/bin/spark-submit --class %s --master yarn --deploy-mode cluster %s %s %s";
    return String.format(format, ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkHome),
        mainClass, getSparkArgs(), getSubmitJar(), getJobArgs());
  }

  @Override
  protected void beforeExecuteCommand() {

  }

  @Override
  public Map<String, String> setProcessEnv() {
    Map<String, String> env = new HashMap<>(16);
    env.put(EnvConstant.JAVA_HOME, ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.JavaHome));
    env.put(EnvConstant.YARN_CONF_DIR, ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.YarnConfDir));
    env.put(EnvConstant.SPARK_HOME, ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkHome));
    env.put(EnvConstant.HADOOP_CONF_DIR, ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.HadoopConfDir));
    env.put(EnvConstant.HADOOP_USER_NAME, instance.getOwner());
    return env;
  }

  private String getJobArgs() throws IOException {
    return null;
  }

  private String getSubmitJar() throws IOException {
    File sparkSubmitJarPath = new File(ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkSubmitJarPath));
    if (!sparkSubmitJarPath.isDirectory()) {
      throw new HoodieTableManagementException("Spark submit jar path " + sparkSubmitJarPath + " should be be a directory");
    }
    File[] jars = sparkSubmitJarPath.listFiles(file -> !file.getName().endsWith(".jar"));
    if (jars == null || jars.length != 1) {
      throw new HoodieTableManagementException("Spark submit jar path " + sparkSubmitJarPath
          + " should only have one jar, jars = " + Arrays.toString(jars));
    }
    return jars[0].getCanonicalPath();
  }

  private String getSparkArgs() {
    StringBuilder sparkArgs = new StringBuilder();
    sparkArgs.append("--queue ").append(instance.getQueue());
    sparkArgs.append(" --name ").append(jobName);

    Map<String, String> sparkParams = new HashMap<>();
    sparkParams.put("mapreduce.job.queuename", instance.getQueue());
    sparkParams.put("spark.shuffle.hdfs.enabled", ServiceConfig.getInstance()
        .getString(ServiceConfig.ServiceConfVars.SparkShuffleHdfsEnabled));
    String parallelism = StringUtils.isNullOrEmpty(instance.getParallelism())
        ? ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MaxExecutors)
        : instance.getParallelism();
    sparkParams.put("spark.dynamicAllocation.maxExecutors", parallelism);
    sparkParams.put("spark.dynamicAllocation.minExecutors",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.MinExecutors));
    sparkParams.put("spark.vcore.boost",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkVcoreBoost));
    sparkParams.put("spark.vcore.boost.ratio",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkVcoreBoostRatio));
    sparkParams.put("spark.speculation",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkSpeculation));
    String driverResource;
    String executorResource;
    String resource = instance.getResource().trim();
    if (StringUtils.isNullOrEmpty(resource)) {
      driverResource = ServiceConfig.getInstance()
          .getString(ServiceConfig.ServiceConfVars.DriverMemory);
      executorResource = ServiceConfig.getInstance()
          .getString(ServiceConfig.ServiceConfVars.ExecutorMemory);
    } else {
      String[] resourceArray = resource.split(":");
      if (resourceArray.length == 1) {
        driverResource = resourceArray[0];
        executorResource = resourceArray[0];
      } else if (resourceArray.length == 2) {
        driverResource = resourceArray[0];
        executorResource = resourceArray[1];
      } else {
        throw new RuntimeException(
            "Invalid conf: " + instance.getIdentifier() + ", resource: " + resource);
      }
    }
    sparkParams.put("spark.executor.cores",
        ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.ExecutorCores));
    sparkParams.put("spark.executor.memory", executorResource);
    sparkParams.put("spark.driver.memory", driverResource);
    sparkParams.put("spark.executor.memoryOverhead", ServiceConfig.getInstance()
        .getString(ServiceConfig.ServiceConfVars.ExecutorMemoryOverhead));

    for (Map.Entry<String, String> entry : sparkParams.entrySet()) {
      sparkArgs
          .append(" --conf ")
          .append(entry.getKey())
          .append("=")
          .append(entry.getValue());
    }

    return sparkArgs.toString();
  }

  @Override
  public String executeCommand(String jobName, Instance instance) throws HoodieTableManagementException {
    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(scala.collection.JavaConversions.propertiesAsScalaMap(System.getProperties()));
    SparkLauncher sparkLauncher;
    try {
      sparkLauncher = initLauncher(sparkPropertiesPath);
    } catch (URISyntaxException e) {
      LOG.error("Failed to init spark launcher");
      throw new HoodieTableManagementException("Failed to init spark launcher", e);
    }

    String master = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkMaster);
    String sparkMemory = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.ExecutorMemory);
    String parallelism = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.SparkParallelism);
    String retry = ServiceConfig.getInstance().getString(ServiceConfig.ServiceConfVars.RetryTimes);

    sparkLauncher.addAppArgs("COMPACT_RUN", master, sparkMemory, instance.getBasePath(),
        instance.getTableName(), instance.getInstant(), parallelism, "", retry, "");

    Process process;
    try {
      process = sparkLauncher.launch();
    } catch (IOException e) {
      LOG.error("Failed to launcher spark process");
      throw new HoodieTableManagementException("Failed to init spark launcher", e);
    }

    InputStream inputStream = null;
    BufferedReader bufferedReader = null;
    String applicationId = null;
    try {
      inputStream = process.getInputStream();
      bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        LOG.info(line);
        if (line.contains(YARN_SUBMITTED)) {
          String[] split = line.split(YARN_SUBMITTED);
          applicationId = split[1].trim();
          LOG.info("Execute job {} get application id {}", jobName, applicationId);
          break;
        }
      }
    } catch (Exception e) {
      LOG.error("execute {} process get application id error", jobName, e);
      throw new HoodieTableManagementException("execute " + jobName + " process get application id error", e);
    } finally {
      if (process != null) {
        process.destroyForcibly();
      }
      if (inputStream != null) {
        IOUtils.closeQuietly(inputStream);
      }
      if (bufferedReader != null) {
        IOUtils.closeQuietly(bufferedReader);
      }
    }

    return applicationId;
  }
}
