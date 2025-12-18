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

package org.apache.hudi.integ.testsuite.validation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testcontainers-based migration of the 'validate.sh' CI script.
 * <p>Strategy:
 * Uses a single "CI" container to preserve the shared filesystem/network environment required by Hudi's integration tests, but orchestrates the logic (setup, service start, execution) in Java rather
 * than delegating to a shell script.
 */
@Testcontainers
public class SparkHadoopMrBundleTest {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHadoopMrBundleTest.class);

  // Image matching your CI environment
  private static final String DOCKER_IMAGE = "apachehudi/hudi-ci-bundle-validation-base:flink1200hive313spark350scala213";

  // Paths inside the container
  private static final String WORKDIR = "/opt/bundle-validation";
  private static final String JARS_DIR = WORKDIR + "/jars";

  // Services Paths
  private static final String DERBY_HOME = WORKDIR + "/db-derby-10.14.1.0-bin";
  private static final String HIVE_HOME = WORKDIR + "/apache-hive-3.1.3-bin";
  private static final String SPARK_HOME = WORKDIR + "/spark-3.5.0-bin-hadoop3";

  // Java 11 Path (as used in the script for this profile)
  private static final String JAVA_HOME_8 = "/opt/java/openjdk";
  private static final String JAVA_HOME_11 = "/usr/lib/jvm/java-11-openjdk";
  private static final String JAVA_HOME_17 = "/usr/lib/jvm/java-17-openjdk";
  private static final String JAVA_HOME = JAVA_HOME_8;

  // Local paths - YOU MUST UPDATE THESE to point to where your built jars and test scripts live locally
  private static final String LOCAL_SCRIPTS_PATH = "../packaging/bundle-validation/";
  private static final String LOCAL_SPARK_BUNDLE_TARGET = "../packaging/hudi-spark-bundle/target";
  private static final String LOCAL_MR_BUNDLE_TARGET = "../packaging/hudi-hadoop-mr-bundle/target";

  @Container
  private static final GenericContainer<?> CI_CONTAINER = new GenericContainer<>(DockerImageName.parse(DOCKER_IMAGE))
      .withCommand("tail", "-f", "/dev/null") // Keep container alive
      .withLogConsumer(new Slf4jLogConsumer(LOG));

  static {
    mountLocalAssets(CI_CONTAINER);
  }

  private static void mountLocalAssets(GenericContainer<?> container) {
    // 1. Mount the Scala scripts needed by test_spark_hadoop_mr_bundles
    container.withCopyFileToContainer(
        MountableFile.forHostPath(Paths.get(LOCAL_SCRIPTS_PATH, "spark_hadoop_mr").toAbsolutePath()),
        WORKDIR + "/spark_hadoop_mr"
    );

    // 2. Mount the Compiled Jars
    // We mount the distinct target directories to temporary locations inside the container
    // The setup() method will consolidate them into the expected jars/ folder.
    container.withCopyFileToContainer(
        MountableFile.forHostPath(Paths.get(LOCAL_SPARK_BUNDLE_TARGET).toAbsolutePath()),
        "/tmp/spark_bundle_target"
    );

    container.withCopyFileToContainer(
        MountableFile.forHostPath(Paths.get(LOCAL_MR_BUNDLE_TARGET).toAbsolutePath()),
        "/tmp/mr_bundle_target"
    );
  }

  @BeforeEach
  public void setup() throws IOException, InterruptedException {
    // 0. Consolidate Jars
    // Copy the specific jar files from the mounted target directories to the central jars directory
    execBash("mkdir -p " + JARS_DIR);
    execBash("ls -la");
    execBash("cp /tmp/spark_bundle_target/hudi-spark*.jar " + JARS_DIR + "/");
    execBash("cp /tmp/mr_bundle_target/hudi-hadoop-mr*.jar " + JARS_DIR + "/");

    // 1. Setup Symlinks (translating 'ln -sf' from validate.sh)
    // Local target dirs often contain sources/javadoc jars. Wildcards in 'ln' fail if multiple files match.
    // We explicitly select the main jar (filtering out sources/javadoc and taking the first match).
    execBash("ln -sf $(ls " + JARS_DIR + "/hudi-hadoop-mr*.jar | grep -v 'sources' | grep -v 'javadoc' | head -n 1) " + JARS_DIR + "/hadoop-mr.jar");
    execBash("ln -sf $(ls " + JARS_DIR + "/hudi-spark*.jar | grep -v 'sources' | grep -v 'javadoc' | head -n 1) " + JARS_DIR + "/spark.jar");

    // 2. Start Services (Derby & Hive)
    startServices();
  }

  @AfterEach
  public void cleanup() throws IOException, InterruptedException {
    // Kill java processes (Hive/Derby) to clean up for next test if needed
    CI_CONTAINER.execInContainer("pkill", "java");
  }

  @Test
  public void testSparkHadoopMrBundles() throws IOException, InterruptedException {
    LOG.info("Starting Spark + Hadoop MR bundle validation...");

    // Step 1: Write sample data via Spark DataSource and run Hive Sync
    String writeCommand = String.format(
        "export JAVA_HOME=%s && %s/bin/spark-shell " +
            "--jars %s/spark.jar " +
            "--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' " +
            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' " +
            "--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' " +
            "--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' " +
            "< %s/spark_hadoop_mr/write.scala",
        JAVA_HOME, SPARK_HOME, JARS_DIR, WORKDIR
    );
    execBash(writeCommand);

    // Step 2: Query and validate the results using Spark SQL
    LOG.info("Validating with Spark SQL...");
    String validateCommand = String.format(
        "export JAVA_HOME=%s && %s/bin/spark-shell " +
            "--jars %s/spark.jar " +
            "--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' " +
            "--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' " +
            "--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar' " +
            "--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' " +
            "< %s/spark_hadoop_mr/validate.scala",
        JAVA_HOME, SPARK_HOME, JARS_DIR, WORKDIR
    );
    execBash(validateCommand);

    // Step 3: Check Spark Results
    // The script checks: numRecords=$(cat /tmp/spark-bundle/sparksql/trips/results/*.csv | wc -l)
    String sparkResultCount = execBashAndGetOutput("cat /tmp/spark-bundle/sparksql/trips/results/*.csv | wc -l").trim();
    assertEquals("10", sparkResultCount, "Spark SQL validation failed. Expected 10 records.");

    // Step 4: Query and validate the results using HiveQL
    LOG.info("Validating with HiveQL...");
    String hiveOutputDir = "/tmp/hadoop-mr-bundle/hiveql/trips/results";
    execBash("mkdir -p " + hiveOutputDir);

    // We skip Hive2 check here as the image implies Hive3
    String hiveQuery =
        "export JAVA_HOME=" + JAVA_HOME + " && " +
            HIVE_HOME + "/bin/beeline " +
            "--hiveconf hive.input.format=org.apache.hudi.hadoop.HoodieParquetInputFormat " +
            "-u jdbc:hive2://localhost:10000/default " +
            "--showHeader=false --outputformat=csv2 " +
            "-e 'select * from trips' >> " + hiveOutputDir + "/results.csv";

    execBash(hiveQuery);

    // Step 5: Check Hive Results
    String hiveResultCount = execBashAndGetOutput("cat " + hiveOutputDir + "/*.csv | wc -l").trim();
    assertEquals("10", hiveResultCount, "HiveQL validation failed. Expected 10 records.");
  }

  private void startServices() throws IOException, InterruptedException {
    LOG.info("Starting Derby and HiveServer2...");

    // Start Derby in background
    // We use 'nohup ... &' wrapped in bash -c to ensure execInContainer doesn't hang waiting for it
    String startDerby = String.format(
        "export JAVA_HOME=%s && nohup %s/bin/startNetworkServer -h 0.0.0.0 > /tmp/derby.log 2>&1 &",
        JAVA_HOME, DERBY_HOME
    );
    LOG.info("Executing: {}", startDerby);
    CI_CONTAINER.execInContainer("/bin/bash", "-c", startDerby);
    waitForPort(1527, "Derby");

    // Start HiveServer2 in background
    String startHive = String.format(
        "export JAVA_HOME=%s && nohup %s/bin/hiveserver2 --hiveconf hive.aux.jars.path=%s/hadoop-mr.jar > /tmp/hive.log 2>&1 &",
        JAVA_HOME, HIVE_HOME, JARS_DIR
    );
    LOG.info("Executing: {}", startHive);
    CI_CONTAINER.execInContainer("/bin/bash", "-c", startHive);
    waitForPort(10000, "HiveServer2");
  }

  private void waitForPort(int port, String serviceName) throws InterruptedException, IOException {
    LOG.info("Waiting for {} on port {}...", serviceName, port);
    for (int i = 0; i < 300; i++) {
      // Use netstat to check if port is listening
      org.testcontainers.containers.Container.ExecResult result =
          CI_CONTAINER.execInContainer("/bin/bash", "-c", "netstat -an | grep " + port);

      if (result.getExitCode() == 0 && !result.getStdout().isEmpty()) {
        LOG.info("{} is ready.", serviceName);
        return;
      }
      Thread.sleep(2000);
    }
    throw new RuntimeException("Timed out waiting for " + serviceName + " to start.");
  }

  /**
   * Helper to execute a bash command and ensure it succeeds.
   */
  private void execBash(String command) throws IOException, InterruptedException {
    LOG.info("Exec: {}", command);
    org.testcontainers.containers.Container.ExecResult result =
        CI_CONTAINER.execInContainer("/bin/bash", "-c", command);

    if (result.getExitCode() != 0) {
      LOG.error("Command failed: {}\nSTDERR: {}", command, result.getStderr());
      throw new RuntimeException("Command failed with exit code " + result.getExitCode());
    }
  }

  /**
   * Helper to execute a bash command and return stdout.
   */
  private String execBashAndGetOutput(String command) throws IOException, InterruptedException {
    org.testcontainers.containers.Container.ExecResult result =
        CI_CONTAINER.execInContainer("/bin/bash", "-c", command);
    if (result.getExitCode() != 0) {
      throw new RuntimeException("Command failed: " + command + "\n" + result.getStderr());
    }
    return result.getStdout();
  }
}