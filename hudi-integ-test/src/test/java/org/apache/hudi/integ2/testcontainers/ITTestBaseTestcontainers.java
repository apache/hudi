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

package org.apache.hudi.integ2.testcontainers;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.integ2.testcontainers.service.HiveService;
import org.apache.hudi.integ2.testcontainers.service.SparkService;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

import static org.apache.hudi.integ2.testcontainers.service.SparkService.ADHOC_1;
import static org.apache.hudi.integ2.testcontainers.service.SparkService.ADHOC_2;

/**
 * Base test class for integration tests using Testcontainers with Docker Compose.
 * Uses a "Service" pattern, where each external service (Hive, Spark, etc.) is
 * represented by a dedicated service object. This provides better separation of
 * concerns and a cleaner API for tests.
 */
@Slf4j
@Testcontainers
public abstract class ITTestBaseTestcontainers implements ContainerProvider {

  // Constants for container names remain the same...
  protected static final String NAMENODE_CONTAINER = "namenode";
  protected static final String DATANODE_1_CONTAINER = "datanode1";
  protected static final String HISTORY_SERVER_CONTAINER = "historyserver";
  protected static final String HIVE_METASTORE_PGSQL_CONTAINER = "hive-metastore-postgresql";
  protected static final String HIVE_METASTORE_CONTAINER = "hivemetastore";
  protected static final String ZOOKEEPER_CONTAINER = "zookeeper";
  protected static final String KAFKA_CONTAINER = "kafka";
  protected static final String SPARK_MASTER_CONTAINER = "sparkmaster";
  protected static final String SPARK_WORKER_1_CONTAINER = "sparkmaster";

  // Default compose files for Spark 4.0.1. Override with -Dspark.docker.compose.prefix to use
  // a different Spark version (e.g., "docker-compose_hadoop284_hive2310_spark353" for Spark 3.5.3).
  static final String COMPOSE_PREFIX_PROPERTY = "spark.docker.compose.prefix";
//  static final String DEFAULT_COMPOSE_PREFIX = "docker-compose_hadoop340_hive313_spark401";
  static final String DEFAULT_COMPOSE_PREFIX = "docker-compose_hadoop284_hive2310_spark353";
  // Token present in compose prefixes that run Spark 4.x (e.g. "...spark401"). Used to gate Spark 4+ only tests.
  private static final String SPARK_4_PREFIX_TOKEN = "spark4";
  private static final String COMPOSE_DIR = "../docker/compose/";

  protected static ComposeContainer environment;

  // Service objects for interacting with different components
  protected HiveService hive;
  protected SparkService sparkAdhoc1;
  protected SparkService sparkAdhoc2;

  @BeforeAll
  public static void setupDockerCompose() {
    String composeFilePath = createProcessedComposeFilePath(getDockerComposeFilePath());
    String hudiWorkspace = getHudiWorkspace();

    log.info("Starting Docker Compose environment");
    log.info("Compose file: {}", composeFilePath);
    log.info("HUDI_WS: {}", hudiWorkspace);

    final int sparkMasterServicePort = 8080;
    environment = new ComposeContainer(new File(composeFilePath))
        .withEnv("HUDI_WS", hudiWorkspace)
        .withExposedService(SPARK_MASTER_CONTAINER, sparkMasterServicePort,
            Wait.forListeningPort().forPorts(sparkMasterServicePort).withStartupTimeout(Duration.ofMinutes(5)))
        .withStartupTimeout(Duration.ofMinutes(5));
    environment.start();

    log.info("Docker Compose environment started successfully");
    log.info("All containers verified and running");
  }

  /**
   * Initialize service objects. Should be called in @BeforeEach or constructor of test class.
   */
  protected void initializeServices() {
    this.hive = new HiveService(this);
    this.sparkAdhoc1 = new SparkService(this, ADHOC_1);
    this.sparkAdhoc2 = new SparkService(this, ADHOC_2);
  }

  /**
   * Skips the test unless the active docker-compose prefix points to a Spark 4.x stack.
   * Use for tests that rely on Spark 4.0+ only features (e.g. VARIANT type).
   */
  protected static void assumeSpark4Compose() {
    String composePrefix = System.getProperty(COMPOSE_PREFIX_PROPERTY, DEFAULT_COMPOSE_PREFIX);
    Assumptions.assumeTrue(
        composePrefix.contains(SPARK_4_PREFIX_TOKEN),
        "Test requires a Spark 4.x compose stack; active prefix is '" + composePrefix + "'");
  }

  /**
   * Waits for HDFS namenode to be ready by retrying the safemode wait command.
   * The namenode may take some time to start after Docker Compose reports containers as running.
   */
  protected void waitForHdfs() throws Exception {
    final int maxRetries = 12;
    final long retryIntervalMs = 10_000;
    for (int i = 1; i <= maxRetries; i++) {
      try {
        sparkAdhoc1.executeShellCommand("hdfs dfsadmin -safemode wait").expectToSucceed();
        log.info("HDFS namenode is ready");
        return;
      } catch (Throwable e) {
        if (i == maxRetries) {
          throw new RuntimeException("HDFS namenode did not become ready after " + maxRetries + " retries", e);
        }
        log.info("Waiting for HDFS namenode to be ready (attempt {}/{})", i, maxRetries);
        Thread.sleep(retryIntervalMs);
      }
    }
  }

  /**
   * Get a container by service name from the Docker Compose environment.
   * Docker Compose appends _1 suffix to service names.
   * Implements ContainerProvider interface.
   */
  @Override
  public ContainerState getContainer(String serviceName) {
    try {
      return environment.getContainerByServiceName(serviceName)
          .orElseThrow(() -> new IllegalStateException("Container not found: " + serviceName));
    } catch (IllegalStateException e) {
      log.error("Failed to get container: {}", serviceName, e);
      throw e;
    }
  }

  private static String getHudiWorkspace() {
    String projectDir = System.getProperty("user.dir");
    return new File(projectDir, "..").getAbsolutePath();
  }

  private static String getDockerComposeFilePath() {
    String projectDir = System.getProperty("user.dir");
    String os = System.getProperty("os.name").toLowerCase();
    String arch = System.getProperty("os.arch").toLowerCase();
    String composePrefix = System.getProperty(COMPOSE_PREFIX_PROPERTY, DEFAULT_COMPOSE_PREFIX);

    // Determine which compose file to use based on OS and architecture
    boolean isMacArm64 = os.contains("mac") && arch.contains("aarch64");
    String archSuffix = isMacArm64 ? "_arm64" : "_amd64";
    File dockerComposeFile = new File(projectDir, COMPOSE_DIR + composePrefix + archSuffix + ".yml");
    if (!dockerComposeFile.isFile() || !dockerComposeFile.exists()) {
      throw new HoodieException(String.format("%s does not exist", dockerComposeFile.getAbsolutePath()));
    }
    return dockerComposeFile.getAbsolutePath();
  }

  private static String getHadoopEnvFilePath() {
    return new File(System.getProperty("user.dir"), "../docker/compose/hadoop.env").getAbsolutePath();
  }

  /**
   * Reads the original docker-compose file, removes all 'container_name' directives, and returns a temporary file containing the modified content. Including Testcontainers in the docker-compose file
   * will cause ContainerLaunchExceptions to be thrown.
   * <p>
   * Any env_file will normalize/canonicalize to the temporary directory used. This function will make a copy of hadoop.env into the temporary directory to ensure that no error is thrown.
   */
  private static String createProcessedComposeFilePath(String composeFile) {
    try {
      // Read all bytes from the file and convert to a String
      byte[] bytes = Files.readAllBytes(Paths.get(composeFile));
      String originalContent = new String(bytes, StandardCharsets.UTF_8);

      // Use a regular expression to find and remove all lines containing 'container_name'
      String modifiedContent = originalContent.replaceAll("(?m)^\\s*container_name:.*$", "");

      // Create a temporary file to hold our modified configuration
      Path tempDir = Files.createTempDirectory("hudi-test-compose-");

      // Ensure the temporary directory is deleted when the JVM exits
      tempDir.toFile().deleteOnExit();

      // Write the modified content to the temporary file as a byte array.
      File tempDockerComposeFile = new File(tempDir.toFile(), "docker-compose.yml");
      Files.write(tempDockerComposeFile.toPath(), modifiedContent.getBytes(StandardCharsets.UTF_8));

      // tempDir is used as the working directory, docker-compose will look for hadoop.env in the SAME temp directory
      // Copy hadoop.env into the SAME temp directory
      Path destHadoopEnvPath = tempDir.resolve("hadoop.env");
      Path originalHadoopEnvPath = Paths.get(getHadoopEnvFilePath()).toAbsolutePath().normalize();
      Files.copy(originalHadoopEnvPath, destHadoopEnvPath, StandardCopyOption.REPLACE_EXISTING);

      // Return the temporary file for Testcontainers to use
      return tempDockerComposeFile.toPath().toAbsolutePath().toString();
    } catch (IOException e) {
      throw new RuntimeException("Failed to process the docker-compose file", e);
    }
  }
}