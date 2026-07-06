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

import java.time.Duration;
import java.util.List;

/**
 * Central configuration for the integ2 Testcontainers harness. Every constant that
 * describes "how this harness expects the compose environment to look" belongs here:
 * container service names, container-side paths, network endpoints, timeouts, and
 * the keys / defaults of system properties that tune the harness.
 *
 * <p>Per-test fixture data (table paths, per-test {@code .commands} scripts) does
 * not belong here, those stay in the relevant test class, though they should build
 * their common prefix from {@link Paths#DEMO_DIR} / {@link Paths#WS_ROOT} rather
 * than inlining the path literal.
 */
public final class TestcontainersConfig {

  private TestcontainersConfig() {
  }

  /** Docker Compose service names. Must match the compose YAML verbatim. */
  public static final class Containers {
    public static final String NAMENODE = "namenode";
    public static final String DATANODE_1 = "datanode1";
    public static final String HISTORY_SERVER = "historyserver";
    public static final String HIVE_METASTORE_PGSQL = "hive-metastore-postgresql";
    public static final String HIVE_METASTORE = "hivemetastore";
    public static final String HIVESERVER = "hiveserver";
    public static final String ZOOKEEPER = "zookeeper";
    // TODO: The value intentionally matches SPARK_MASTER, preserved from the
    // pre-refactor source to avoid changing behavior. If the compose YAML has a
    // distinct sparkworker1 service, correct this in a follow-up commit.
    public static final String SPARK_WORKER_1 = "sparkmaster";
    public static final String KAFKA = "kafka";
    public static final String SPARK_MASTER = "sparkmaster";
    // Testcontainers appends the replica index, so the adhoc services resolve as "<name>-1".
    public static final String ADHOC_1 = "adhoc-1-1";
    public static final String ADHOC_2 = "adhoc-2-1";

    private Containers() {
    }
  }

  /** Paths inside the containers (absolute) and the host-side compose directory. */
  public static final class Paths {
    public static final String WS_ROOT = "/var/hoodie/ws";
    public static final String DEMO_DIR = WS_ROOT + "/docker/demo";
    public static final String DEMO_SETUP = DEMO_DIR + "/setup_demo_container.sh";
    public static final String HIVE_TARGET = WS_ROOT + "/docker/hoodie/hadoop/hive_base/target";
    public static final String HADOOP_MR_BUNDLE = HIVE_TARGET + "/hoodie-hadoop-mr-bundle.jar";
    public static final String SPARK_BUNDLE = HIVE_TARGET + "/hoodie-spark-bundle.jar";
    public static final String HADOOP_CONF_DIR = "/etc/hadoop";
    /** Host-side, relative to the hudi-integ-test module working directory. */
    public static final String COMPOSE_DIR = "../docker/compose/";

    private Paths() {
    }
  }

  /** Network endpoints the harness exposes to tests. */
  public static final class Network {
    public static final int HIVE_SERVER_JDBC_PORT = 10000;
    public static final String HIVE_SERVER_JDBC_URL =
        "jdbc:hive2://" + Containers.HIVESERVER + ":" + HIVE_SERVER_JDBC_PORT;
    public static final int SPARK_MASTER_WEB_UI_PORT = 8080;

    private Network() {
    }
  }

  /** Waits and timeouts used by the harness. */
  public static final class Timeouts {
    public static final Duration CONTAINER_STARTUP = Duration.ofMinutes(5);
    public static final int HDFS_MAX_RETRIES = 12;
    public static final Duration HDFS_RETRY_INTERVAL = Duration.ofSeconds(10);

    private Timeouts() {
    }
  }

  /** System-property keys and their defaults (read via {@link System#getProperty}). */
  public static final class SystemProps {
    public static final String COMPOSE_PREFIX = "spark.docker.compose.prefix";
    public static final String DEFAULT_COMPOSE_PREFIX = "docker-compose_hadoop340_hive2310_spark402";
    /** Substring present in compose prefixes that run Spark 4.x (e.g. "...spark402"). */
    public static final String SPARK_4_PREFIX_TOKEN = "spark4";

    /**
     * Flip to {@code true} (e.g. {@code -Dhudi.integ.hive.verbose=true}) to route
     * Hive logs to the console so exception stack traces show up in test output.
     */
    public static final String HIVE_VERBOSE = "hudi.integ.hive.verbose";

    private SystemProps() {
    }
  }

  /**
   * Hiveconf entries applied only when {@link SystemProps#HIVE_VERBOSE} is enabled.
   * See {@code HiveService#execute} for the per-flag rationale.
   */
  public static final List<String> VERBOSE_HIVECONFS =
      List.of("hive.root.logger=INFO,console",
          "hive.exec.mode.local.auto=false",
          "hive.log.explain.output=true",
          "hive.server2.logging.operation.verbose=true");
}
