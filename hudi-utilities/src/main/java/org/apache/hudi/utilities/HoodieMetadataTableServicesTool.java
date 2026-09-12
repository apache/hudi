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

package org.apache.hudi.utilities;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataTableServiceMode;
import org.apache.hudi.metadata.MetadataTableServiceRequest;
import org.apache.hudi.metadata.SparkMetadataWriterFactory;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Standalone Spark tool for scheduling and executing metadata table services directly through MDT writer APIs.
 * This tool does not require an HTTP table-service-manager server. Users must submit and schedule the job
 * separately; enabling ingestion-side metadata table service delegation does not launch it automatically.
 */
@Slf4j
public class HoodieMetadataTableServicesTool {

  private final Config cfg;
  private final TypedProperties props;
  private final HoodieSparkEngineContext engineContext;
  private final HadoopStorageConfiguration storageConf;
  private final HoodieTableMetaClient dataMetaClient;
  private final Set<TableServiceType> services;
  private final MetadataTableServiceMode mode;

  public HoodieMetadataTableServicesTool(Config cfg, JavaSparkContext jsc) {
    this(cfg, jsc, UtilHelpers.buildProperties(jsc.hadoopConfiguration(), cfg.propsFilePath, cfg.configs));
  }

  HoodieMetadataTableServicesTool(Config cfg, JavaSparkContext jsc, TypedProperties props) {
    this.cfg = cfg;
    this.props = props;
    this.engineContext = new HoodieSparkEngineContext(jsc);
    this.storageConf = new HadoopStorageConfiguration(jsc.hadoopConfiguration());
    this.dataMetaClient = HoodieTableMetaClient.builder()
        .setConf(storageConf)
        .setBasePath(cfg.basePath)
        .build();
    this.services = parseServices(cfg.services);
    this.mode = MetadataTableServiceMode.fromValue(cfg.mode);
    validateRequest(mode, services, cfg.instantTime);
  }

  public void run() {
    dataMetaClient.reloadTableConfig();
    if (!dataMetaClient.getTableConfig().isMetadataTableAvailable()) {
      log.warn("Metadata table is not initialized for data table {}, skipping table services", cfg.basePath);
      return;
    }

    Set<TableServiceType> compactionServices = services.stream()
        .filter(service -> service == TableServiceType.COMPACT || service == TableServiceType.LOG_COMPACT)
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(TableServiceType.class)));

    validateDataTableLockConfiguration(buildWriteConfig(WriteConcurrencyMode.SINGLE_WRITER));

    // Finish plans left pending by a previous run before cleaning or publishing new plans.
    if (mode.includesExecute() && !compactionServices.isEmpty()) {
      executeTableServicesPhase(compactionServices);
    }

    // Execute clean with the OCC writer so its own transaction manager controls the required lock scope.
    if (mode.includesExecute() && services.contains(TableServiceType.CLEAN)) {
      executeTableServicesPhase(EnumSet.of(TableServiceType.CLEAN));
    }

    if (mode.includesSchedule() && !compactionServices.isEmpty()) {
      if (mode == MetadataTableServiceMode.SCHEDULE_AND_EXECUTE) {
        // Preserve inline ordering: fully process compaction before log compaction.
        scheduleAndExecuteCompactionService(TableServiceType.COMPACT, compactionServices);
        scheduleAndExecuteCompactionService(TableServiceType.LOG_COMPACT, compactionServices);
      } else {
        // Pure scheduling only publishes plans while holding the data-table lock.
        scheduleTableServicesPhase(compactionServices);
      }
    }

    // Archive after all requested compaction services have completed.
    if (mode.includesExecute() && services.contains(TableServiceType.ARCHIVE)) {
      executeTableServicesPhase(EnumSet.of(TableServiceType.ARCHIVE));
    }
  }

  private void scheduleAndExecuteCompactionService(TableServiceType service,
                                                   Set<TableServiceType> requestedServices) {
    if (!requestedServices.contains(service)) {
      return;
    }
    Set<TableServiceType> serviceSet = EnumSet.of(service);
    // Publish the plan under the data-table lock, then release it before expensive OCC execution.
    scheduleTableServicesPhase(serviceSet);
    executeTableServicesPhase(serviceSet);
  }

  private void executeTableServicesPhase(Set<TableServiceType> executionServices) {
    // Let the OCC writer's transaction manager acquire the shared data-table logical lock only where required.
    // With no explicit instant, compaction services execute every pending plan for the requested service types.
    HoodieWriteConfig executionConfig = buildWriteConfig(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    try (HoodieTableMetadataWriter writer = createWriter(executionConfig)) {
      writer.executeTableServices(newRequest(MetadataTableServiceMode.EXECUTE, executionServices));
    } catch (Exception e) {
      throw new HoodieException("Failed to execute metadata table services " + executionServices, e);
    }
  }

  private void scheduleTableServicesPhase(Set<TableServiceType> schedulingServices) {
    // The outer transaction owns the shared data-table lock; the SINGLE_WRITER MDT writer must not reacquire it.
    HoodieWriteConfig schedulingConfig = buildWriteConfig(WriteConcurrencyMode.SINGLE_WRITER);
    try (TransactionManager transactionManager = createTransactionManager(schedulingConfig)) {
      transactionManager.beginStateChange(Option.empty(), Option.empty());
      // Close the writer before releasing the lock, preserving any primary failure if either close fails.
      try (AutoCloseable lock = () -> transactionManager.endStateChange(Option.empty());
           HoodieTableMetadataWriter writer = createWriter(schedulingConfig)) {
        writer.scheduleTableServices(newRequest(MetadataTableServiceMode.SCHEDULE, schedulingServices));
      } catch (Exception e) {
        throw new HoodieException("Failed to run lock-protected metadata table services", e);
      }
    }
  }

  TransactionManager createTransactionManager(HoodieWriteConfig writeConfig) {
    return new TransactionManager(writeConfig, dataMetaClient.getStorage());
  }

  HoodieTableMetadataWriter createWriter(HoodieWriteConfig writeConfig) {
    HoodieTableMetadataWriter writer = SparkMetadataWriterFactory.create(
        storageConf, writeConfig, engineContext, Option.empty(), dataMetaClient.getTableConfig());
    if (!writer.isInitialized()) {
      try {
        writer.close();
      } catch (Exception e) {
        log.warn("Failed to close an uninitialized metadata table writer", e);
      }
      throw new HoodieException("Metadata table writer could not be initialized for " + cfg.basePath);
    }
    return writer;
  }

  HoodieWriteConfig buildWriteConfig(WriteConcurrencyMode metadataConcurrencyMode) {
    Properties profileProps = new Properties();
    profileProps.putAll(props);
    profileProps.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
    profileProps.setProperty(HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key(), "false");
    // A standalone table-service job must fail visibly so that the scheduler can retry or alert.
    profileProps.setProperty(HoodieMetadataConfig.FAIL_ON_TABLE_SERVICE_FAILURES.key(), "true");
    profileProps.setProperty(HoodieMetadataConfig.METADATA_WRITE_CONCURRENCY_MODE.key(), metadataConcurrencyMode.name());
    // The tool is the table-service manager and must not delegate its own requests.
    profileProps.setProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_ENABLED.key(), "false");
    profileProps.setProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(), "");
    profileProps.setProperty(HoodieMetadataConfig.TABLE_SERVICE_MANAGER_SCHEDULE_ACTIONS.key(), "");
    return HoodieWriteConfig.newBuilder()
        .combineInput(true, true)
        .withPath(cfg.basePath)
        .forTable(dataMetaClient.getTableConfig().getTableName())
        .withProps(profileProps)
        .build();
  }

  private MetadataTableServiceRequest newRequest(MetadataTableServiceMode requestMode,
                                                 Set<TableServiceType> requestServices) {
    return MetadataTableServiceRequest.newBuilder()
        .withMode(requestMode)
        .withServices(requestServices)
        .withInstantTime(Option.ofNullable(cfg.instantTime))
        .disableTableServiceManagerDelegation(true)
        .build();
  }

  static void validateDataTableLockConfiguration(HoodieWriteConfig writeConfig) {
    if (!writeConfig.getWriteConcurrencyMode().isOptimisticConcurrencyControl()) {
      throw new HoodieException("Running metadata table services concurrently requires the data table write concurrency mode to be "
          + WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
    }
    String lockProviderClass = writeConfig.getLockProviderClass();
    if (lockProviderClass == null) {
      throw new HoodieException("A distributed data table lock provider is required to run metadata table services concurrently");
    }
    // Validate that an OCC MDT writer can derive a lock configuration sharing the data-table logical lock.
    HoodieLockConfig.deriveLockConfigForDifferentTable(lockProviderClass, writeConfig);
  }

  static void validateRequest(MetadataTableServiceMode mode,
                              Set<TableServiceType> services,
                              String instantTime) {
    MetadataTableServiceRequest request = MetadataTableServiceRequest.newBuilder()
        .withMode(mode)
        .withServices(services)
        .withInstantTime(Option.ofNullable(instantTime))
        .build();
    boolean containsCompactionService = request.includes(TableServiceType.COMPACT)
        || request.includes(TableServiceType.LOG_COMPACT);
    if (mode == MetadataTableServiceMode.SCHEDULE && !containsCompactionService) {
      throw new IllegalArgumentException("Schedule mode requires compaction or logcompaction");
    }
    if (mode == MetadataTableServiceMode.SCHEDULE) {
      Set<TableServiceType> skippedServices = EnumSet.copyOf(services);
      skippedServices.retainAll(EnumSet.of(TableServiceType.CLEAN, TableServiceType.ARCHIVE));
      if (!skippedServices.isEmpty()) {
        log.warn("Skipping services {} in schedule-only mode; clean and archive require execute or schedule-and-execute mode",
            skippedServices);
      }
    }
  }

  static Set<TableServiceType> parseServices(String value) {
    if (value == null || value.trim().isEmpty() || "all".equalsIgnoreCase(value.trim())) {
      return EnumSet.of(TableServiceType.COMPACT, TableServiceType.LOG_COMPACT,
          TableServiceType.CLEAN, TableServiceType.ARCHIVE);
    }
    EnumSet<TableServiceType> result = EnumSet.noneOf(TableServiceType.class);
    for (String service : value.split(",")) {
      switch (service.trim().toLowerCase(Locale.ROOT).replace("-", "")) {
        case "compaction":
          result.add(TableServiceType.COMPACT);
          break;
        case "logcompaction":
          result.add(TableServiceType.LOG_COMPACT);
          break;
        case "clean":
          result.add(TableServiceType.CLEAN);
          break;
        case "archive":
          result.add(TableServiceType.ARCHIVE);
          break;
        default:
          throw new IllegalArgumentException("Unsupported metadata table service: " + service);
      }
    }
    return result;
  }

  /** CLI configuration. */
  public static class Config implements Serializable {

    @Parameter(names = {"--base-path"}, description = "Base path of the data table", required = true)
    public String basePath;

    @Parameter(names = {"--services"}, description = "Comma-separated services: compaction,logcompaction,clean,archive")
    public String services = "all";

    @Parameter(names = {"--mode"}, description = "schedule, execute, or schedule-and-execute")
    public String mode = "schedule-and-execute";

    @Parameter(names = {"--instant-time"}, description = "Optional compaction or log-compaction instant to execute")
    public String instantTime;

    @Parameter(names = {"--props"}, description = "Path to a properties file containing write and lock configurations")
    public String propsFilePath;

    @Parameter(names = {"--hoodie-conf"}, description = "Additional Hoodie configuration in key=value form",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--spark-master"}, description = "Spark master")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--enable-hive-support", "-ehs"}, description = "Enable Hive support")
    public Boolean enableHiveSupport = false;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) {
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      return;
    }

    String tableName = new Path(cfg.basePath).getName();
    JavaSparkContext jsc = UtilHelpers.buildSparkContext(
        "hoodie-metadata-table-services-" + tableName, cfg.sparkMaster, cfg.enableHiveSupport);
    int exitCode = 0;
    try {
      new HoodieMetadataTableServicesTool(cfg, jsc).run();
    } catch (Throwable throwable) {
      exitCode = 1;
      throw new HoodieException("Failed to run metadata table services for " + cfg.basePath, throwable);
    } finally {
      SparkAdapterSupport$.MODULE$.sparkAdapter().stopSparkContext(jsc, exitCode);
    }
    log.info("Metadata table services completed successfully");
  }
}
