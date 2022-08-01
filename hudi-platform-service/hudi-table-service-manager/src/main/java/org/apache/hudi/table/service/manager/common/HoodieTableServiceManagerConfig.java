package org.apache.hudi.table.service.manager.common;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Hoodie Table Service Manager Configs.
 */
@ConfigClassProperty(name = "Hoodie table service manager configs",
    groupName = ConfigGroups.Names.PLATFORM_SERVICE,
    description = "Configure the execution parameters of the table service manager, submit job resources.")
public class HoodieTableServiceManagerConfig extends HoodieConfig {
  private HoodieTableServiceManagerConfig() {
    super();
  }

  public static final ConfigProperty<Long> SCHEDULE_INTERVAL_MS = ConfigProperty
      .key("schedule.interval.ms")
      .defaultValue(30000L)
      .sinceVersion("0.13.0")
      .withDocumentation("");

  public static final ConfigProperty<Integer> SCHEDULE_CORE_EXECUTE_SIZE = ConfigProperty
      .key("schedule.core.executor.size")
      .defaultValue(300)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> SCHEDULE_MAX_EXECUTE_SIZE = ConfigProperty
      .key("schedule.max.executor.size")
      .defaultValue(1000)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<String> METADATA_STORE_CLASS = ConfigProperty
      .key("metadata.store.class")
      .defaultValue("org.apache.hudi.table.service.manager.store.impl.RelationDBBasedStore")
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Boolean> INSTANCE_CACHE_ENABLE = ConfigProperty
      .key("instance.cache.enable")
      .defaultValue(true)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> INSTANCE_MAX_RETRY_NUM = ConfigProperty
      .key("instance.max.retry.num")
      .defaultValue(3)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> INSTANCE_SUBMIT_TIMEOUT_SEC = ConfigProperty
      .key("instance.submit.timeout.seconds")
      .defaultValue(600)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  /**
   * Spark Submit Config.
   */

  public static final ConfigProperty<String> SPARK_SUBMIT_JAR_PATH = ConfigProperty
      .key("spark.submit.jar.path")
      .noDefaultValue()
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> SPARK_PARALLELISM = ConfigProperty
      .key("spark.parallelism")
      .defaultValue(100)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<String> SPARK_MASTER = ConfigProperty
      .key("spark.master")
      .defaultValue("yarn")
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<String> SPARK_EXECUTOR_MEMORY = ConfigProperty
      .key("spark.executor.memory")
      .defaultValue("4g")
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<String> SPARK_DRIVER_MEMORY = ConfigProperty
      .key("spark.driver.memory")
      .defaultValue("2g")
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<String> SPARK_EXECUTOR_MEMORY_OVERHEAD = ConfigProperty
      .key("spark.executor.memory.overhead")
      .defaultValue("200m")
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> SPARK_EXECUTOR_CORES = ConfigProperty
      .key("spark.executor.cores")
      .defaultValue(1)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> SPARK_MIN_EXECUTORS = ConfigProperty
      .key("spark.min.executors")
      .defaultValue(1)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public static final ConfigProperty<Integer> SPARK_MAX_EXECUTORS = ConfigProperty
      .key("spark.max.executors")
      .defaultValue(100)
      .sinceVersion("0.13.0")
      .withDocumentation("AWS session token");

  public Long getScheduleIntervalMs() {
    return getLong(SCHEDULE_INTERVAL_MS);
  }

  public int getScheduleCoreExecuteSize() {
    return getInt(SCHEDULE_CORE_EXECUTE_SIZE);
  }

  public int getScheduleMaxExecuteSize() {
    return getInt(SCHEDULE_MAX_EXECUTE_SIZE);
  }

  public String getMetadataStoreClass() {
    return getString(METADATA_STORE_CLASS);
  }

  public boolean getInstanceCacheEnable() {
    return getBoolean(INSTANCE_CACHE_ENABLE);
  }

  public int getInstanceMaxRetryNum() {
    return getInt(INSTANCE_MAX_RETRY_NUM);
  }

  public int getInstanceSubmitTimeoutSec() {
    return getInt(INSTANCE_SUBMIT_TIMEOUT_SEC);
  }

  public String getSparkSubmitJarPath() {
    return getString(SPARK_SUBMIT_JAR_PATH);
  }

  public int getSparkParallelism() {
    return getInt(SPARK_PARALLELISM);
  }

  public String getSparkMaster() {
    return getString(SPARK_MASTER);
  }

  public String getSparkExecutorMemory() {
    return getString(SPARK_EXECUTOR_MEMORY);
  }

  public String getSparkDriverMemory() {
    return getString(SPARK_DRIVER_MEMORY);
  }

  public String getSparkExecutorMemoryOverhead() {
    return getString(SPARK_EXECUTOR_MEMORY_OVERHEAD);
  }

  public int getSparkExecutorCores() {
    return getInt(SPARK_EXECUTOR_CORES);
  }

  public int getSparkMinExecutors() {
    return getInt(SPARK_MIN_EXECUTORS);
  }

  public int getSparkMaxExecutors() {
    return getInt(SPARK_MAX_EXECUTORS);
  }

  public static HoodieTableServiceManagerConfig.Builder newBuilder() {
    return new HoodieTableServiceManagerConfig.Builder();
  }

  public static class Builder {

    private final HoodieTableServiceManagerConfig tableServiceManagerConfig = new HoodieTableServiceManagerConfig();

    public HoodieTableServiceManagerConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.tableServiceManagerConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieTableServiceManagerConfig.Builder fromProperties(Properties props) {
      this.tableServiceManagerConfig.getProps().putAll(props);
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withScheduleIntervalMs(long scheduleIntervalMs) {
      tableServiceManagerConfig.setValue(SCHEDULE_INTERVAL_MS, String.valueOf(scheduleIntervalMs));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withScheduleCoreExecuteSize(int scheduleCoreExecuteSize) {
      tableServiceManagerConfig.setValue(SCHEDULE_CORE_EXECUTE_SIZE, String.valueOf(scheduleCoreExecuteSize));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withScheduleMaxExecuteSize(int scheduleMaxExecuteSize) {
      tableServiceManagerConfig.setValue(SCHEDULE_MAX_EXECUTE_SIZE, String.valueOf(scheduleMaxExecuteSize));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withMetadataStoreClass(String metadataStoreClass) {
      tableServiceManagerConfig.setValue(METADATA_STORE_CLASS, metadataStoreClass);
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withInstanceCacheEnable(boolean instanceCacheEnable) {
      tableServiceManagerConfig.setValue(INSTANCE_CACHE_ENABLE, String.valueOf(instanceCacheEnable));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withInstanceMaxRetryNum(int instanceMaxRetryNum) {
      tableServiceManagerConfig.setValue(INSTANCE_MAX_RETRY_NUM, String.valueOf(instanceMaxRetryNum));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withInstanceSubmitTimeoutSec(int instanceSubmitTimeoutSec) {
      tableServiceManagerConfig.setValue(INSTANCE_SUBMIT_TIMEOUT_SEC, String.valueOf(instanceSubmitTimeoutSec));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkSubmitJarPath(String sparkSubmitJarPath) {
      tableServiceManagerConfig.setValue(SPARK_SUBMIT_JAR_PATH, String.valueOf(sparkSubmitJarPath));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkParallelism(int sparkParallelism) {
      tableServiceManagerConfig.setValue(SPARK_PARALLELISM, String.valueOf(sparkParallelism));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkMaster(String sparkMaster) {
      tableServiceManagerConfig.setValue(SPARK_MASTER, sparkMaster);
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkExecutorMemory(String sparkExecutorMemory) {
      tableServiceManagerConfig.setValue(SPARK_EXECUTOR_MEMORY, sparkExecutorMemory);
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkDriverMemory(String sparkDriverMemory) {
      tableServiceManagerConfig.setValue(SPARK_DRIVER_MEMORY, sparkDriverMemory);
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkExecutorMemoryOverhead(String sparkExecutorMemoryOverhead) {
      tableServiceManagerConfig.setValue(SPARK_EXECUTOR_MEMORY_OVERHEAD, sparkExecutorMemoryOverhead);
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkExecutorCores(int sparkExecutorCores) {
      tableServiceManagerConfig.setValue(SPARK_EXECUTOR_CORES, String.valueOf(sparkExecutorCores));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkMinExecutors(int sparkMinExecutors) {
      tableServiceManagerConfig.setValue(SPARK_MIN_EXECUTORS, String.valueOf(sparkMinExecutors));
      return this;
    }

    public HoodieTableServiceManagerConfig.Builder withSparkMaxExecutors(int sparkMaxExecutors) {
      tableServiceManagerConfig.setValue(SPARK_MAX_EXECUTORS, String.valueOf(sparkMaxExecutors));
      return this;
    }

    public HoodieTableServiceManagerConfig build() {
      tableServiceManagerConfig.setDefaults(HoodieTableServiceManagerConfig.class.getName());
      return tableServiceManagerConfig;
    }
  }
}
