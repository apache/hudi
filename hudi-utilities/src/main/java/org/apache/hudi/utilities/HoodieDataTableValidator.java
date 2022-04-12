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

package org.apache.hudi.utilities;

import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.repair.RepairUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A validator with spark-submit to ensure there are no dangling data files in the data table.
 * No data files found for commits prior to active timeline.
 * No extra data files found for completed commits more than whats present in commit metadata.
 *
 * <p>
 * - Default : This validator will validate the data files only once.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieDataTableValidator \
 * --master spark://xxxx:7077 \
 * --driver-memory 1g \
 * --executor-memory 1g \
 * $HUDI_DIR/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 * --base-path basePath
 * ```
 *
 * <p>
 * Also You can set `--continuous` for long running this validator.
 * And use `--min-validate-interval-seconds` to control the validation frequency, default is 10 minutes.
 * <p>
 * Example command:
 * ```
 * spark-submit \
 * --class org.apache.hudi.utilities.HoodieDataTableValidator \
 * --master spark://xxxx:7077 \
 * --driver-memory 1g \
 * --executor-memory 1g \
 * $HUDI_DIR/hudi/packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.11.0-SNAPSHOT.jar \
 * --base-path basePath
 * --continuous \
 * --min-validate-interval-seconds 60
 * ```
 */
public class HoodieDataTableValidator implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieDataTableValidator.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  private HoodieTableMetaClient metaClient;

  protected transient Option<AsyncDataTableValidateService> asyncDataTableValidateService;

  public HoodieDataTableValidator(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public HoodieDataTableValidator(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);

    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();

    this.asyncDataTableValidateService = cfg.continuous ? Option.of(new AsyncDataTableValidateService()) : Option.empty();
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;

    @Parameter(names = {"--continuous"}, description = "Running MetadataTableValidator in continuous. "
        + "Can use --min-validate-interval-seconds to control validation frequency", required = false)
    public boolean continuous = false;

    @Parameter(names = {"--min-validate-interval-seconds"},
        description = "the min validate interval of each validate when set --continuous, default is 10 minutes.")
    public Integer minValidateIntervalSeconds = 10 * 60;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for validation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--ignore-failed", "-ig"}, description = "Ignore data table validate failure and continue.", required = false)
    public boolean ignoreFailed = false;

    @Parameter(names = {"--assume-date-partitioning"}, description = "Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path."
        + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually", required = false)
    public Boolean assumeDatePartitioning = false;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "MetadataTableValidatorConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --continuous " + continuous + ", \n"
          + "   --ignore-failed " + ignoreFailed + ", \n"
          + "   --min-validate-interval-seconds " + minValidateIntervalSeconds + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --assumeDatePartitioning-memory " + assumeDatePartitioning + ", \n"
          + "   --props " + propsFilePath + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      HoodieMetadataTableValidator.Config config = (HoodieMetadataTableValidator.Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(continuous, config.continuous)
          && Objects.equals(minValidateIntervalSeconds, config.minValidateIntervalSeconds)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(ignoreFailed, config.ignoreFailed)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(assumeDatePartitioning, config.assumeDatePartitioning)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, continuous, minValidateIntervalSeconds, parallelism, ignoreFailed, sparkMaster, sparkMemory,
          assumeDatePartitioning, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Hoodie-Data-Table-Validator", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    HoodieDataTableValidator validator = new HoodieDataTableValidator(jsc, cfg);

    try {
      validator.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to do hoodie Data table validation for " + validator.cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg);
      if (cfg.continuous) {
        LOG.info(" ****** do hoodie data table validation in CONTINUOUS mode ******");
        doHoodieDataTableValidationContinuous();
      } else {
        LOG.info(" ****** do hoodie data table validation once ******");
        doHoodieDataTableValidationOnce();
      }
    } catch (Exception e) {
      throw new HoodieException("Unable to do hoodie data table validation in " + cfg.basePath, e);
    } finally {

      if (asyncDataTableValidateService.isPresent()) {
        asyncDataTableValidateService.get().shutdown(true);
      }
    }
  }

  private void doHoodieDataTableValidationOnce() {
    try {
      doDataTableValidation();
    } catch (HoodieValidationException e) {
      LOG.error("Metadata table validation failed to HoodieValidationException", e);
      if (!cfg.ignoreFailed) {
        throw e;
      }
    }
  }

  private void doHoodieDataTableValidationContinuous() {
    asyncDataTableValidateService.ifPresent(service -> {
      service.start(null);
      try {
        service.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException(e.getMessage(), e);
      }
    });
  }

  public void doDataTableValidation() {
    boolean finalResult = true;
    metaClient.reloadActiveTimeline();
    String basePath = metaClient.getBasePath();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    try {
      HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(
          engineContext, engineContext.getHadoopConf(), cfg.basePath, cfg.assumeDatePartitioning);
      List<Path> allDataFilePaths = HoodieDataTableUtils.getBaseAndLogFilePathsFromFileSystem(tableMetadata, cfg.basePath);
      // verify that no data files present with commit time < earliest commit in active timeline.
      if (metaClient.getActiveTimeline().firstInstant().isPresent()) {
        String earliestInstant = metaClient.getActiveTimeline().firstInstant().get().getTimestamp();
        List<Path> danglingFilePaths = allDataFilePaths.stream().filter(path -> {
          String instantTime = FSUtils.getCommitTime(path.getName());
          return HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN, earliestInstant);
        }).collect(Collectors.toList());

        if (!danglingFilePaths.isEmpty() && danglingFilePaths.size() > 0) {
          LOG.error("Data table validation failed due to dangling files count " + danglingFilePaths.size() + ", found before active timeline");
          danglingFilePaths.forEach(entry -> LOG.error("Dangling file: " + entry.toString()));
          finalResult = false;
          if (!cfg.ignoreFailed) {
            throw new HoodieValidationException("Data table validation failed due to dangling files " + danglingFilePaths.size());
          }
        }

        // Verify that for every completed commit in active timeline, there are no extra files found apart from what is present in
        // commit metadata.
        Map<String, List<String>> instantToFilesMap = RepairUtils.tagInstantsOfBaseAndLogFiles(
            metaClient.getBasePath(), allDataFilePaths);
        HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
        List<HoodieInstant> hoodieInstants = activeTimeline.filterCompletedInstants().getInstants().collect(Collectors.toList());

        List<String> danglingFiles = engineContext.flatMap(hoodieInstants, instant -> {
          Option<Set<String>> filesFromTimeline = RepairUtils.getBaseAndLogFilePathsFromTimeline(
              activeTimeline, instant);
          List<String> baseAndLogFilesFromFs = instantToFilesMap.containsKey(instant.getTimestamp()) ? instantToFilesMap.get(instant.getTimestamp())
              : Collections.emptyList();
          if (!baseAndLogFilesFromFs.isEmpty()) {
            Set<String> danglingInstantFiles = new HashSet<>(baseAndLogFilesFromFs);
            if (filesFromTimeline.isPresent()) {
              danglingInstantFiles.removeAll(filesFromTimeline.get());
            }
            return new ArrayList<>(danglingInstantFiles).stream();
          } else {
            return Stream.empty();
          }
        }, hoodieInstants.size()).stream().collect(Collectors.toList());

        if (!danglingFiles.isEmpty()) {
          LOG.error("Data table validation failed due to extra files found for completed commits " + danglingFiles.size());
          danglingFiles.forEach(entry -> LOG.error("Dangling file: " + entry.toString()));
          finalResult = false;
          if (!cfg.ignoreFailed) {
            throw new HoodieValidationException("Data table validation failed due to dangling files " + danglingFiles.size());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Data table validation failed due to " + e.getMessage(), e);
      if (!cfg.ignoreFailed) {
        throw new HoodieValidationException("Data table validation failed due to " + e.getMessage(), e);
      }
    }

    if (finalResult) {
      LOG.info("Data table validation succeeded.");
    } else {
      LOG.warn("Data table validation failed.");
    }
  }

  public class AsyncDataTableValidateService extends HoodieAsyncService {
    private final transient ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        while (true) {
          try {
            long start = System.currentTimeMillis();
            doDataTableValidation();
            long toSleepMs = cfg.minValidateIntervalSeconds * 1000 - (System.currentTimeMillis() - start);

            if (toSleepMs > 0) {
              LOG.info("Last validate ran less than min validate interval: " + cfg.minValidateIntervalSeconds + " s, sleep: "
                  + toSleepMs + " ms.");
              Thread.sleep(toSleepMs);
            }
          } catch (HoodieValidationException e) {
            LOG.error("Shutting down AsyncDataTableValidateService due to HoodieValidationException", e);
            if (!cfg.ignoreFailed) {
              throw e;
            }
          } catch (InterruptedException e) {
            // ignore InterruptedException here.
          }
        }
      }, executor), executor);
    }
  }
}