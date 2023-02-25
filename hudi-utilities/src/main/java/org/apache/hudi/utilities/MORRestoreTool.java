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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 *
 */
public class MORRestoreTool implements Serializable {

  private static final Logger LOG = LogManager.getLogger(MORRestoreTool.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  private final HoodieTableMetaClient metaClient;

  public MORRestoreTool(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public MORRestoreTool(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);

    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .build();
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

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--commitTime", "-c"}, description = "Instant Time to restore to", required = true)
    public String commitTime = "";

    @Parameter(names = {"--dryRun"}, description = "Dry run without deleting any files", required = false)
    public boolean dryRun = true;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--assume-date-partitioning"}, description = "Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path."
        + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually", required = false)
    public Boolean assumeDatePartitioning = false;

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
      return "MorRestoreTool {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --commitTime " + commitTime + ", \n"
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
      Config config = (Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(commitTime, config.commitTime)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(assumeDatePartitioning, config.assumeDatePartitioning)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, commitTime,
          parallelism, sparkMaster, sparkMemory, assumeDatePartitioning, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Mor-Restore", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    MORRestoreTool validator = new MORRestoreTool(jsc, cfg);

    try {
      validator.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to do hoodie metadata table validation for " + validator.cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public boolean run() {
    boolean result = false;
    try {
      LOG.info(cfg);
      LOG.info(" ****** Triggering restore to " + cfg.commitTime + " ******");
      result = doRestore();
    } catch (Exception e) {
      throw new HoodieException("Unable to restore table to " + cfg.commitTime, e);
    } finally {
      return result;
    }
  }

  public boolean doRestore() {
    AtomicBoolean finalResult = new AtomicBoolean(true);
    String basePath = metaClient.getBasePath();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(false).build();
    List<String> partitions = FSUtils.getAllPartitionPaths(engineContext, metadataConfig, basePath);
    SerializableConfiguration serializedConf = new SerializableConfiguration(metaClient.getHadoopConf());

    List<Pair<String, String>> totalFilesToDelete = engineContext.parallelize(partitions)
        .flatMap((SerializableFunction<String, Iterator<Pair<String, String>>>) pPath -> {
          List<Pair<String, String>> filesToDelete = new ArrayList<>();
          HoodieMetadataConfig metadataConfig1 = HoodieMetadataConfig.newBuilder().enable(false).build();
          HoodieTableFileSystemView fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
              new HoodieLocalEngineContext(serializedConf.get()), metaClient, metadataConfig1);
          LOG.info("Processing partition " + pPath);
          List<FileSlice> fileSlices = fileSystemView.getAllFileSlices(pPath).collect(Collectors.toList());
          // process file slices in reverse order
          for (FileSlice fileSlice : fileSlices) {
            LOG.info("File slice commit time " + fileSlice.getBaseInstantTime());
            if (HoodieTimeline.compareTimestamps(fileSlice.getBaseInstantTime(), HoodieTimeline.GREATER_THAN, cfg.commitTime)) {
              LOG.info("Deleting entire file slice ");
              if (fileSlice.getBaseFile().isPresent()) {
                LOG.info("Base file to delete " + fileSlice.getBaseFile().get().getPath());
                filesToDelete.add(Pair.of(pPath, fileSlice.getBaseFile().get().getPath()));
              }
              fileSlice.getLogFiles().forEach(logFile -> {
                LOG.info("   log file to delete " + logFile.getPath().toString());
                filesToDelete.add(Pair.of(pPath, logFile.getPath().toString()));
              });
            } else if (HoodieTimeline.compareTimestamps(fileSlice.getBaseInstantTime(), HoodieTimeline.EQUALS, cfg.commitTime)) {
              LOG.info("Deleting all log files except base file");
              if (fileSlice.getBaseFile().isPresent()) {
                LOG.info("Not deleting Base file " + fileSlice.getBaseFile().get().getPath());
              }
              fileSlice.getLogFiles().forEach(logFile -> {
                LOG.info("   log file to delete " + logFile.getPath().toString());
                filesToDelete.add(Pair.of(pPath, logFile.getPath().toString()));
              });
              LOG.info("Not processing remaining file slices");
              break;
            } else {
              // we need to collect only partial list of log files to be deleted
              /*TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
              try {
                Schema schema = tableSchemaResolver.getTableAvroSchema();
                HoodieLogFormatReader logFormatReaderWrapper = new HoodieLogFormatReader(metaClient.getFs(),
                    fileSlice.getLogFiles().collect(Collectors.toList()),
                    schema, true, true, HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue(), true, HoodieRecord.RECORD_KEY_METADATA_FIELD,
                    InternalSchema.getEmptyInternalSchema());

                while (logFormatReaderWrapper.hasNext()) {
                  HoodieLogFile logFile = logFormatReaderWrapper.getLogFile();
                  LOG.info("Scanning log file " + logFile);
                  HoodieLogBlock logBlock = logFormatReaderWrapper.next();
                  final String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);
                  // process until we hit a log block whose instant time is < commit to restore
                  if (instantTime != null && HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN, cfg.commitTime)) {
                    LOG.info("Hit a log block whose commit time is < commit to restore. Skipping rest of log files " + instantTime);
                    break;
                  } else {
                    // collect all log blocks until we hit a block whose time < commit to restore.
                    filesToDelete.get(pPath).add(logFile.getPath().toString());
                  }
                }
              } catch (Exception e) {
                LOG.error("Failed with an exception ", e);
              } */
            }
          }
          return filesToDelete.iterator();
        }).collectAsList();

    FileSystem fs = metaClient.getFs();
    LOG.info("\n\n================================================================================");
    LOG.info("List of files to delete ");
    if (totalFilesToDelete.size() > 0) {
      totalFilesToDelete.forEach(fileToDelete -> {
        LOG.info("   File to delete " + fileToDelete.getValue());
      });
      if (!cfg.dryRun) {
        int parallelism = Math.max(1, totalFilesToDelete.size() / 100);
        List<Boolean> result = engineContext.parallelize(totalFilesToDelete).repartition(parallelism)
            .map((SerializableFunction<Pair<String, String>, Boolean>) partitionFileToDeletePair
                -> {
              Path pathToDelete = new Path(partitionFileToDeletePair.getValue());
              FileSystem fileSystem = pathToDelete.getFileSystem(serializedConf.get());
              LOG.info("   File to delete " + partitionFileToDeletePair.getValue());
              return fileSystem.delete(new Path(partitionFileToDeletePair.getValue()));
            }).collectAsList();
        result.forEach(entry -> finalResult.set(finalResult.get() && entry));
      }
    }

    if (!cfg.dryRun) {
      LOG.info("Removing timeline files ");
      metaClient.reloadActiveTimeline().getReverseOrderedInstants().collect(Collectors.toList())
          .forEach(instant -> {
            if (HoodieTimeline.compareTimestamps(instant.getTimestamp(), HoodieTimeline.GREATER_THAN, cfg.commitTime)) {
              LOG.info("Deleting timeline file for commit " + instant.getTimestamp());
              try {
                if (instant.isCompleted()) {
                  LOG.info("Deleting all 3 timeline files");
                  fs.delete(new Path(basePath + "/.hoodie/" + instant.getFileName()));
                  String action = instant.getAction();
                  if (instant.getAction().equals("commit")) {
                    action = "compaction";
                  }
                  fs.delete(new Path(basePath + "/.hoodie/" + instant.getTimestamp() + "." + action
                      + HoodieTimeline.INFLIGHT_EXTENSION));
                  fs.delete(new Path(basePath + "/.hoodie/" + instant.getTimestamp() + "." + action
                      + HoodieTimeline.REQUESTED_EXTENSION));
                } else if (instant.isInflight()) {
                  LOG.info("Deleting requested and inflight timeline files");
                  fs.delete(new Path(basePath + "/.hoodie/" + instant.getTimestamp() + "." + instant.getAction()
                      + HoodieTimeline.INFLIGHT_EXTENSION));
                  fs.delete(new Path(basePath + "/.hoodie/" + instant.getTimestamp() + "." + instant.getAction()
                      + HoodieTimeline.REQUESTED_EXTENSION));
                } else {
                  LOG.info("Deleting only requested timeline files");
                  fs.delete(new Path(basePath + "/.hoodie/" + instant.getTimestamp() + "." + instant.getAction()
                      + HoodieTimeline.REQUESTED_EXTENSION));
                }
              } catch (IOException e) {
                LOG.error("Failed to delete timeline file ", e);
              }
            }
          });
    }
    return finalResult.get();
  }
}
