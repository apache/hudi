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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TableSizeUtil implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TableSizeUtil.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public TableSizeUtil(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
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
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = false)
    public String basePath = null;

    @Parameter(names = {"--props-path", "-pp"}, description = "Properties file containing base paths one per line", required = false)
    public String propsFilePath = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 200;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

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
          + "   --parallelism " + parallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
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
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, parallelism, sparkMaster, sparkMemory, propsFilePath, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Hoodie-Metadata-Table-Validator", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      TableSizeUtil validator = new TableSizeUtil(jsc, cfg);
      validator.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s]. "
          + "Skipping the validation of the metadata table.", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Fail to do hoodie metadata table validation for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.info(" ****** Fetching total table size ******");
      if (cfg.propsFilePath != null) {
        List<String> filePaths = getFilePaths(cfg.propsFilePath, jsc.hadoopConfiguration());
        for (String filePath : filePaths) {
          getTableSize(filePath);
        }
      } else {
        if (cfg.basePath == null) {
          throw new HoodieIOException("Base path needs to be set");
        }
        getTableSize(cfg.basePath);
      }

    } catch (Exception e) {
      throw new HoodieException("Unable to do fetch total table size " + cfg.basePath, e);
    }
  }

  private List<String> getFilePaths(String propsPath, Configuration hadoopConf) {
    List<String> filePaths = new ArrayList<>();
    FileSystem fs = FSUtils.getFs(
        propsPath,
        Option.ofNullable(hadoopConf).orElseGet(Configuration::new)
    );

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(propsPath))))) {
      String line = reader.readLine();
      while (line != null) {
        filePaths.add(line);
        line = reader.readLine();
      }
    } catch (IOException ioe) {
      LOG.error("Error reading in properties from dfs from file " + propsPath);
      throw new HoodieIOException("Cannot read properties from dfs from file " + propsPath, ioe);
    }
    return filePaths;
  }

  private long getTableSize(String basePath) throws IOException {
    LOG.warn("Processing table " + basePath);
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(false)
        .build();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, basePath,
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue());
    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(jsc.hadoopConfiguration());

    List<String> allPartitions = tableMetadata.getAllPartitionPaths();
    List<Long> fileSizes = engineContext.parallelize(allPartitions, cfg.parallelism).map((SerializableFunction<String, Long>) partition -> {
      HoodieTableMetaClient metaClientLocal = HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(serializableConfiguration.get()).build();
      HoodieMetadataConfig metadataConfig1 = HoodieMetadataConfig.newBuilder()
          .enable(false)
          .build();
      HoodieTableFileSystemView fileSystemView = FileSystemViewManager
          .createInMemoryFileSystemView(new HoodieLocalEngineContext(serializableConfiguration.get()),
              metaClientLocal, metadataConfig1);
      List<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(partition).collect(Collectors.toList());
      AtomicLong totalSize = new AtomicLong(0);
      baseFiles.forEach(baseFile -> totalSize.addAndGet(baseFile.getFileSize()));
      LOG.info("Total size for partition " + partition + " = " + totalSize.get());
      return totalSize.get();
    }).collectAsList();

    AtomicLong totalTableSize = new AtomicLong(0);
    fileSizes.forEach(fileSize -> totalTableSize.addAndGet(fileSize));
    LOG.warn("Total size for " + basePath + " : " + totalTableSize.get() + ", in GB " + ((totalTableSize.get() * 1.0) / 1024 / 1024 / 1024));
    return totalTableSize.get();
  }
}
