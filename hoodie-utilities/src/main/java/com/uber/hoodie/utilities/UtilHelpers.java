/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities;

import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.util.ReflectionUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.Source;
import com.uber.hoodie.utilities.sources.SourceDataFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Bunch of helper methods
 */
public class UtilHelpers {
  private static Logger logger = LogManager.getLogger(UtilHelpers.class);

  public static Source createSource(String sourceClass, PropertiesConfiguration cfg,
      JavaSparkContext jssc, SourceDataFormat dataFormat, SchemaProvider schemaProvider)
      throws IOException {
    try {
      return (Source) ConstructorUtils.invokeConstructor(Class.forName(sourceClass), (Object) cfg,
          (Object) jssc, (Object) dataFormat, (Object) schemaProvider);
    } catch (Throwable e) {
      throw new IOException("Could not load source class " + sourceClass, e);
    }
  }

  public static SchemaProvider createSchemaProvider(String schemaProviderClass,
      PropertiesConfiguration cfg) throws IOException {
    try {
      return (SchemaProvider) ConstructorUtils.invokeConstructor(Class.forName(schemaProviderClass),
          (Object) cfg);
    } catch (Throwable e) {
      throw new IOException("Could not load schema provider class " + schemaProviderClass, e);
    }
  }

  /**
   * TODO: Support hierarchical config files (see CONFIGURATION-609 for sample)
   */
  public static PropertiesConfiguration readConfig(FileSystem fs, Path cfgPath) {
    try {
      FSDataInputStream in = fs.open(cfgPath);
      PropertiesConfiguration config = new PropertiesConfiguration();
      config.load(in);
      in.close();
      return config;
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read config file at :" + cfgPath, e);
    } catch (ConfigurationException e) {
      throw new HoodieDeltaStreamerException("Invalid configs found in config file at :" + cfgPath,
          e);
    }
  }

  /**
   * Parse Schema from file
   *
   * @param fs         File System
   * @param schemaFile Schema File
   */
  public static String parseSchema(FileSystem fs, String schemaFile) throws Exception {
    // Read schema file.
    Path p = new Path(schemaFile);
    if (!fs.exists(p)) {
      throw new Exception(String.format("Could not find - %s - schema file.", schemaFile));
    }
    long len = fs.getFileStatus(p).getLen();
    ByteBuffer buf = ByteBuffer.allocate((int) len);
    FSDataInputStream inputStream = null;
    try {
      inputStream = fs.open(p);
      inputStream.readFully(0, buf.array(), 0, buf.array().length);
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    return new String(buf.array());
  }

  /**
   * Build Spark Context for ingestion/compaction
   * @return
   */
  public static JavaSparkContext buildSparkContext(String tableName, String sparkMaster, String sparkMemory) {
    SparkConf sparkConf = new SparkConf().setAppName("hoodie-data-importer-" + tableName);
    sparkConf.setMaster(sparkMaster);

    if (sparkMaster.startsWith("yarn")) {
      sparkConf.set("spark.eventLog.overwrite", "true");
      sparkConf.set("spark.eventLog.enabled", "true");
    }

    sparkConf.set("spark.driver.maxResultSize", "2g");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.executor.memory", sparkMemory);

    // Configure hadoop conf
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    sparkConf = HoodieWriteClient.registerClasses(sparkConf);
    return new JavaSparkContext(sparkConf);
  }

  /**
   * Build Hoodie write client
   *
   * @param jsc         Java Spark Context
   * @param basePath    Base Path
   * @param schemaStr   Schema
   * @param parallelism Parallelism
   */
  public static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath,
      String schemaStr, int parallelism, Optional<String> compactionStrategyClass) throws Exception {
    HoodieCompactionConfig compactionConfig =
        compactionStrategyClass.map(strategy -> HoodieCompactionConfig.newBuilder().withInlineCompaction(false)
            .withCompactionStrategy(ReflectionUtils.loadClass(strategy))
            .build()).orElse(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build());
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withParallelism(parallelism, parallelism).withSchema(schemaStr)
        .combineInput(true, true)
        .withCompactionConfig(compactionConfig)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
    return new HoodieWriteClient(jsc, config);
  }

  public static int handleErrors(JavaSparkContext jsc, String instantTime, JavaRDD<WriteStatus> writeResponse) {
    Accumulator<Integer> errors = jsc.accumulator(0);
    writeResponse.foreach(writeStatus -> {
      if (writeStatus.hasErrors()) {
        errors.add(1);
        logger.error(String.format("Error processing records :writeStatus:%s",
            writeStatus.getStat().toString()));
      }
    });
    if (errors.value() == 0) {
      logger.info(
          String.format("Dataset imported into hoodie dataset with %s instant time.", instantTime));
      return 0;
    }
    logger.error(String.format("Import failed with %d errors.", errors.value()));
    return -1;
  }
}
