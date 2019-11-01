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

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieWriteClient;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.util.DFSPropertiesConfiguration;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Bunch of helper methods
 */
public class UtilHelpers {
  private static Logger logger = LogManager.getLogger(UtilHelpers.class);

  public static Source createSource(String sourceClass, TypedProperties cfg, JavaSparkContext jssc,
      SparkSession sparkSession, SchemaProvider schemaProvider) throws IOException {
    try {
      return (Source) ReflectionUtils.loadClass(sourceClass,
          new Class<?>[] {TypedProperties.class, JavaSparkContext.class, SparkSession.class, SchemaProvider.class}, cfg,
          jssc, sparkSession, schemaProvider);
    } catch (Throwable e) {
      throw new IOException("Could not load source class " + sourceClass, e);
    }
  }

  public static SchemaProvider createSchemaProvider(String schemaProviderClass, TypedProperties cfg,
      JavaSparkContext jssc) throws IOException {
    try {
      return schemaProviderClass == null ? null
          : (SchemaProvider) ReflectionUtils.loadClass(schemaProviderClass, cfg, jssc);
    } catch (Throwable e) {
      throw new IOException("Could not load schema provider class " + schemaProviderClass, e);
    }
  }

  public static Transformer createTransformer(String transformerClass) throws IOException {
    try {
      return transformerClass == null ? null : (Transformer) ReflectionUtils.loadClass(transformerClass);
    } catch (Throwable e) {
      throw new IOException("Could not load transformer class " + transformerClass, e);
    }
  }

  /**
   */
  public static DFSPropertiesConfiguration readConfig(FileSystem fs, Path cfgPath, List<String> overriddenProps) {
    try {
      DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration(cfgPath.getFileSystem(fs.getConf()), cfgPath);
      if (!overriddenProps.isEmpty()) {
        logger.info("Adding overridden properties to file properties.");
        conf.addProperties(new BufferedReader(new StringReader(String.join("\n", overriddenProps))));
      }
      return conf;
    } catch (Exception e) {
      throw new HoodieException("Unable to read props file at :" + cfgPath, e);
    }
  }

  public static TypedProperties buildProperties(List<String> props) {
    TypedProperties properties = new TypedProperties();
    props.stream().forEach(x -> {
      String[] kv = x.split("=");
      Preconditions.checkArgument(kv.length == 2);
      properties.setProperty(kv[0], kv[1]);
    });
    return properties;
  }

  /**
   * Parse Schema from file
   *
   * @param fs File System
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
    try (FSDataInputStream inputStream = fs.open(p)) {
      inputStream.readFully(0, buf.array(), 0, buf.array().length);
    }
    return new String(buf.array());
  }

  private static SparkConf buildSparkConf(String appName, String defaultMaster) {
    return buildSparkConf(appName, defaultMaster, new HashMap<>());
  }

  private static SparkConf buildSparkConf(String appName, String defaultMaster, Map<String, String> additionalConfigs) {
    final SparkConf sparkConf = new SparkConf().setAppName(appName);
    String master = sparkConf.get("spark.master", defaultMaster);
    sparkConf.setMaster(master);
    if (master.startsWith("yarn")) {
      sparkConf.set("spark.eventLog.overwrite", "true");
      sparkConf.set("spark.eventLog.enabled", "true");
    }
    sparkConf.setIfMissing("spark.driver.maxResultSize", "2g");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    additionalConfigs.entrySet().forEach(e -> sparkConf.set(e.getKey(), e.getValue()));
    SparkConf newSparkConf = HoodieWriteClient.registerClasses(sparkConf);
    return newSparkConf;
  }

  public static JavaSparkContext buildSparkContext(String appName, String defaultMaster, Map<String, String> configs) {
    return new JavaSparkContext(buildSparkConf(appName, defaultMaster, configs));
  }

  public static JavaSparkContext buildSparkContext(String appName, String defaultMaster) {
    return new JavaSparkContext(buildSparkConf(appName, defaultMaster));
  }

  /**
   * Build Spark Context for ingestion/compaction
   * 
   * @return
   */
  public static JavaSparkContext buildSparkContext(String appName, String sparkMaster, String sparkMemory) {
    SparkConf sparkConf = buildSparkConf(appName, sparkMaster);
    sparkConf.set("spark.executor.memory", sparkMemory);
    return new JavaSparkContext(sparkConf);
  }

  /**
   * Build Hoodie write client
   *
   * @param jsc Java Spark Context
   * @param basePath Base Path
   * @param schemaStr Schema
   * @param parallelism Parallelism
   */
  public static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath, String schemaStr,
      int parallelism, Option<String> compactionStrategyClass, TypedProperties properties) throws Exception {
    HoodieCompactionConfig compactionConfig = compactionStrategyClass
        .map(strategy -> HoodieCompactionConfig.newBuilder().withInlineCompaction(false)
            .withCompactionStrategy(ReflectionUtils.loadClass(strategy)).build())
        .orElse(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build());
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withParallelism(parallelism, parallelism)
            .withBulkInsertParallelism(parallelism)
            .withSchema(schemaStr).combineInput(true, true).withCompactionConfig(compactionConfig)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withProps(properties).build();
    return new HoodieWriteClient(jsc, config);
  }

  public static int handleErrors(JavaSparkContext jsc, String instantTime, JavaRDD<WriteStatus> writeResponse) {
    Accumulator<Integer> errors = jsc.accumulator(0);
    writeResponse.foreach(writeStatus -> {
      if (writeStatus.hasErrors()) {
        errors.add(1);
        logger.error(String.format("Error processing records :writeStatus:%s", writeStatus.getStat().toString()));
      }
    });
    if (errors.value() == 0) {
      logger.info(String.format("Dataset imported into hoodie dataset with %s instant time.", instantTime));
      return 0;
    }
    logger.error(String.format("Import failed with %d errors.", errors.value()));
    return -1;
  }

  public static TypedProperties readConfig(InputStream in) throws IOException {
    TypedProperties defaults = new TypedProperties();
    defaults.load(in);
    return defaults;
  }

  public static DFSPathSelector createSourceSelector(String sourceSelectorClass, TypedProperties props,
      Configuration conf) throws IOException {
    try {
      return (DFSPathSelector) ReflectionUtils.loadClass(sourceSelectorClass,
          new Class<?>[]{TypedProperties.class, Configuration.class},
          props, conf);
    } catch (Throwable e) {
      throw new IOException("Could not load source selector class " + sourceSelectorClass, e);
    }
  }
}
