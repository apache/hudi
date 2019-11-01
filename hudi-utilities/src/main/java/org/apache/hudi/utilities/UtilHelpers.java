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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utilities.checkpointing.InitialCheckPointProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry;
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Bunch of helper methods.
 */
public class UtilHelpers {
  private static final Logger LOG = LogManager.getLogger(UtilHelpers.class);

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

  public static Option<Transformer> createTransformer(List<String> classNames) throws IOException {
    try {
      List<Transformer> transformers = new ArrayList<>();
      for (String className : Option.ofNullable(classNames).orElse(Collections.emptyList())) {
        transformers.add(ReflectionUtils.loadClass(className));
      }
      return transformers.isEmpty() ? Option.empty() : Option.of(new ChainedTransformer(transformers));
    } catch (Throwable e) {
      throw new IOException("Could not load transformer class(es) " + classNames, e);
    }
  }

  public static InitialCheckPointProvider createInitialCheckpointProvider(
      String className, TypedProperties props) throws IOException {
    try {
      return (InitialCheckPointProvider) ReflectionUtils.loadClass(className, new Class<?>[] {TypedProperties.class}, props);
    } catch (Throwable e) {
      throw new IOException("Could not load initial checkpoint provider class " + className, e);
    }
  }

  /**
   *
   */
  public static DFSPropertiesConfiguration readConfig(FileSystem fs, Path cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf;
    try {
      conf = new DFSPropertiesConfiguration(cfgPath.getFileSystem(fs.getConf()), cfgPath);
    } catch (Exception e) {
      conf = new DFSPropertiesConfiguration();
      LOG.warn("Unexpected error read props file at :" + cfgPath, e);
    }

    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addProperties(new BufferedReader(new StringReader(String.join("\n", overriddenProps))));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  public static TypedProperties buildProperties(List<String> props) {
    TypedProperties properties = new TypedProperties();
    props.forEach(x -> {
      String[] kv = x.split("=");
      ValidationUtils.checkArgument(kv.length == 2);
      properties.setProperty(kv[0], kv[1]);
    });
    return properties;
  }

  public static void validateAndAddProperties(String[] configs, SparkLauncher sparkLauncher) {
    Arrays.stream(configs).filter(config -> config.contains("=") && config.split("=").length == 2).forEach(sparkLauncher::addAppArgs);
  }

  /**
   * Parse Schema from file.
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

    additionalConfigs.forEach(sparkConf::set);
    return HoodieWriteClient.registerClasses(sparkConf);
  }

  public static JavaSparkContext buildSparkContext(String appName, String defaultMaster, Map<String, String> configs) {
    return new JavaSparkContext(buildSparkConf(appName, defaultMaster, configs));
  }

  public static JavaSparkContext buildSparkContext(String appName, String defaultMaster) {
    return new JavaSparkContext(buildSparkConf(appName, defaultMaster));
  }

  /**
   * Build Spark Context for ingestion/compaction.
   *
   * @return
   */
  public static JavaSparkContext buildSparkContext(String appName, String sparkMaster, String sparkMemory) {
    SparkConf sparkConf = buildSparkConf(appName, sparkMaster);
    sparkConf.set("spark.executor.memory", sparkMemory);
    return new JavaSparkContext(sparkConf);
  }

  /**
   * Build Hoodie write client.
   *
   * @param jsc         Java Spark Context
   * @param basePath    Base Path
   * @param schemaStr   Schema
   * @param parallelism Parallelism
   */
  public static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath, String schemaStr,
                                                     int parallelism, Option<String> compactionStrategyClass, TypedProperties properties) {
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
        LOG.error(String.format("Error processing records :writeStatus:%s", writeStatus.getStat().toString()));
      }
    });
    if (errors.value() == 0) {
      LOG.info(String.format("Table imported into hoodie with %s instant time.", instantTime));
      return 0;
    }
    LOG.error(String.format("Import failed with %d errors.", errors.value()));
    return -1;
  }

  /**
   * Returns a factory for creating connections to the given JDBC URL.
   *
   * @param options - JDBC options that contains url, table and other information.
   * @return
   * @throws SQLException if the driver could not open a JDBC connection.
   */
  private static Connection createConnectionFactory(Map<String, String> options) throws SQLException {
    String driverClass = options.get(JDBCOptions.JDBC_DRIVER_CLASS());
    DriverRegistry.register(driverClass);
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    Driver driver = null;
    while (drivers.hasMoreElements()) {
      Driver d = drivers.nextElement();
      if (d instanceof DriverWrapper) {
        if (((DriverWrapper) d).wrapped().getClass().getCanonicalName().equals(driverClass)) {
          driver = d;
        }
      } else if (d.getClass().getCanonicalName().equals(driverClass)) {
        driver = d;
      }
      if (driver != null) {
        break;
      }
    }

    Objects.requireNonNull(driver, String.format("Did not find registered driver with class %s", driverClass));

    Properties properties = new Properties();
    properties.putAll(options);
    Connection connect;
    String url = options.get(JDBCOptions.JDBC_URL());
    connect = driver.connect(url, properties);
    Objects.requireNonNull(connect, String.format("The driver could not open a JDBC connection. Check the URL: %s", url));
    return connect;
  }

  /**
   * Returns true if the table already exists in the JDBC database.
   */
  private static Boolean tableExists(Connection conn, Map<String, String> options) {
    JdbcDialect dialect = JdbcDialects.get(options.get(JDBCOptions.JDBC_URL()));
    try (PreparedStatement statement = conn.prepareStatement(dialect.getTableExistsQuery(options.get(JDBCOptions.JDBC_TABLE_NAME())))) {
      statement.setQueryTimeout(Integer.parseInt(options.get(JDBCOptions.JDBC_QUERY_TIMEOUT())));
      statement.executeQuery();
    } catch (SQLException e) {
      return false;
    }
    return true;
  }

  /***
   * call spark function get the schema through jdbc.
   * The code logic implementation refers to spark 2.4.x and spark 3.x.
   * @param options
   * @return
   * @throws Exception
   */
  public static Schema getJDBCSchema(Map<String, String> options) throws Exception {
    Connection conn = createConnectionFactory(options);
    String url = options.get(JDBCOptions.JDBC_URL());
    String table = options.get(JDBCOptions.JDBC_TABLE_NAME());
    boolean tableExists = tableExists(conn, options);

    if (tableExists) {
      JdbcDialect dialect = JdbcDialects.get(url);
      try (PreparedStatement statement = conn.prepareStatement(dialect.getSchemaQuery(table))) {
        statement.setQueryTimeout(Integer.parseInt(options.get("queryTimeout")));
        try (ResultSet rs = statement.executeQuery()) {
          StructType structType;
          if (Boolean.parseBoolean(options.get("nullable"))) {
            structType = JdbcUtils.getSchema(rs, dialect, true);
          } else {
            structType = JdbcUtils.getSchema(rs, dialect, false);
          }
          return AvroConversionUtils.convertStructTypeToAvroSchema(structType, table, "hoodie." + table);
        }
      }
    } else {
      throw new HoodieException(String.format("%s table does not exists!", table));
    }
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
