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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StorageSchemes;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.utilities.checkpointing.InitialCheckPointProvider;
import org.apache.hudi.utilities.config.SchemaProviderPostProcessorConfig;
import org.apache.hudi.utilities.exception.HoodieSchemaFetchException;
import org.apache.hudi.utilities.exception.HoodieSchemaPostProcessException;
import org.apache.hudi.utilities.exception.HoodieSourcePostProcessException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.DelegatingSchemaProvider;
import org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaPostProcessor;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProviderWithPostProcessor;
import org.apache.hudi.utilities.schema.postprocessor.ChainedSchemaPostProcessor;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.processor.ChainedJsonKafkaSourcePostProcessor;
import org.apache.hudi.utilities.sources.processor.JsonKafkaSourcePostProcessor;
import org.apache.hudi.utilities.streamer.StreamContext;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.ErrorTableAwareChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry;
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;

/**
 * Bunch of helper methods.
 */
public class UtilHelpers {

  public static final String EXECUTE = "execute";
  public static final String SCHEDULE = "schedule";
  public static final String SCHEDULE_AND_EXECUTE = "scheduleandexecute";
  public static final String PURGE_PENDING_INSTANT = "purge_pending_instant";

  private static final Logger LOG = LoggerFactory.getLogger(UtilHelpers.class);

  public static HoodieRecordMerger createRecordMerger(Properties props) {
    return HoodieRecordUtils.createRecordMerger(null, EngineType.SPARK,
        StringUtils.split(ConfigUtils.getStringWithAltKeys(props, HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES, null), ","),
        ConfigUtils.getStringWithAltKeys(props, HoodieWriteConfig.RECORD_MERGE_STRATEGY_ID, null));
  }

  public static Source createSource(String sourceClass, TypedProperties cfg, JavaSparkContext jssc,
                                    SparkSession sparkSession, HoodieIngestionMetrics metrics, StreamContext streamContext) throws IOException {
    // All possible constructors.
    Class<?>[] constructorArgsStreamContextMetrics = new Class<?>[] {TypedProperties.class, JavaSparkContext.class, SparkSession.class, HoodieIngestionMetrics.class, StreamContext.class};
    Class<?>[] constructorArgsStreamContext = new Class<?>[] {TypedProperties.class, JavaSparkContext.class, SparkSession.class, StreamContext.class};
    Class<?>[] constructorArgsMetrics = new Class<?>[] {TypedProperties.class, JavaSparkContext.class, SparkSession.class, SchemaProvider.class, HoodieIngestionMetrics.class};
    Class<?>[] constructorArgs = new Class<?>[] {TypedProperties.class, JavaSparkContext.class, SparkSession.class, SchemaProvider.class};
    // List of constructor and their respective arguments.
    List<Pair<Class<?>[], Object[]>> sourceConstructorAndArgs = new ArrayList<>();
    sourceConstructorAndArgs.add(Pair.of(constructorArgsStreamContextMetrics, new Object[] {cfg, jssc, sparkSession, metrics, streamContext}));
    sourceConstructorAndArgs.add(Pair.of(constructorArgsStreamContext, new Object[] {cfg, jssc, sparkSession, streamContext}));
    sourceConstructorAndArgs.add(Pair.of(constructorArgsMetrics, new Object[] {cfg, jssc, sparkSession, streamContext.getSchemaProvider(), metrics}));
    sourceConstructorAndArgs.add(Pair.of(constructorArgs, new Object[] {cfg, jssc, sparkSession, streamContext.getSchemaProvider()}));

    List<HoodieException> nonMatchingConstructorExceptions = new ArrayList<>();
    for (Pair<Class<?>[], Object[]> constructor : sourceConstructorAndArgs) {
      try {
        return (Source) ReflectionUtils.loadClass(sourceClass, constructor.getLeft(), constructor.getRight());
      } catch (HoodieException e) {
        if (e.getCause() instanceof NoSuchMethodException) {
          // If the cause is a NoSuchMethodException, ignore
          continue;
        }
        nonMatchingConstructorExceptions.add(e);
        String constructorSignature = Arrays.stream(constructor.getLeft())
            .map(Class::getSimpleName)
            .collect(Collectors.joining(", ", "[", "]"));
        LOG.error("Unexpected error while loading source class {} with constructor signature {}", sourceClass, constructorSignature, e);
      } catch (Throwable t) {
        throw new IOException("Could not load source class due to unexpected error " + sourceClass, t);
      }
    }

    // Rather than throw the last failure, we will only throw failures that did not occur due to NoSuchMethodException.
    IOException ioe = new IOException("Could not load any source class for " + sourceClass);
    nonMatchingConstructorExceptions.forEach(ioe::addSuppressed);
    throw ioe;
  }

  public static JsonKafkaSourcePostProcessor createJsonKafkaSourcePostProcessor(String postProcessorClassNames, TypedProperties props) throws IOException {
    if (StringUtils.isNullOrEmpty(postProcessorClassNames)) {
      return null;
    }

    try {
      List<JsonKafkaSourcePostProcessor> processors = new ArrayList<>();
      for (String className : (postProcessorClassNames.split(","))) {
        processors.add((JsonKafkaSourcePostProcessor) ReflectionUtils.loadClass(className, props));
      }
      return new ChainedJsonKafkaSourcePostProcessor(processors, props);
    } catch (Throwable e) {
      throw new HoodieSourcePostProcessException("Could not load postProcessorClassNames class(es) " + postProcessorClassNames, e);
    }
  }

  public static SchemaProvider createSchemaProvider(String schemaProviderClass, TypedProperties cfg,
                                                    JavaSparkContext jssc) throws IOException {
    try {
      return StringUtils.isNullOrEmpty(schemaProviderClass) ? null
          : (SchemaProvider) ReflectionUtils.loadClass(schemaProviderClass, cfg, jssc);
    } catch (Throwable e) {
      throw new IOException("Could not load schema provider class " + schemaProviderClass, e);
    }
  }

  public static SchemaPostProcessor createSchemaPostProcessor(
      String schemaPostProcessorClassNames, TypedProperties cfg, JavaSparkContext jssc) {

    if (StringUtils.isNullOrEmpty(schemaPostProcessorClassNames)) {
      return null;
    }

    try {
      List<SchemaPostProcessor> processors = new ArrayList<>();
      for (String className : (schemaPostProcessorClassNames.split(","))) {
        processors.add((SchemaPostProcessor) ReflectionUtils.loadClass(className, cfg, jssc));
      }
      return new ChainedSchemaPostProcessor(cfg, jssc, processors);
    } catch (Throwable e) {
      throw new HoodieSchemaPostProcessException("Could not load schemaPostProcessorClassNames class(es) " + schemaPostProcessorClassNames, e);
    }

  }

  public static StructType getSourceSchema(SchemaProvider schemaProvider) {
    if (schemaProvider != null && schemaProvider.getSourceSchema() != null && schemaProvider.getSourceSchema() != InputBatch.NULL_SCHEMA) {
      return AvroConversionUtils.convertAvroSchemaToStructType(schemaProvider.getSourceSchema());
    }
    return null;
  }

  public static Option<Transformer> createTransformer(Option<List<String>> classNamesOpt, Supplier<Option<Schema>> sourceSchemaSupplier,
                                                      boolean isErrorTableWriterEnabled) throws IOException {

    try {
      Function<List<String>, Transformer> chainedTransformerFunction = classNames ->
          isErrorTableWriterEnabled ? new ErrorTableAwareChainedTransformer(classNames, sourceSchemaSupplier)
              : new ChainedTransformer(classNames, sourceSchemaSupplier);
      return classNamesOpt.map(classNames -> classNames.isEmpty() ? null : chainedTransformerFunction.apply(classNames));
    } catch (Throwable e) {
      throw new IOException("Could not load transformer class(es) " + classNamesOpt.get(), e);
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

  public static DFSPropertiesConfiguration readConfig(Configuration hadoopConfig,
                                                      Path cfgPath,
                                                      List<String> overriddenProps) {
    StoragePath storagePath = convertToStoragePath(cfgPath);
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration(hadoopConfig, storagePath);
    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addPropsFromStream(new BufferedReader(new StringReader(String.join("\n", overriddenProps))), storagePath);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  public static DFSPropertiesConfiguration getConfig(List<String> overriddenProps) {
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration();
    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addPropsFromStream(new BufferedReader(new StringReader(String.join("\n", overriddenProps))), null);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  public static TypedProperties buildProperties(Configuration hadoopConf, String propsFilePath, List<String> props) {
    return StringUtils.isNullOrEmpty(propsFilePath)
        ? UtilHelpers.buildProperties(props)
        : UtilHelpers.readConfig(hadoopConf, new Path(propsFilePath), props)
        .getProps(true);
  }

  public static TypedProperties buildProperties(List<String> props) {
    TypedProperties properties = DFSPropertiesConfiguration.getGlobalProps();
    props.forEach(x -> {
      // Some values may contain '=', such as the partition path
      String[] kv = x.split("=", 2);
      ValidationUtils.checkArgument(kv.length == 2);
      properties.setProperty(kv[0], kv[1]);
    });
    return properties;
  }

  public static void validateAndAddProperties(String[] configs, SparkLauncher sparkLauncher) {
    Arrays.stream(configs).filter(config -> config.contains("=") && config.split("=", 2).length == 2).forEach(sparkLauncher::addAppArgs);
  }

  /**
   * Parse Schema from file.
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

  public static SparkConf buildSparkConf(String appName, String defaultMaster) {
    return buildSparkConf(appName, defaultMaster, new HashMap<>());
  }

  private static SparkConf buildSparkConf(String appName, String defaultMaster, Map<String, String> additionalConfigs) {
    final SparkConf sparkConf = new SparkConf().setAppName(appName);
    String master = sparkConf.get("spark.master", defaultMaster);
    sparkConf.setMaster(master);
    if (master.startsWith("yarn")) {
      sparkConf.setIfMissing("spark.eventLog.overwrite", "true");
      sparkConf.setIfMissing("spark.eventLog.enabled", "true");
    }
    sparkConf.setIfMissing("spark.ui.port", "8090");
    sparkConf.setIfMissing("spark.driver.maxResultSize", "2g");
    sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.setIfMissing("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
    sparkConf.setIfMissing("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compress", "true");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.type", "BLOCK");
    sparkConf.setIfMissing("spark.driver.allowMultipleContexts", "true");

    additionalConfigs.forEach(sparkConf::set);
    return sparkConf;
  }

  private static SparkConf buildSparkConf(String appName, Map<String, String> additionalConfigs) {
    final SparkConf sparkConf = new SparkConf().setAppName(appName);
    sparkConf.setIfMissing("spark.ui.port", "8090");
    sparkConf.setIfMissing("spark.driver.maxResultSize", "2g");
    sparkConf.setIfMissing("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.setIfMissing("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
    sparkConf.setIfMissing("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compress", "true");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.setIfMissing("spark.hadoop.mapred.output.compression.type", "BLOCK");

    additionalConfigs.forEach(sparkConf::set);
    return sparkConf;
  }

  public static JavaSparkContext buildSparkContext(String appName, Map<String, String> configs) {
    return new JavaSparkContext(buildSparkConf(appName, configs));
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
    if (sparkMemory != null) {
      sparkConf.set("spark.executor.memory", sparkMemory);
    }
    return new JavaSparkContext(sparkConf);
  }

  /**
   * Build Hoodie write client.
   *
   * @param jsc Java Spark Context
   * @param basePath Base Path
   * @param schemaStr Schema
   * @param parallelism Parallelism
   */
  public static SparkRDDWriteClient<HoodieRecordPayload> createHoodieClient(JavaSparkContext jsc, String basePath, String schemaStr,
      int parallelism, Option<String> compactionStrategyClass, TypedProperties properties) {
    Option<CompactionStrategy> strategyOpt = compactionStrategyClass.map(ReflectionUtils::loadClass);
    HoodieCompactionConfig compactionConfig = strategyOpt
        .map(strategy -> HoodieCompactionConfig.newBuilder().withInlineCompaction(false)
            .withCompactionStrategy(strategy).build())
        .orElse(HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build());
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withParallelism(parallelism, parallelism)
            .withBulkInsertParallelism(parallelism)
            .withDeleteParallelism(parallelism)
            .withSchema(schemaStr).combineInput(true, true).withCompactionConfig(compactionConfig)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withProps(properties).build();
    return new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), config);
  }

  public static int handleErrors(JavaSparkContext jsc, String instantTime, JavaRDD<WriteStatus> writeResponse) {
    LongAccumulator errors = jsc.sc().longAccumulator();
    writeResponse.foreach(writeStatus -> {
      if (writeStatus.hasErrors()) {
        errors.add(1);
        LOG.error("Error processing records :writeStatus:{}", writeStatus.getStat().toString());
      }
    });
    if (errors.value() == 0) {
      LOG.info("Table imported into hoodie with {} instant time.", instantTime);
      return 0;
    }
    LOG.error("Import failed with {} errors.", errors.value());
    return -1;
  }

  public static int handleErrors(HoodieCommitMetadata metadata, String instantTime) {
    List<HoodieWriteStat> writeStats = metadata.getWriteStats();
    long errorsCount = writeStats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum();
    if (errorsCount == 0) {
      LOG.info("Finish job with {} instant time.", instantTime);
      return 0;
    }

    LOG.error("Job failed with {} errors.", errorsCount);
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
  private static Boolean tableExists(Connection conn, Map<String, String> options) throws SQLException {
    JdbcDialect dialect = JdbcDialects.get(options.get(JDBCOptions.JDBC_URL()));
    try (PreparedStatement statement = conn.prepareStatement(dialect.getTableExistsQuery(options.get(JDBCOptions.JDBC_TABLE_NAME())))) {
      statement.setQueryTimeout(Integer.parseInt(options.get(JDBCOptions.JDBC_QUERY_TIMEOUT())));
      statement.executeQuery();
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
  public static Schema getJDBCSchema(Map<String, String> options) {
    Connection conn;
    String url;
    String table;
    boolean tableExists;
    try {
      conn = createConnectionFactory(options);
      url = options.get(JDBCOptions.JDBC_URL());
      table = options.get(JDBCOptions.JDBC_TABLE_NAME());
      tableExists = tableExists(conn, options);
    } catch (Exception e) {
      throw new HoodieSchemaFetchException("Failed to connect to jdbc", e);
    }

    if (!tableExists) {
      throw new HoodieSchemaFetchException(String.format("%s table does not exists!", table));
    }

    try {
      JdbcDialect dialect = JdbcDialects.get(url);
      try (PreparedStatement statement = conn.prepareStatement(dialect.getSchemaQuery(table))) {
        statement.setQueryTimeout(Integer.parseInt(options.get("queryTimeout")));
        try (ResultSet rs = statement.executeQuery()) {
          StructType structType;
          if (Boolean.parseBoolean(options.get("nullable"))) {
            structType = SparkAdapterSupport$.MODULE$.sparkAdapter().getSchemaUtils()
                .getSchema(conn, rs, dialect, true, false);
          } else {
            structType = SparkAdapterSupport$.MODULE$.sparkAdapter().getSchemaUtils()
                .getSchema(conn, rs, dialect, false, false);
          }
          return AvroConversionUtils.convertStructTypeToAvroSchema(structType, table, "hoodie." + table);
        }
      }
    } catch (HoodieException e) {
      throw e;
    } catch (Exception e) {
      throw new HoodieSchemaFetchException(String.format("Unable to fetch schema from %s table", table), e);
    }
  }

  public static SchemaProvider getOriginalSchemaProvider(SchemaProvider schemaProvider) {
    SchemaProvider originalProvider = schemaProvider;
    if (schemaProvider instanceof SchemaProviderWithPostProcessor) {
      originalProvider = ((SchemaProviderWithPostProcessor) schemaProvider).getOriginalSchemaProvider();
    } else if (schemaProvider instanceof DelegatingSchemaProvider) {
      originalProvider = ((DelegatingSchemaProvider) schemaProvider).getSourceSchemaProvider();
    }
    return originalProvider;
  }

  public static SchemaProvider wrapSchemaProviderWithPostProcessor(SchemaProvider provider,
                                                                                    TypedProperties cfg, JavaSparkContext jssc, List<String> transformerClassNames) {

    if (provider == null) {
      return null;
    }

    if (provider instanceof SchemaProviderWithPostProcessor) {
      return provider;
    }

    String schemaPostProcessorClass = getStringWithAltKeys(
        cfg, SchemaProviderPostProcessorConfig.SCHEMA_POST_PROCESSOR, true);

    if (schemaPostProcessorClass == null || schemaPostProcessorClass.isEmpty()) {
      return provider;
    }

    return new SchemaProviderWithPostProcessor(provider,
        Option.ofNullable(createSchemaPostProcessor(schemaPostProcessorClass, cfg, jssc)));
  }

  public static SchemaProvider getSchemaProviderForKafkaSource(SchemaProvider provider, TypedProperties cfg, JavaSparkContext jssc) {
    if (KafkaOffsetPostProcessor.Config.shouldAddOffsets(cfg)) {
      return new SchemaProviderWithPostProcessor(provider,
          Option.ofNullable(new KafkaOffsetPostProcessor(cfg, jssc)));
    }
    return provider;
  }

  public static SchemaProvider createRowBasedSchemaProvider(StructType structType,
                                                            TypedProperties cfg,
                                                            JavaSparkContext jssc) {
    SchemaProvider rowSchemaProvider = new RowBasedSchemaProvider(structType);
    return wrapSchemaProviderWithPostProcessor(rowSchemaProvider, cfg, jssc, null);
  }

  public static Option<Schema> getLatestTableSchema(JavaSparkContext jssc,
                                                    HoodieStorage storage,
                                                    String basePath,
                                                    HoodieTableMetaClient tableMetaClient) {
    try {
      if (FSUtils.isTableExists(basePath, storage)) {
        TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(tableMetaClient);

        return tableSchemaResolver.getTableAvroSchemaFromLatestCommit(false);
      }
    } catch (Exception e) {
      LOG.error("Failed to fetch latest table's schema", e);
    }

    return Option.empty();
  }

  public static HoodieTableMetaClient createMetaClient(
      JavaSparkContext jsc, String basePath, boolean shouldLoadActiveTimelineOnLoad) {
    return HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(basePath)
        .setLoadActiveTimelineOnLoad(shouldLoadActiveTimelineOnLoad)
        .build();
  }

  public static void addLockOptions(String basePath, String schema, TypedProperties props) {
    if (!props.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key())) {
      List<String> customSupportedFSs = props.getStringList(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key(), ",", new ArrayList<>());
      if (schema == null || customSupportedFSs.contains(schema) || StorageSchemes.isAtomicCreationSupported(schema)) {
        props.putAll(FileSystemBasedLockProvider.getLockConfig(basePath));
      }
    }
  }

  @FunctionalInterface
  public interface CheckedSupplier<T> {
    T get() throws Throwable;
  }

  public static int retry(int maxRetryCount, CheckedSupplier<Integer> supplier, String errorMessage) {
    int ret = -1;
    try {
      do {
        ret = supplier.get();
      } while (ret != 0 && maxRetryCount-- > 0);
    } catch (Throwable t) {
      LOG.error(errorMessage, t);
      throw new RuntimeException("Failed in retry", t);
    }
    return ret;
  }

  public static String getSchemaFromLatestInstant(HoodieTableMetaClient metaClient) throws Exception {
    TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
    Schema schema = schemaResolver.getTableAvroSchema(false);
    return schema.toString();
  }
}
