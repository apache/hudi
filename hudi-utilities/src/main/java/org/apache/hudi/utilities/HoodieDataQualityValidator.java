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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

public class HoodieDataQualityValidator implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieDataQualityValidator.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public HoodieDataQualityValidator(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;

    this.props = UtilHelpers.buildProperties(cfg.configs);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-bp"}, description = "Base path for the table", required = false)
    public String basePath = null;

    @Parameter(names = {"--source-path", "-sp"}, description = "Source path for parquet input", required = false)
    public String sourcePath = null;

    @Parameter(names = {"--record-key-field", "-rk"}, description = "Source path for parquet input", required = false)
    public String recordKeyField = null;

    @Parameter(names = {"--partition-path-field", "-pp"}, description = "Source path for parquet input", required = false)
    public String partitionPathField = null;

    @Parameter(names = {"--fields-to-validate", "-ftv"}, description = "Ordering field config", required = false)
    public String fieldsToValidate = null;

    @Parameter(names = {"--use-overwrite-with-latest", "-uowl"}, description = "User overwrite with latest to find reduce final values " +
        "from input data", required = false)
    public Boolean useOverwriteWithLatest = false;

    @Parameter(names = {"--ordering-field-config", "-of"}, description = "Ordering field config", required = false)
    public String orderingFieldConfig = null;

    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for valuation", required = false)
    public int parallelism = 10;

    @Parameter(names = {"--log-records", "-lr"}, description = "Log records" +
        "from input data", required = false)
    public Boolean logRecords = false;

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
      return "TableSizeStats {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --parallelism " + parallelism + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
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
      TableSizeStats.Config config = (TableSizeStats.Config) o;
      return basePath.equals(config.basePath)
          && Objects.equals(parallelism, config.parallelism)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, sourcePath, parallelism, sparkMaster, sparkMemory, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Table-Size-Stats", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      HoodieDataQualityValidator sourceDataValidator = new HoodieDataQualityValidator(jsc, cfg);
      sourceDataValidator.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.basePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg.toString());
      LOG.info(" ****** Validating Hudi table data with source data ******");
      assertDataQuality();
    } catch (Exception e) {
      throw new HoodieException("Unable to do data quality validation." + cfg.basePath, e);
    }
  }

  private void assertDataQuality() {
    SparkSession spark = SparkSession.builder().config(jsc.getConf()).enableHiveSupport().getOrCreate();
    List<String> fieldsToValidate = Arrays.asList(cfg.fieldsToValidate.split(","));
    Dataset<Row> expectedDf = cfg.useOverwriteWithLatest ? getInputDfBasedOnLatestValue(cfg.recordKeyField, cfg.partitionPathField,
        "rider", spark, cfg.sourcePath,
        fieldsToValidate): getInputDfBasedOnOrderingValue(cfg.recordKeyField, cfg.partitionPathField,
        cfg.orderingFieldConfig, spark, cfg.sourcePath,
        fieldsToValidate);
    Dataset<Row> hudiDf = getDatasetToValidate(spark, cfg.basePath, fieldsToValidate,
        cfg.useOverwriteWithLatest ? "rider" : cfg.orderingFieldConfig);

    Dataset<Row> exceptInputDf = expectedDf.except(hudiDf);
    Dataset<Row> exceptHudiDf = hudiDf.except(expectedDf);
    long exceptInputCount = exceptInputDf.count();
    long exceptHudiCount = exceptHudiDf.count();
    LOG.info("Except input df count " + exceptInputDf + ", except hudi count " + exceptHudiCount +", total records found " + expectedDf.count());
    if (exceptInputCount != 0 || exceptHudiCount != 0) {
      if (cfg.logRecords) {
        LOG.info("Logging Source except hudi df ");
        expectedDf.except(hudiDf).sort(cfg.partitionPathField, cfg.recordKeyField).collectAsList().forEach(row -> LOG.info("   " + row.toString()));
        LOG.info("Logging Hudi except input data ");
        hudiDf.except(expectedDf).sort(cfg.partitionPathField, cfg.recordKeyField).collectAsList().forEach(row -> LOG.info("   " + row.toString()));
      }
      LOG.error("Data set validation failed. Total count in hudi " + expectedDf.count() + ", input df count " + hudiDf.count()
          + ". InputDf except hudi df = " + exceptInputCount + ", Hudi df except Input df " + exceptHudiCount);
      throw new AssertionError("Hudi contents does not match contents input data. ");
    }
    LOG.info("****** Validation succeeded");
  }

  private Dataset<Row> getInputDfBasedOnOrderingValue(String recordKeyField, String partitionPathField, String orderingField,
                                                      SparkSession session, String inputPath,
                                                      List<String> fieldsToValidate) {
    Dataset<Row> inputDf = session.read().format("parquet").load(inputPath + "/*/*");

    ExpressionEncoder encoder = getEncoder(inputDf.schema());
    List<Column> allCols = fieldsToValidate.stream().map(entry -> new Column(entry)).collect(Collectors.toList());
    return inputDf
        .groupByKey((MapFunction<Row, String>) row -> (row.getAs(partitionPathField) + "+" + row.getAs(recordKeyField)), Encoders.STRING())
        .reduceGroups(new ReduceFunction<Row>() {
          @Override
          public Row call(Row v1, Row v2) throws Exception {
            long ts1 = v1.getAs(orderingField);
            long ts2 = v2.getAs(orderingField);
            if (ts1 > ts2) {
              return v1;
            } else {
              return v2;
            }
          }
        })
        .map((MapFunction<Tuple2<String, Row>, Row>) value -> value._2, encoder)
        .filter("_hoodie_is_deleted != true")
        .select(JavaConverters.asScalaIteratorConverter(allCols.iterator()).asScala().toSeq())
        .orderBy(cfg.partitionPathField, cfg.recordKeyField, orderingField);
  }

  private Dataset<Row> getInputDfBasedOnLatestValue(String recordKeyField, String partitionPathField, String orderingField,
                                                      SparkSession session, String inputPath,
                                                      List<String> fieldsToValidate) {
    Dataset<Row> inputDf = session.read().format("parquet").load(inputPath + "/*/*");

    ExpressionEncoder encoder = getEncoder(inputDf.schema());
    List<Column> allCols = fieldsToValidate.stream().map(entry -> new Column(entry)).collect(Collectors.toList());
    return inputDf
        .groupByKey((MapFunction<Row, String>) row -> (row.getAs(partitionPathField) + "+" + row.getAs(recordKeyField)), Encoders.STRING())
        .reduceGroups(new ReduceFunction<Row>() {
          @Override
          public Row call(Row v1, Row v2) throws Exception {
            String ts1 = v1.getAs(orderingField);
            String ts2 = v2.getAs(orderingField);
            if (ts1.compareTo(ts2) > 0) {
              return v1;
            } else {
              return v2;
            }
          }
        })
        .map((MapFunction<Tuple2<String, Row>, Row>) value -> value._2, encoder)
        .filter("_hoodie_is_deleted != true")
        .select(JavaConverters.asScalaIteratorConverter(allCols.iterator()).asScala().toSeq())
        .orderBy(cfg.partitionPathField, cfg.recordKeyField, orderingField);
  }

  public Dataset<Row> getDatasetToValidate(SparkSession session, String basePath, List<String> fieldsToValidate, String orderingField) {

    // fieldNames.head, fieldNames.tail: _*)
    // HoodieRecord.HOODIE_META_COLUMNS.asScala.toSeq: _*
    Dataset<Row> hudiDf = session.read().format("hudi").load(basePath);
    List<Column> allCols = fieldsToValidate.stream().map(entry -> new Column(entry)).collect(Collectors.toList());
    return hudiDf.select(JavaConverters.asScalaIteratorConverter(allCols.iterator()).asScala().toSeq())
        .orderBy(cfg.partitionPathField, cfg.recordKeyField, orderingField);
  }

  private ExpressionEncoder getEncoder(StructType schema) {
    return SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema);
  }
}
