/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.utilities;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieJsonPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.index.HoodieIndex;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class HDFSParquetImporter implements Serializable {

  private static volatile Logger logger = LogManager.getLogger(HDFSParquetImporter.class);
  private final Config cfg;
  private final transient FileSystem fs;
  public static final SimpleDateFormat PARTITION_FORMATTER = new SimpleDateFormat("yyyy/MM/dd");

  public HDFSParquetImporter(
      Config cfg) throws IOException {
    this.cfg = cfg;
    fs = FSUtils.getFs();
  }

  public static class FormatValidator implements IValueValidator<String> {

    List<String> validFormats = Arrays.asList("parquet");

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || !validFormats.contains(value)) {
        throw new ParameterException(String
            .format("Invalid format type: value:%s: supported formats:%s", value,
                validFormats));
      }
    }
  }

  public static class SourceTypeValidator implements IValueValidator<String> {

    List<String> validSourceTypes = Arrays.asList("hdfs");

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || !validSourceTypes.contains(value)) {
        throw new ParameterException(String
            .format("Invalid source type: value:%s: supported source types:%s", value,
                validSourceTypes));
      }
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--src-path",
        "-sp"}, description = "Base path for the input dataset", required = true)
    public String srcPath = null;
    @Parameter(names = {"--src-type",
        "-st"}, description = "Source type for the input dataset", required = true,
        validateValueWith = SourceTypeValidator.class)
    public String srcType = null;
    @Parameter(names = {"--target-path",
        "-tp"}, description = "Base path for the target hoodie dataset", required = true)
    public String targetPath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--table-type", "-tt"}, description = "Table type", required = true)
    public String tableType = null;
    @Parameter(names = {"--row-key-field",
        "-rk"}, description = "Row key field name", required = true)
    public String rowKey = null;
    @Parameter(names = {"--partition-key-field",
        "-pk"}, description = "Partition key field name", required = true)
    public String partitionKey = null;
    @Parameter(names = {"--parallelism",
        "-pl"}, description = "Parallelism for hoodie insert", required = true)
    public int parallelism = 1;
    @Parameter(names = {"--schema-file",
        "-sf"}, description = "path for Avro schema file", required = true)
    public String schemaFile = null;
    @Parameter(names = {"--format",
        "-f"}, description = "Format for the input data.", required = false,
        validateValueWith = FormatValidator.class)
    public String format = null;
    @Parameter(names = {"--spark-master",
        "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory",
        "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--retry",
        "-rt"}, description = "number of retries", required = false)
    public int retry = 0;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String args[]) throws Exception {
    final HDFSParquetImporter.Config cfg = new HDFSParquetImporter.Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);
    dataImporter.dataImport(dataImporter.getSparkContext(), cfg.retry);
  }

  private JavaSparkContext getSparkContext() {
    SparkConf sparkConf = new SparkConf().setAppName("hoodie-data-importer-" + cfg.tableName);
    sparkConf.setMaster(cfg.sparkMaster);

    if (cfg.sparkMaster.startsWith("yarn")) {
      sparkConf.set("spark.eventLog.overwrite", "true");
      sparkConf.set("spark.eventLog.enabled", "true");
    }

    sparkConf.set("spark.driver.maxResultSize", "2g");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);

    // Configure hadoop conf
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    sparkConf = HoodieWriteClient.registerClasses(sparkConf);
    return new JavaSparkContext(sparkConf);
  }

  private String getSchema() throws Exception {
    // Read schema file.
    Path p = new Path(cfg.schemaFile);
    if (!fs.exists(p)) {
      throw new Exception(
          String.format("Could not find - %s - schema file.", cfg.schemaFile));
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

  public int dataImport(JavaSparkContext jsc, int retry) throws Exception {
    int ret = -1;
    try {
      // Verify that targetPath is not present.
      if (fs.exists(new Path(cfg.targetPath))) {
        throw new HoodieIOException(
            String.format("Make sure %s is not present.", cfg.targetPath));
      }
      do {
        ret = dataImport(jsc);
      } while (ret != 0 && retry-- > 0);
    } catch (Throwable t) {
      logger.error(t);
    }
    return ret;
  }

  @VisibleForTesting
  protected int dataImport(JavaSparkContext jsc) throws IOException {
    try {
      if (fs.exists(new Path(cfg.targetPath))) {
        // cleanup target directory.
        fs.delete(new Path(cfg.targetPath), true);
      }

      //Get schema.
      String schemaStr = getSchema();

      // Initialize target hoodie table.
      Properties properties = new Properties();
      properties.put(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, cfg.tableName);
      properties.put(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, cfg.tableType);
      HoodieTableMetaClient.initializePathAsHoodieDataset(fs, cfg.targetPath, properties);

      HoodieWriteClient client = createHoodieClient(jsc, cfg.targetPath, schemaStr,
          cfg.parallelism);

      Job job = Job.getInstance(jsc.hadoopConfiguration());
      // To parallelize reading file status.
      job.getConfiguration().set(FileInputFormat.LIST_STATUS_NUM_THREADS, "1024");
      AvroReadSupport.setAvroReadSchema(jsc.hadoopConfiguration(),
          (new Schema.Parser().parse(schemaStr)));
      ParquetInputFormat.setReadSupportClass(job, (AvroReadSupport.class));

      JavaRDD<HoodieRecord<HoodieJsonPayload>> hoodieRecords = jsc
          .newAPIHadoopFile(cfg.srcPath, ParquetInputFormat.class, Void.class,
              GenericRecord.class, job.getConfiguration())
          // To reduce large number of tasks.
          .coalesce(16 * cfg.parallelism)
          .map(new Function<Tuple2<Void, GenericRecord>, HoodieRecord<HoodieJsonPayload>>() {
                 @Override
                 public HoodieRecord<HoodieJsonPayload> call(Tuple2<Void, GenericRecord> entry)
                     throws Exception {
                   GenericRecord genericRecord = entry._2();
                   Object partitionField = genericRecord.get(cfg.partitionKey);
                   if (partitionField == null) {
                     throw new HoodieIOException(
                         "partition key is missing. :" + cfg.partitionKey);
                   }
                   Object rowField = genericRecord.get(cfg.rowKey);
                   if (rowField == null) {
                     throw new HoodieIOException(
                         "row field is missing. :" + cfg.rowKey);
                   }
                   long ts = (long) ((Double) partitionField * 1000l);
                   String partitionPath = PARTITION_FORMATTER.format(new Date(ts));
                   return new HoodieRecord<HoodieJsonPayload>(
                       new HoodieKey((String) rowField, partitionPath),
                       new HoodieJsonPayload(genericRecord.toString()));
                 }
               }
          );
      // Get commit time.
      String commitTime = client.startCommit();

      JavaRDD<WriteStatus> writeResponse = client.bulkInsert(hoodieRecords, commitTime);
      Accumulator<Integer> errors = jsc.accumulator(0);
      writeResponse.foreach(new VoidFunction<WriteStatus>() {
        @Override
        public void call(WriteStatus writeStatus) throws Exception {
          if (writeStatus.hasErrors()) {
            errors.add(1);
            logger.error(String.format("Error processing records :writeStatus:%s",
                writeStatus.getStat().toString()));
          }
        }
      });
      if (errors.value() == 0) {
        logger.info(String
            .format("Dataset imported into hoodie dataset with %s commit time.",
                commitTime));
        return 0;
      }
      logger.error(String.format("Import failed with %d errors.", errors.value()));
    } catch (Throwable t) {
      logger.error("Error occurred.", t);
    }
    return -1;
  }

  private static HoodieWriteClient createHoodieClient(JavaSparkContext jsc, String basePath,
      String schemaStr, int parallelism) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withParallelism(parallelism, parallelism).withSchema(schemaStr)
        .combineInput(true, true).withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .build();
    return new HoodieWriteClient(jsc, config);
  }
}
