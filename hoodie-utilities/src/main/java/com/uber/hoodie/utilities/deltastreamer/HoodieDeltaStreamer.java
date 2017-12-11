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

package com.uber.hoodie.utilities.deltastreamer;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.KeyGenerator;
import com.uber.hoodie.OverwriteWithLatestAvroPayload;
import com.uber.hoodie.SimpleKeyGenerator;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HiveIncrementalPuller;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.FilebasedSchemaProvider;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.DFSSource;
import com.uber.hoodie.utilities.sources.Source;
import com.uber.hoodie.utilities.sources.SourceDataFormat;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.JavaConversions;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply
 * it to the target dataset. Does not maintain any state, queries at runtime to see how far behind
 * the target dataset is from the source dataset. This can be overriden to force sync from a
 * timestamp.
 */
public class HoodieDeltaStreamer implements Serializable {

  private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);

  private static String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";

  private final Config cfg;

  /**
   * Source to pull deltas from
   */
  private transient Source source;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target
   * table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Extract the key for the target dataset
   */
  private KeyGenerator keyGenerator;

  /**
   * Filesystem used
   */
  private transient FileSystem fs;

  /**
   * Timeline with completed commits
   */
  private transient Optional<HoodieTimeline> commitTimelineOpt;

  /**
   * Spark context
   */
  private transient JavaSparkContext jssc;


  public HoodieDeltaStreamer(Config cfg) throws IOException {
    this.cfg = cfg;
    this.jssc = getSparkContext();
    this.fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());

    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(fs.getConf(), cfg.targetBasePath);
      this.commitTimelineOpt = Optional
          .of(meta.getActiveTimeline().getCommitsTimeline()
              .filterCompletedInstants());
    } else {
      this.commitTimelineOpt = Optional.empty();
    }

    //TODO(vc) Should these be passed from outside?
    initSchemaProvider();
    initKeyGenerator();
    initSource();
  }

  private void initSource() throws IOException {
    // Create the source & schema providers
    PropertiesConfiguration sourceCfg = UtilHelpers.readConfig(fs, new Path(cfg.sourceConfigProps));
    log.info("Creating source " + cfg.sourceClassName + " with configs : " + sourceCfg.toString());
    this.source = UtilHelpers
        .createSource(cfg.sourceClassName, sourceCfg, jssc, cfg.sourceFormat, schemaProvider);
  }

  private void initSchemaProvider() throws IOException {
    PropertiesConfiguration schemaCfg = UtilHelpers
        .readConfig(fs, new Path(cfg.schemaProviderConfigProps));
    log.info(
        "Creating schema provider " + cfg.schemaProviderClassName + " with configs : " + schemaCfg
            .toString());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, schemaCfg);
  }

  private void initKeyGenerator() throws IOException {
    PropertiesConfiguration keygenCfg = UtilHelpers.readConfig(fs, new Path(cfg.keyGeneratorProps));
    log.info("Creating key generator " + cfg.keyGeneratorClass + " with configs : " + keygenCfg
        .toString());
    this.keyGenerator = DataSourceUtils.createKeyGenerator(cfg.keyGeneratorClass, keygenCfg);
  }


  private JavaSparkContext getSparkContext() {
    SparkConf sparkConf = new SparkConf()
        .setAppName("hoodie-delta-streamer-" + cfg.targetTableName);
    //sparkConf.setMaster(cfg.sparkMaster);
    sparkConf.setMaster("local[2]");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.driver.maxResultSize", "2g");

    // Configure hadoop conf
    sparkConf.set("spark.hadoop.mapred.output.compress", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true");
    sparkConf.set("spark.hadoop.mapred.output.compression.codec",
        "org.apache.hadoop.io.compress.GzipCodec");
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK");

    sparkConf = HoodieWriteClient.registerClasses(sparkConf);
    // register the schemas, so that shuffle does not serialize the full schemas
    List<Schema> schemas = Arrays
        .asList(schemaProvider.getSourceSchema(), schemaProvider.getTargetSchema());
    sparkConf.registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
    return new JavaSparkContext(sparkConf);
  }

  private void sync() throws Exception {
    // Retrieve the previous round checkpoints, if any
    Optional<String> resumeCheckpointStr = Optional.empty();
    if (commitTimelineOpt.isPresent()) {
      Optional<HoodieInstant> lastCommit = commitTimelineOpt.get().lastInstant();
      if (lastCommit.isPresent()) {
        HoodieCommitMetadata commitMetadata =
            HoodieCommitMetadata
                .fromBytes(commitTimelineOpt.get().getInstantDetails(lastCommit.get()).get());
        if (commitMetadata.getMetadata(CHECKPOINT_KEY) != null) {
          resumeCheckpointStr = Optional.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else {
          throw new HoodieDeltaStreamerException(
              "Unable to find previous checkpoint. Please double check if this table " +
                  "was indeed built via delta streamer ");
        }
      }
    } else {
      Properties properties = new Properties();
      properties.put(HoodieWriteConfig.TABLE_NAME, cfg.targetTableName);
      HoodieTableMetaClient
          .initializePathAsHoodieDataset(
              FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()), cfg.targetBasePath,
              properties);
    }
    log.info("Checkpoint to resume from : " + resumeCheckpointStr);

    // Pull the data from the source & prepare the write
    Pair<Optional<JavaRDD<GenericRecord>>, String> dataAndCheckpoint = source
        .fetchNewData(resumeCheckpointStr, cfg.maxInputBytes);

    if (!dataAndCheckpoint.getKey().isPresent()) {
      log.info("No new data, nothing to commit.. ");
      return;
    }

    JavaRDD<GenericRecord> avroRDD = dataAndCheckpoint.getKey().get();
    JavaRDD<HoodieRecord> records = avroRDD
        .map(gr -> {
          HoodieRecordPayload payload = DataSourceUtils.createPayload(
              cfg.payloadClassName,
              gr,
              (Comparable) gr.get(cfg.sourceOrderingField));
          return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
        });

    // Perform the write
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig(cfg.hoodieClientProps);
    HoodieWriteClient client = new HoodieWriteClient<>(jssc, hoodieCfg);
    String commitTime = client.startCommit();
    log.info("Starting commit  : " + commitTime);

    JavaRDD<WriteStatus> writeStatusRDD;
    if (cfg.operation == Operation.INSERT) {
      writeStatusRDD = client.insert(records, commitTime);
    } else if (cfg.operation == Operation.UPSERT) {
      writeStatusRDD = client.upsert(records, commitTime);
    } else {
      throw new HoodieDeltaStreamerException("Unknown operation :" + cfg.operation);
    }

    // Simply commit for now. TODO(vc): Support better error handlers later on
    HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
    checkpointCommitMetadata.put(CHECKPOINT_KEY, dataAndCheckpoint.getValue());

    boolean success = client
        .commit(commitTime, writeStatusRDD, Optional.of(checkpointCommitMetadata));
    if (success) {
      log.info("Commit " + commitTime + " successful!");
      // TODO(vc): Kick off hive sync from here.

    } else {
      log.info("Commit " + commitTime + " failed!");
    }
    client.close();
  }

  private HoodieWriteConfig getHoodieClientConfig(String hoodieClientCfgPath) throws Exception {
    return HoodieWriteConfig.newBuilder()
        .combineInput(true, true)
        .withPath(cfg.targetBasePath)
        .withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withPayloadClass(OverwriteWithLatestAvroPayload.class.getName()).build())
        .withSchema(schemaProvider.getTargetSchema().toString())
        .forTable(cfg.targetTableName)
        .withIndexConfig(
            HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .fromInputStream(fs.open(new Path(hoodieClientCfgPath)))
        .build();
  }

  private enum Operation {
    UPSERT,
    INSERT
  }

  private class OperationConvertor implements IStringConverter<Operation> {

    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }

  private class SourceFormatConvertor implements IStringConverter<SourceDataFormat> {

    @Override
    public SourceDataFormat convert(String value) throws ParameterException {
      return SourceDataFormat.valueOf(value);
    }
  }

  public static class Config implements Serializable {

    /**
     * TARGET CONFIGS
     **/
    @Parameter(names = {
        "--target-base-path"}, description = "base path for the target hoodie dataset", required = true)
    public String targetBasePath;

    // TODO: How to obtain hive configs to register?
    @Parameter(names = {
        "--target-table"}, description = "name of the target table in Hive", required = true)
    public String targetTableName;

    @Parameter(names = {"--hoodie-client-config"}, description =
        "path to properties file on localfs or dfs, with hoodie client config. Sane defaults" +
            "are used, but recommend use to provide basic things like metrics endpoints, hive configs etc")
    public String hoodieClientProps = null;

    /**
     * SOURCE CONFIGS
     **/
    @Parameter(names = {"--source-class"}, description =
        "subclass of com.uber.hoodie.utilities.sources.Source to use to read data. " +
            "built-in options: com.uber.hoodie.utilities.common.{DFSSource (default), KafkaSource, HiveIncrPullSource}")
    public String sourceClassName = DFSSource.class.getName();

    @Parameter(names = {"--source-config"}, description =
        "path to properties file on localfs or dfs, with source configs. " +
            "For list of acceptable properties, refer the source class", required = true)
    public String sourceConfigProps = null;

    @Parameter(names = {"--source-format"}, description =
        "Format of data in source, JSON (default), Avro. All source data is " +
            "converted to Avro using the provided schema in any case", converter = SourceFormatConvertor.class)
    public SourceDataFormat sourceFormat = SourceDataFormat.JSON;

    @Parameter(names = {"--source-ordering-field"}, description =
        "Field within source record to decide how to break ties between " +
            " records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--key-generator-class"}, description =
        "Subclass of com.uber.hoodie.utilities.common.KeyExtractor to generate" +
            "a HoodieKey from the given avro record. Built in: SimpleKeyGenerator (Uses provided field names as recordkey & partitionpath. "
            +
            "Nested fields specified via dot notation, e.g: a.b.c)")
    public String keyGeneratorClass = SimpleKeyGenerator.class.getName();

    @Parameter(names = {"--key-generator-config"}, description =
        "Path to properties file on localfs or dfs, with KeyGenerator configs. " +
            "For list of acceptable properites, refer the KeyGenerator class", required = true)
    public String keyGeneratorProps = null;

    @Parameter(names = {"--payload-class"}, description =
        "subclass of HoodieRecordPayload, that works off a GenericRecord. " +
            "Default: SourceWrapperPayload. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

    @Parameter(names = {"--schemaprovider-class"}, description =
        "subclass of com.uber.hoodie.utilities.schema.SchemaProvider " +
            "to attach schemas to input & target table data, built in options: FilebasedSchemaProvider")
    public String schemaProviderClassName = FilebasedSchemaProvider.class.getName();

    @Parameter(names = {"--schemaprovider-config"}, description =
        "path to properties file on localfs or dfs, with schema configs. " +
            "For list of acceptable properties, refer the schema provider class", required = true)
    public String schemaProviderConfigProps = null;


    /**
     * Other configs
     **/
    @Parameter(names = {
        "--max-input-bytes"}, description = "Maximum number of bytes to read from source. Default: 1TB")
    public long maxInputBytes = 1L * 1024 * 1024 * 1024 * 1024;

    @Parameter(names = {"--op"}, description =
        "Takes one of these values : UPSERT (default), INSERT (use when input " +
            "is purely new data/inserts to gain speed)", converter = OperationConvertor.class)
    public Operation operation = Operation.UPSERT;


    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    new HoodieDeltaStreamer(cfg).sync();
  }
}
