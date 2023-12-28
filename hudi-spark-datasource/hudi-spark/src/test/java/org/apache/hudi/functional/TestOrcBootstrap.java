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

package org.apache.hudi.functional;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.bootstrap.HoodieSparkBootstrapSchemaProvider;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.OrcReaderIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.bootstrap.BootstrapUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests Bootstrap Client functionality.
 */
@Tag("functional")
public class TestOrcBootstrap extends TestBootstrapBase {

  @Override
  protected boolean shouldRunTest() {
    return HoodieSparkUtils.gteqSpark3_0();
  }

  @Override
  protected HoodieFileFormat getFileFormat() {
    return HoodieFileFormat.ORC;
  }

  @Override
  protected Schema getSchema(String srcPath) throws IOException {
    String filePath = FileStatusUtils.toPath(
        BootstrapUtils.getAllLeafFoldersWithFiles(metaClient.getTableConfig().getBaseFileFormat(),
                metaClient.getFs(),
                srcPath, context).stream().findAny().map(p -> p.getValue().stream().findAny())
            .orElse(null).get().getPath()).toString();
    StructType orcSchema = context.getSqlContext().read()
        .option("basePath", srcPath)
        .orc(filePath)
        .schema();
    return AvroConversionUtils.convertStructTypeToAvroSchema(orcSchema, "test_orc_record", null);
  }

  @Override
  protected String getFullBootstrapDataProviderName() {
    return TestFullBootstrapDataProvider.class.getName();
  }

  @Override
  protected void checkBootstrapResults(int totalRecords, Schema schema, String instant,
                                       boolean checkNumRawFiles,
                                       int expNumInstants, int numVersions, long expTimestamp,
                                       long expROTimestamp, boolean isDeltaCommit,
                                       List<String> instantsWithValidRecords,
                                       boolean validateCommitRecords) throws Exception {
    metaClient.reloadActiveTimeline();
    assertEquals(expNumInstants,
        metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertEquals(instant, metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());

    Dataset<Row> bootstrapped = sqlContext.read().format("orc").load(basePath);
    Dataset<Row> original = sqlContext.read().format("orc").load(bootstrapBasePath);
    bootstrapped.registerTempTable("bootstrapped");
    original.registerTempTable("original");
    if (checkNumRawFiles) {
      List<HoodieFileStatus> files =
          BootstrapUtils.getAllLeafFoldersWithFiles(metaClient.getTableConfig().getBaseFileFormat(),
                  metaClient.getFs(),
                  bootstrapBasePath, context).stream().flatMap(x -> x.getValue().stream())
              .collect(Collectors.toList());
      assertEquals(files.size() * numVersions,
          sqlContext.sql("select distinct _hoodie_file_name from bootstrapped").count());
    }

    if (!isDeltaCommit) {
      String predicate = String.join(", ",
          instantsWithValidRecords.stream().map(p -> "\"" + p + "\"")
              .collect(Collectors.toList()));
      if (validateCommitRecords) {
        assertEquals(totalRecords,
            sqlContext.sql("select * from bootstrapped where _hoodie_commit_time IN "
                + "(" + predicate + ")").count());
      }
      Dataset<Row> missingOriginal =
          sqlContext.sql("select a._row_key from original a where a._row_key not "
              + "in (select _hoodie_record_key from bootstrapped)");
      assertEquals(0, missingOriginal.count());
      Dataset<Row> missingBootstrapped =
          sqlContext.sql("select a._hoodie_record_key from bootstrapped a "
              + "where a._hoodie_record_key not in (select _row_key from original)");
      assertEquals(0, missingBootstrapped.count());
      //sqlContext.sql("select * from bootstrapped").show(10, false);
    }
  }

  @Override
  protected JavaRDD<HoodieRecord> generateInputBatch(JavaSparkContext jsc,
                                                     List<Pair<String, List<HoodieFileStatus>>> partitionPaths,
                                                     Schema writerSchema) {
    return generateInputBatchInternal(jsc, partitionPaths, writerSchema);
  }

  private static JavaRDD<HoodieRecord> generateInputBatchInternal(JavaSparkContext jsc,
                                                                  List<Pair<String, List<HoodieFileStatus>>> partitionPaths,
                                                                  Schema writerSchema) {
    List<Pair<String, Path>> fullFilePathsWithPartition =
        partitionPaths.stream().flatMap(p -> p.getValue().stream()
                .map(x -> Pair.of(p.getKey(), FileStatusUtils.toPath(x.getPath()))))
            .collect(Collectors.toList());
    return jsc.parallelize(fullFilePathsWithPartition.stream().flatMap(p -> {
      try {
        Configuration conf = jsc.hadoopConfiguration();
        AvroReadSupport.setAvroReadSchema(conf, writerSchema);
        Reader orcReader = OrcFile.createReader(
            p.getValue(),
            new OrcFile.ReaderOptions(jsc.hadoopConfiguration()));
        RecordReader recordReader = orcReader.rows();

        TypeDescription orcSchema = orcReader.getSchema();

        Iterator<GenericRecord> recIterator =
            new OrcReaderIterator(recordReader, writerSchema, orcSchema);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(recIterator, 0), false)
            .map(gr -> {
              try {
                String key = gr.get("_row_key").toString();
                String pPath = p.getKey();
                return new HoodieAvroRecord<>(new HoodieKey(key, pPath),
                    new RawTripTestPayload(gr.toString(), key, pPath,
                        HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
              } catch (IOException e) {
                throw new HoodieIOException(e.getMessage(), e);
              }
            });
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }).collect(Collectors.toList()));
  }

  public static class TestFullBootstrapDataProvider extends
      FullRecordBootstrapDataProvider<JavaRDD<HoodieRecord>> {

    public TestFullBootstrapDataProvider(TypedProperties props, HoodieSparkEngineContext context) {
      super(props, context);
    }

    @Override
    public JavaRDD<HoodieRecord> generateInputRecords(String tableName, String sourceBasePath,
                                                      List<Pair<String, List<HoodieFileStatus>>> partitionPaths,
                                                      HoodieWriteConfig config) {
      Schema avroSchema = new HoodieSparkBootstrapSchemaProvider(config)
          .getBootstrapSchema(context, partitionPaths);
      return generateInputBatchInternal(
          HoodieSparkEngineContext.getSparkContext(context), partitionPaths, avroSchema);
    }
  }
}
