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

package org.apache.hudi.utilities.applied.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.applied.schema.UrsaBatchProtoSchemaProvider;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.S3EventsHoodieIncrSource;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectIncrCheckpoint;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.FILES_COLUMNS_TO_ADD_IN_DATA_SCHEMA;
import static org.apache.hudi.utilities.config.CloudSourceConfig.ENABLE_ADD_INPUT_FILE;
import static org.apache.hudi.utilities.config.CloudSourceConfig.INPUT_FILE_COLUMN;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.Type.S3;
import static  org.apache.spark.sql.protobuf.functions.from_protobuf;
import static scala.collection.JavaConverters.asScalaIterator;

public class UrsaBatchS3EventsHoodieIncrSource extends S3EventsHoodieIncrSource {
  private static final Logger LOG = LoggerFactory.getLogger(UrsaBatchS3EventsHoodieIncrSource.class);

  public static final String BUCKET_JOIN_COLUMN = "__ursa_bucket_name";
  public static final String FILE_KEY_JOIN_COLUMN = "__ursa_file_key";
  private final UrsaBatchProtoSchemaProvider protoSchemaProvider;

  public static final String INPUT_FILE_NAME_COLUMN = "__ursa_input_file_name";

  private static TypedProperties addSourceSpecificProperties(TypedProperties properties) {
    properties.setProperty(INPUT_FILE_COLUMN.key(), INPUT_FILE_NAME_COLUMN);
    properties.setProperty(ENABLE_ADD_INPUT_FILE.key(), "true");
    return properties;
  }

  public UrsaBatchS3EventsHoodieIncrSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, SchemaProvider schemaProvider) {
    super(addSourceSpecificProperties(props), sparkContext, sparkSession, schemaProvider, null);
    protoSchemaProvider = new UrsaBatchProtoSchemaProvider(props);
  }

  public UrsaBatchS3EventsHoodieIncrSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, UrsaBatchProtoSchemaProvider schemaProvider) {
    super(addSourceSpecificProperties(props), sparkContext, sparkSession, (SchemaProvider) null, null);
    protoSchemaProvider = schemaProvider;
  }

  public UrsaBatchS3EventsHoodieIncrSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    super(addSourceSpecificProperties(props), sparkContext, sparkSession, schemaProvider, metrics);
    protoSchemaProvider = new UrsaBatchProtoSchemaProvider(props);
  }

  public UrsaBatchS3EventsHoodieIncrSource(TypedProperties props,
                                           JavaSparkContext sparkContext,
                                           SparkSession sparkSession,
                                           HoodieIngestionMetrics metrics,
                                           StreamContext streamContext) {
    super(addSourceSpecificProperties(props), sparkContext, sparkSession, metrics, streamContext);
    protoSchemaProvider = new UrsaBatchProtoSchemaProvider(props);
  }

  protected Dataset<Row> joinAndDeserialize(Dataset<Row> sourceDataDf, Dataset<Row> filesDf) {
    List<String> joinColumns = Arrays.asList(BUCKET_JOIN_COLUMN, FILE_KEY_JOIN_COLUMN);

    // Setup Files Dataframe and get ready to broadcast it.
    List<Column> filesProjectedColumns = new ArrayList<>();
    joinColumns.forEach(col -> filesProjectedColumns.add(new Column(col)));
    Arrays.stream(FILES_COLUMNS_TO_ADD_IN_DATA_SCHEMA).forEach(col -> filesProjectedColumns.add(new Column(col)));
    filesDf = filesDf.select(filesProjectedColumns.toArray(new Column[0]));
    if (LOG.isDebugEnabled()) {
      LOG.info("filesDf=");
      filesDf.select(BUCKET_JOIN_COLUMN, FILE_KEY_JOIN_COLUMN).show(30, false);
      LOG.info("sourceDataDf=");
      sourceDataDf.select(BUCKET_JOIN_COLUMN, FILE_KEY_JOIN_COLUMN).show(30, false);
    }
    // Join Both dataframes
    sourceDataDf = sourceDataDf.join(functions.broadcast(filesDf), asScalaIterator(joinColumns.iterator()).toSeq(), "left_outer");
    sourceDataDf = sourceDataDf.drop(BUCKET_JOIN_COLUMN, FILE_KEY_JOIN_COLUMN, INPUT_FILE_NAME_COLUMN);
    // Now deserialize Proto and return
    return deserializeProtobufSchema(sourceDataDf);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> extractPartitionedSource(CloudDataFetcher cloudDataFetcher,
                                                                        QueryRunner queryRunner,
                                                                        Option<SchemaProvider> schemaProvider,
                                                                        Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter,
                                                                        CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint,
                                                                        QueryInfo queryInfo, long sourceLimit) {
    protoSchemaProvider.refresh();
    Pair<Pair<Option<Dataset<Row>>, Option<Dataset<Row>>>, String> res =
        cloudDataFetcher.fetchPartitionedSource(S3, cloudObjectIncrCheckpoint, sourceProfileSupplier,
                queryRunner.run(queryInfo, snapshotLoadQuerySplitter), schemaProvider, sourceLimit);
    if (res.getLeft().getLeft().isPresent()) {
      Dataset<Row> sourceDataDf = res.getLeft().getLeft().get()
              .withColumn(BUCKET_JOIN_COLUMN, functions.split(functions.split(functions.col(INPUT_FILE_NAME_COLUMN), "://").getItem(1), "/").getItem(0))
              .withColumn(FILE_KEY_JOIN_COLUMN, functions.split(functions.split(functions.col(INPUT_FILE_NAME_COLUMN), "://").getItem(1), "/", 2).getItem(1));
      Dataset<Row> filesDf = res.getLeft().getRight().get().withColumn(BUCKET_JOIN_COLUMN, new Column("s3.bucket.name")).withColumn(FILE_KEY_JOIN_COLUMN, new Column("s3.object.key"));
      // Add join key for source dataframe
      sourceDataDf = joinAndDeserialize(sourceDataDf, filesDf);
      return Pair.of(Option.of(sourceDataDf), res.getRight());
    } else {
      return Pair.of(res.getLeft().getLeft(), res.getRight());
    }
  }

  protected Dataset<Row> deserializeProtobufSchema(Dataset<Row> input) {
    input.show(10, false);
    return input.withColumn(protoSchemaProvider.getDeserializedParentColName(),
                    from_protobuf(functions.col(protoSchemaProvider.getSerializedSourceColumn()),
                            protoSchemaProvider.getProtoMessageTypeName(),
                            protoSchemaProvider.getProtoLatestLocalSchemaFile().getAbsolutePath()))
            .drop(protoSchemaProvider.getSerializedSourceColumn());
  }
}