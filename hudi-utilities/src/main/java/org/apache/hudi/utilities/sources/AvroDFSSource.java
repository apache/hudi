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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.DFSPathSelector;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * DFS Source that reads avro data.
 */
public class AvroDFSSource extends AvroSource {

  private final DFSPathSelector pathSelector;

  public AvroDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) throws IOException {
    super(props, sparkContext, sparkSession, schemaProvider);
    sparkContext.hadoopConfiguration().set("avro.schema.input.key", schemaProvider.getSourceSchema().toString());
    this.pathSelector = DFSPathSelector
        .createSourceSelector(props, sparkContext.hadoopConfiguration());
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<String>, String> selectPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(sparkContext, lastCkptStr, sourceLimit);
    return selectPathsWithMaxModificationTime.getLeft()
        .map(pathStr -> new InputBatch<>(Option.of(fromFiles(pathStr)), selectPathsWithMaxModificationTime.getRight()))
        .orElseGet(() -> new InputBatch<>(Option.empty(), selectPathsWithMaxModificationTime.getRight()));
  }

  private JavaRDD<GenericRecord> fromFiles(String pathStr) {
    sparkContext.setJobGroup(this.getClass().getSimpleName(), "Fetch Avro data from files");
    JavaPairRDD<AvroKey, NullWritable> avroRDD = sparkContext.newAPIHadoopFile(pathStr, AvroKeyInputFormat.class,
        AvroKey.class, NullWritable.class, sparkContext.hadoopConfiguration());
    return avroRDD.keys().map(r -> ((GenericRecord) r.datum()));
  }
}
