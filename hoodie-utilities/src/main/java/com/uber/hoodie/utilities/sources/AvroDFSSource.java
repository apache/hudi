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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.helpers.DFSPathSelector;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * DFS Source that reads avro data
 */
public class AvroDFSSource extends AvroSource {

  private final DFSPathSelector pathSelector;

  public AvroDFSSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
    this.pathSelector = new DFSPathSelector(props, sparkContext.hadoopConfiguration());
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Optional<String> lastCkptStr,
      long sourceLimit) {
    Pair<Optional<String>, String> selectPathsWithMaxModificationTime =
        pathSelector.getNextFilePathsAndMaxModificationTime(lastCkptStr, sourceLimit);
    return selectPathsWithMaxModificationTime.getLeft().map(pathStr -> new InputBatch<>(
        Optional.of(fromFiles(pathStr)),
        selectPathsWithMaxModificationTime.getRight()))
        .orElseGet(() -> new InputBatch<>(Optional.empty(), selectPathsWithMaxModificationTime.getRight()));
  }

  private JavaRDD<GenericRecord> fromFiles(String pathStr) {
    JavaPairRDD<AvroKey, NullWritable> avroRDD = sparkContext.newAPIHadoopFile(pathStr,
        AvroKeyInputFormat.class, AvroKey.class, NullWritable.class,
        sparkContext.hadoopConfiguration());
    return avroRDD.keys().map(r -> ((GenericRecord) r.datum()));
  }
}
