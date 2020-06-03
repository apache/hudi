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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import org.apache.hudi.utilities.sources.helpers.KafkaAvroConverter;
import org.apache.hudi.utilities.sources.helpers.MongoAvroConverter;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class MongoKafkaSource extends AvroSource {

  private static final Logger LOG = LogManager.getLogger(MongoKafkaSource.class);
    
  private final KafkaOffsetGen offsetGen;
    
  private final SchemaProvider schemaProvider;

  public MongoKafkaSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession, SchemaProvider schemaProvider) {
    super(properties, sparkContext, sparkSession, schemaProvider);
    properties.put("key.deserializer", StringDeserializer.class);
    properties.put("value.deserializer", StringDeserializer.class);
    this.offsetGen = new KafkaOffsetGen(properties);
    this.schemaProvider = schemaProvider;
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Option<String> lastCheckpointStr, long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    if (totalNewMsgs <= 0) {
      return new InputBatch<>(Option.empty(), lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : "");
    }
    JavaRDD<GenericRecord> newDataRDD = toRDD(offsetRanges);
    return new InputBatch<>(Option.of(newDataRDD), CheckpointUtils.offsetsToStr(offsetRanges));
  }

  private JavaRDD<GenericRecord> toRDD(OffsetRange[] offsetRanges) {
    final KafkaAvroConverter converter = new MongoAvroConverter(schemaProvider.getSourceSchema());
    return KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
            LocationStrategies.PreferConsistent()).mapPartitions(records -> converter.apply(records));
  }
}

